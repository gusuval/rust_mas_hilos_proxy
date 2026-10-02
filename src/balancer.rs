//! Backends y selección de servidor: round_robin, weighted (smooth WRR),
//! least_conn y least_load (cabecera X-Backend-Load + power of two choices).
//!
//! El estado de salud (`ServerShared`) es atómico y compartido: lo tocan el
//! hilo worker (fallos pasivos) y su hilo de health checks (sondas). El
//! resto vive en `Cell`: solo lo toca el hilo worker dueño del backend.

use std::cell::{Cell, RefCell};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering::*};

use crate::config::{CfgBackend, CfgHealth, HealthType, Strategy};
use crate::util::{mono_ms, rng_next};

pub const LOAD_EMA_ALPHA: f64 = 0.3;
pub const LOAD_STALE_MS: u64 = 5000;

#[derive(Debug)]
pub struct ServerShared {
    pub up: AtomicBool,
    pub fails: AtomicI32,
    pub oks: AtomicI32,
    /// Para la recuperación pasiva sin health activo.
    pub down_since: AtomicU64,
}

impl Default for ServerShared {
    fn default() -> Self {
        ServerShared {
            up: AtomicBool::new(true),
            fails: AtomicI32::new(0),
            oks: AtomicI32::new(0),
            down_since: AtomicU64::new(0),
        }
    }
}

impl ServerShared {
    /// Fallo pasivo o de sonda. true si el servidor pasa a "down".
    pub fn fail(&self, fall: i32, now: u64) -> bool {
        self.oks.store(0, Relaxed);
        let f = self.fails.fetch_add(1, AcqRel) + 1;
        if f >= fall && self.up.swap(false, AcqRel) {
            self.down_since.store(now, Relaxed);
            return true;
        }
        false
    }

    /// Resultado de una sonda activa. true si cambió de estado.
    pub fn probe_result(&self, h: &CfgHealth, ok: bool) -> bool {
        if !ok {
            return self.fail(h.fall, mono_ms());
        }
        self.fails.store(0, Relaxed);
        let k = self.oks.fetch_add(1, AcqRel) + 1;
        if k >= h.rise && !self.up.load(Acquire) {
            self.up.store(true, Release);
            return true;
        }
        false
    }
}

pub struct Server {
    pub addr_str: String,
    pub sa: SocketAddr,
    pub idx: usize,
    pub weight: i32,
    pub cur_weight: Cell<i32>,
    /// Peticiones en curso en este worker.
    pub active: Cell<i32>,
    pub load: Cell<f64>,
    pub load_ts: Cell<u64>,
    pub load_known: Cell<bool>,
    pub requests: Cell<u64>,
    pub failures: Cell<u64>,
    pub shared: Arc<ServerShared>,
    /// Pool keep-alive: índices de upstreams ociosos (LIFO).
    pub idle: RefCell<Vec<usize>>,
}

pub struct Backend {
    pub name: String,
    pub strategy: Strategy,
    pub max_idle: usize,
    pub health: CfgHealth,
    pub servers: Vec<Server>,
    rr: Cell<usize>,
    rng: Cell<u64>,
}

impl Backend {
    pub fn new(cfg: &CfgBackend) -> Backend {
        let servers = cfg
            .servers
            .iter()
            .enumerate()
            .map(|(i, s)| Server {
                addr_str: s.addr.clone(),
                sa: s.sa,
                idx: i,
                weight: s.weight,
                cur_weight: Cell::new(0),
                active: Cell::new(0),
                load: Cell::new(0.0),
                load_ts: Cell::new(0),
                load_known: Cell::new(false),
                requests: Cell::new(0),
                failures: Cell::new(0),
                shared: Arc::new(ServerShared::default()),
                idle: RefCell::new(Vec::new()),
            })
            .collect();
        let seed = 0x9E37_79B9_7F4A_7C15u64 ^ (cfg as *const _ as u64) ^ mono_ms().wrapping_mul(0x1000_0001);
        Backend {
            name: cfg.name.clone(),
            strategy: cfg.strategy,
            max_idle: cfg.max_idle,
            health: cfg.health.clone(),
            servers,
            rr: Cell::new(0),
            rng: Cell::new(seed | 1),
        }
    }

    pub fn is_up(&self, s: &Server, now: u64) -> bool {
        let sh = &s.shared;
        if sh.up.load(Relaxed) {
            return true;
        }
        // Sin health activo, un servidor caído se reintenta tras 'interval'
        // (half-open): un solo fallo más lo vuelve a marcar como caído.
        if self.health.kind == HealthType::None
            && now.saturating_sub(sh.down_since.load(Relaxed)) >= self.health.interval_ms
        {
            sh.fails.store(self.health.fall - 1, Relaxed);
            sh.up.store(true, Relaxed);
            return true;
        }
        false
    }

    fn usable(&self, s: &Server, ex: u64, now: u64) -> bool {
        !(s.idx < 64 && ex & (1u64 << s.idx) != 0) && self.is_up(s, now)
    }

    fn pick_rr(&self, ex: u64, now: u64) -> Option<usize> {
        let n = self.servers.len();
        let rr = self.rr.get();
        for k in 0..n {
            let i = (rr + k) % n;
            if self.usable(&self.servers[i], ex, now) {
                self.rr.set((rr + k + 1) % n);
                return Some(i);
            }
        }
        None
    }

    /// Smooth weighted round robin (nginx): reparto proporcional sin ráfagas.
    fn pick_weighted(&self, ex: u64, now: u64) -> Option<usize> {
        let mut best: Option<usize> = None;
        let mut total = 0;
        for (i, s) in self.servers.iter().enumerate() {
            if !self.usable(s, ex, now) {
                continue;
            }
            s.cur_weight.set(s.cur_weight.get() + s.weight);
            total += s.weight;
            if best.is_none_or(|b| s.cur_weight.get() > self.servers[b].cur_weight.get()) {
                best = Some(i);
            }
        }
        if let Some(b) = best {
            let s = &self.servers[b];
            s.cur_weight.set(s.cur_weight.get() - total);
        }
        best
    }

    fn pick_least_conn(&self, ex: u64, now: u64) -> Option<usize> {
        let n = self.servers.len();
        let rr = self.rr.get();
        let mut best: Option<usize> = None;
        for k in 0..n {
            let i = (rr + k) % n;
            let s = &self.servers[i];
            if self.usable(s, ex, now) && best.is_none_or(|b| s.active.get() < self.servers[b].active.get()) {
                best = Some(i);
            }
        }
        if let Some(b) = best {
            self.rr.set((b + 1) % n);
        }
        best
    }

    fn load_valid(s: &Server, now: u64) -> bool {
        s.load_known.get() && now.saturating_sub(s.load_ts.get()) <= LOAD_STALE_MS
    }

    fn rand(&self) -> u64 {
        let mut r = self.rng.get();
        let v = rng_next(&mut r);
        self.rng.set(r);
        v
    }

    /// Power of two choices sobre la carga reportada.
    fn pick_least_load(&self, ex: u64, now: u64) -> Option<usize> {
        let cand: Vec<usize> = (0..self.servers.len()).filter(|&i| self.usable(&self.servers[i], ex, now)).collect();
        match cand.len() {
            0 => return None,
            1 => return Some(cand[0]),
            _ => {}
        }
        let n = cand.len() as u64;
        let i = (self.rand() % n) as usize;
        let mut j = (self.rand() % (n - 1)) as usize;
        if j >= i {
            j += 1;
        }
        let (xi, yi) = (cand[i], cand[j]);
        let (x, y) = (&self.servers[xi], &self.servers[yi]);
        let (kx, ky) = (Self::load_valid(x, now), Self::load_valid(y, now));
        if kx && ky && (x.load.get() - y.load.get()).abs() > 1e-9 {
            return Some(if x.load.get() < y.load.get() { xi } else { yi });
        }
        // Carga desconocida, caducada o empate: desempata least_conn.
        if x.active.get() != y.active.get() {
            return Some(if x.active.get() < y.active.get() { xi } else { yi });
        }
        // Empate total: se prefiere al que no tiene carga fresca, para
        // conocerla cuanto antes (exploración).
        if kx != ky {
            return Some(if kx { yi } else { xi });
        }
        Some(xi)
    }

    /// Elige un servidor sano, excluyendo los bits de `exclude` (servidores
    /// ya probados en esta petición).
    pub fn pick(&self, exclude: u64, now: u64) -> Option<usize> {
        if self.servers.is_empty() {
            return None;
        }
        match self.strategy {
            Strategy::RoundRobin => self.pick_rr(exclude, now),
            Strategy::Weighted => self.pick_weighted(exclude, now),
            Strategy::LeastConn => self.pick_least_conn(exclude, now),
            Strategy::LeastLoad => self.pick_least_load(exclude, now),
        }
    }

    pub fn report_load(&self, s: &Server, v: f64, now: u64) {
        if Self::load_valid(s, now) {
            s.load.set(LOAD_EMA_ALPHA * v + (1.0 - LOAD_EMA_ALPHA) * s.load.get());
        } else {
            s.load.set(v);
        }
        s.load_known.set(true);
        s.load_ts.set(now);
    }

    /// Fallo pasivo desde el event loop. true si cambió de estado.
    pub fn passive_fail(&self, s: &Server, now: u64) -> bool {
        s.failures.set(s.failures.get() + 1);
        s.shared.fail(self.health.fall, now)
    }

    pub fn passive_ok(s: &Server) {
        if s.shared.fails.load(Relaxed) != 0 {
            s.shared.fails.store(0, Relaxed);
        }
    }
}

/// Valor de X-Backend-Load (0.0 - 1.0).
pub fn parse_load(v: &[u8]) -> Option<f64> {
    let s = std::str::from_utf8(v).ok()?.trim_matches([' ', '\t']);
    if s.is_empty() || s.len() >= 32 {
        return None;
    }
    let d: f64 = s.parse().ok()?;
    (d.is_finite() && (0.0..=1.0).contains(&d)).then_some(d)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CfgServer;

    fn mk(strategy: Strategy, weights: &[i32]) -> (Box<CfgBackend>, Backend) {
        let cfg = Box::new(CfgBackend {
            name: "t".into(),
            strategy,
            max_idle: 8,
            servers: weights
                .iter()
                .enumerate()
                .map(|(i, &w)| {
                    let addr = format!("127.0.0.1:{}", 9000 + i);
                    CfgServer { sa: addr.parse().unwrap(), addr, weight: w }
                })
                .collect(),
            health: CfgHealth {
                kind: HealthType::None,
                path: "/".into(),
                interval_ms: 1000,
                timeout_ms: 100,
                rise: 2,
                fall: 2,
            },
        });
        let b = Backend::new(&cfg);
        (cfg, b)
    }

    #[test]
    fn round_robin() {
        let (_c, b) = mk(Strategy::RoundRobin, &[1, 1, 1]);
        let mut count = [0; 3];
        for _ in 0..300 {
            count[b.pick(0, 1000).unwrap()] += 1;
        }
        assert_eq!(count, [100, 100, 100]);
        let a = b.pick(0, 1000).unwrap();
        assert_eq!(b.pick(0, 1000).unwrap(), (a + 1) % 3, "orden estricto");
    }

    #[test]
    fn weighted_smooth() {
        let (_c, b) = mk(Strategy::Weighted, &[3, 1]);
        let (mut count, mut run, mut maxrun, mut last) = ([0; 2], 0, 0, usize::MAX);
        for _ in 0..400 {
            let k = b.pick(0, 1000).unwrap();
            count[k] += 1;
            run = if k == last { run + 1 } else { 1 };
            last = k;
            maxrun = maxrun.max(run);
        }
        assert_eq!(count, [300, 100]);
        assert!(maxrun <= 3, "sin ráfagas largas del servidor pesado");
    }

    #[test]
    fn least_conn() {
        let (_c, b) = mk(Strategy::LeastConn, &[1, 1, 1]);
        b.servers[0].active.set(5);
        b.servers[1].active.set(1);
        b.servers[2].active.set(3);
        assert_eq!(b.pick(0, 1000), Some(1));
        b.servers[1].active.set(9);
        assert_eq!(b.pick(0, 1000), Some(2));
        for s in &b.servers {
            s.active.set(0);
        }
        let (a, c) = (b.pick(0, 1000), b.pick(0, 1000));
        assert_ne!(a, c, "empate: rotación");
    }

    #[test]
    fn least_load() {
        let (_c, b) = mk(Strategy::LeastLoad, &[1, 1]);
        b.report_load(&b.servers[0], 0.1, 1000);
        b.report_load(&b.servers[1], 0.9, 1000);
        for _ in 0..50 {
            assert_eq!(b.pick(0, 1000), Some(0));
        }
        b.report_load(&b.servers[0], 1.0, 1100);
        let l = b.servers[0].load.get();
        assert!(l > 0.35 && l < 0.45, "EMA: una muestra alta no invierte de golpe ({l})");
    }

    #[test]
    fn least_load_stale() {
        let (_c, b) = mk(Strategy::LeastLoad, &[1, 1]);
        b.report_load(&b.servers[0], 0.9, 1000);
        b.report_load(&b.servers[1], 0.1, 1000);
        b.servers[1].active.set(10);
        let later = 1000 + LOAD_STALE_MS + 1;
        for _ in 0..20 {
            assert_eq!(b.pick(0, later), Some(0), "carga caducada: decide least_conn");
        }
        b.report_load(&b.servers[0], 0.2, later);
        let l = b.servers[0].load.get();
        assert!(l > 0.19 && l < 0.21, "tras caducar se reemplaza, no se promedia");
    }

    #[test]
    fn least_load_explores_unknown() {
        let (_c, b) = mk(Strategy::LeastLoad, &[1, 1]);
        b.report_load(&b.servers[1], 0.9, 1000);
        for _ in 0..20 {
            assert_eq!(b.pick(0, 1000), Some(0));
        }
        b.servers[0].active.set(3);
        for _ in 0..20 {
            assert_eq!(b.pick(0, 1000), Some(1), "least_conn manda sobre la exploración");
        }
    }

    #[test]
    fn down_excluded() {
        for s in [Strategy::RoundRobin, Strategy::Weighted, Strategy::LeastConn, Strategy::LeastLoad] {
            let (_c, mut b) = mk(s, &[1, 1, 1]);
            b.health.kind = HealthType::Tcp; // sin recuperación pasiva
            b.servers[1].shared.up.store(false, Relaxed);
            for _ in 0..60 {
                assert_ne!(b.pick(0, 1000), Some(1));
            }
            b.servers[0].shared.up.store(false, Relaxed);
            b.servers[2].shared.up.store(false, Relaxed);
            assert_eq!(b.pick(0, 1000), None);
        }
    }

    #[test]
    fn exclude_mask() {
        let (_c, b) = mk(Strategy::RoundRobin, &[1, 1, 1]);
        for _ in 0..10 {
            assert_eq!(b.pick(0x3, 1000), Some(2));
        }
        assert_eq!(b.pick(0x7, 1000), None);
    }

    #[test]
    fn passive_health() {
        let (_c, mut b) = mk(Strategy::RoundRobin, &[1, 1]);
        let s = &b.servers[0];
        assert!(!b.passive_fail(s, 1000));
        assert!(b.passive_fail(s, 1000), "fall = 2");
        assert!(!b.is_up(s, 1500));
        assert!(b.is_up(s, 2001), "sin health activo: se reintenta tras interval");
        assert!(b.passive_fail(s, 2002), "half-open: cae al primer fallo");
        b.health.kind = HealthType::Http;
        let s = &b.servers[0];
        s.shared.up.store(false, Relaxed);
        assert!(!s.shared.probe_result(&b.health, true));
        assert!(s.shared.probe_result(&b.health, true), "rise = 2");
        assert!(s.shared.up.load(Relaxed));
    }

    #[test]
    fn load_values() {
        assert!((parse_load(b"0.5").unwrap() - 0.5).abs() < 1e-9);
        assert_eq!(parse_load(b" 1 "), Some(1.0));
        for bad in [&b"1.5"[..], b"-0.1", b"abc", b"", b"nan", b"inf"] {
            assert_eq!(parse_load(bad), None, "{bad:?}");
        }
    }
}
