//! Estadísticas compartidas entre los hilos del proceso. Cada hilo worker
//! escribe solo en su slot (alineado a línea de caché para evitar false
//! sharing): contadores atómicos y una tabla de servidores que publica cada
//! 250 ms bajo el mutex del slot. El hilo master agrega y sirve JSON por un
//! socket UNIX.

use std::fmt::Write as _;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering::Relaxed};

use crate::util::mono_ms;

#[derive(Clone, Default, Debug)]
pub struct ServerStat {
    pub backend: String,
    pub server: String,
    pub up: bool,
    pub active: i64,
    pub idle: i64,
    pub load: Option<f64>,
    pub requests: u64,
    pub failures: u64,
}

pub const ST_NCLASS: usize = 5;

#[repr(align(64))]
#[derive(Default)]
pub struct WorkerStats {
    /// true mientras el hilo worker corre.
    pub alive: AtomicBool,
    pub requests: AtomicU64,
    pub accepted: AtomicU64,
    pub active_conns: AtomicI64,
    pub tls_handshakes: AtomicU64,
    pub upstream_connects: AtomicU64,
    pub upstream_reuses: AtomicU64,
    pub upstream_retries: AtomicU64,
    pub reloads: AtomicU64,
    pub resp: [AtomicU64; ST_NCLASS],
    pub buffers_in_use: AtomicU64,
    pub servers: Mutex<Vec<ServerStat>>,
}

pub struct Stats {
    pub start_mono_ms: u64,
    pub reloads_ok: AtomicU64,
    pub reloads_failed: AtomicU64,
    pub worker_restarts: AtomicU64,
    pub w: Vec<WorkerStats>,
}

#[inline]
pub fn inc(c: &AtomicU64) {
    c.fetch_add(1, Relaxed);
}

impl Stats {
    pub fn new(nworkers: usize) -> Stats {
        Stats {
            start_mono_ms: mono_ms(),
            reloads_ok: AtomicU64::new(0),
            reloads_failed: AtomicU64::new(0),
            worker_restarts: AtomicU64::new(0),
            w: (0..nworkers).map(|_| WorkerStats::default()).collect(),
        }
    }

    /// JSON agregado de todos los workers.
    pub fn json(&self, log_dropped: u64) -> String {
        #[derive(Default)]
        struct Agg {
            s: ServerStat,
            up_workers: i64,
            seen_workers: i64,
            load_sum: f64,
            load_n: i64,
        }
        let (mut req, mut acc, mut tls, mut uc, mut ur, mut retr, mut bufs) = (0u64, 0, 0, 0, 0, 0, 0);
        let mut active = 0i64;
        let mut resp = [0u64; ST_NCLASS];
        let mut agg: Vec<Agg> = Vec::new();
        let mut alive = 0;
        for w in &self.w {
            let is_alive = w.alive.load(Relaxed);
            alive += is_alive as usize;
            req += w.requests.load(Relaxed);
            acc += w.accepted.load(Relaxed);
            active += w.active_conns.load(Relaxed);
            tls += w.tls_handshakes.load(Relaxed);
            uc += w.upstream_connects.load(Relaxed);
            ur += w.upstream_reuses.load(Relaxed);
            retr += w.upstream_retries.load(Relaxed);
            bufs += w.buffers_in_use.load(Relaxed);
            for (r, c) in resp.iter_mut().zip(&w.resp) {
                *r += c.load(Relaxed);
            }
            if !is_alive {
                continue;
            }
            let snapshot = w.servers.lock().unwrap_or_else(|e| e.into_inner()).clone();
            for t in snapshot {
                let a = match agg.iter_mut().position(|a| a.s.backend == t.backend && a.s.server == t.server) {
                    Some(i) => &mut agg[i],
                    None => {
                        agg.push(Agg {
                            s: ServerStat {
                                backend: t.backend.clone(),
                                server: t.server.clone(),
                                ..Default::default()
                            },
                            ..Default::default()
                        });
                        agg.last_mut().unwrap()
                    }
                };
                a.seen_workers += 1;
                a.up_workers += t.up as i64;
                a.s.active += t.active;
                a.s.idle += t.idle;
                a.s.requests += t.requests;
                a.s.failures += t.failures;
                if let Some(l) = t.load {
                    a.load_sum += l;
                    a.load_n += 1;
                }
            }
        }
        let up_s = (mono_ms() - self.start_mono_ms) as f64 / 1000.0;
        let mut o = String::with_capacity(2048);
        let _ = write!(
            o,
            "{{\"uptime_s\":{up_s:.1},\"workers\":{},\"workers_alive\":{alive},\
             \"worker_restarts\":{},\"reloads_ok\":{},\"reloads_failed\":{},\
             \"connections_accepted\":{acc},\"connections_active\":{active},\
             \"tls_handshakes\":{tls},\"requests_total\":{req},\
             \"responses\":{{\"1xx\":{},\"2xx\":{},\"3xx\":{},\"4xx\":{},\"5xx\":{}}},\
             \"upstream\":{{\"connects\":{uc},\"reuses\":{ur},\"retries\":{retr}}},\
             \"buffers_in_use\":{bufs},\"log_dropped\":{log_dropped},\"backends\":[",
            self.w.len(),
            self.worker_restarts.load(Relaxed),
            self.reloads_ok.load(Relaxed),
            self.reloads_failed.load(Relaxed),
            resp[0],
            resp[1],
            resp[2],
            resp[3],
            resp[4],
        );
        for (j, a) in agg.iter().enumerate() {
            let state = if a.up_workers == a.seen_workers {
                "up"
            } else if a.up_workers == 0 {
                "down"
            } else {
                "degraded"
            };
            let load = if a.load_n > 0 { format!("{:.3}", a.load_sum / a.load_n as f64) } else { "null".into() };
            let _ = write!(
                o,
                "{}{{\"backend\":\"{}\",\"server\":\"{}\",\"state\":\"{state}\",\"up_workers\":{},\
                 \"active\":{},\"idle\":{},\"load\":{load},\"requests\":{},\"failures\":{}}}",
                if j > 0 { "," } else { "" },
                json_escape(&a.s.backend),
                json_escape(&a.s.server),
                a.up_workers,
                a.s.active,
                a.s.idle,
                a.s.requests,
                a.s.failures
            );
        }
        o.push_str("]}\n");
        o
    }
}

fn json_escape(s: &str) -> String {
    let mut o = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => o.push_str("\\\""),
            '\\' => o.push_str("\\\\"),
            c if (c as u32) < 0x20 => {
                let _ = write!(o, "\\u{:04x}", c as u32);
            }
            c => o.push(c),
        }
    }
    o
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aggregates_workers() {
        let s = Stats::new(2);
        for (i, w) in s.w.iter().enumerate() {
            w.alive.store(true, Relaxed);
            w.requests.store(10, Relaxed);
            w.resp[1].store(9, Relaxed);
            w.resp[4].store(1, Relaxed);
            *w.servers.lock().unwrap() = vec![ServerStat {
                backend: "api".into(),
                server: "127.0.0.1:1".into(),
                up: i == 0,
                active: 1,
                idle: 2,
                load: Some(0.5),
                requests: 5,
                failures: 1,
            }];
        }
        let j = s.json(3);
        assert!(j.contains("\"workers\":2,\"workers_alive\":2"), "{j}");
        assert!(j.contains("\"requests_total\":20"));
        assert!(j.contains("\"2xx\":18") && j.contains("\"5xx\":2"));
        assert!(j.contains("\"log_dropped\":3"));
        assert!(j.contains("\"state\":\"degraded\",\"up_workers\":1,\"active\":2,\"idle\":4,\"load\":0.500,\"requests\":10,\"failures\":2"), "{j}");
        s.w[1].alive.store(false, Relaxed);
        let j = s.json(0);
        assert!(j.contains("\"workers_alive\":1"));
        assert!(j.contains("\"state\":\"up\""), "solo cuentan los workers vivos: {j}");
        assert_eq!(json_escape("a\"b\\c\n"), "a\\\"b\\\\c\\u000a");
    }
}
