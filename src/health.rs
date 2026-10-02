//! Sondas activas (TCP connect o GET path esperando 2xx) en un hilo aparte,
//! uno por generación de configuración y worker. El hilo solo toca el estado
//! atómico (`ServerShared`) de los servidores, que recibe por `Arc`, así que
//! la generación vieja se puede liberar sin esperar a que el hilo acabe.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use crate::balancer::{Backend, ServerShared};
use crate::config::{CfgHealth, HealthType};
use crate::util::mono_ms;
use crate::{log, logw};

struct Target {
    backend: String,
    addr_str: String,
    sa: SocketAddr,
    cfg: CfgHealth,
    shared: Arc<ServerShared>,
    next: u64,
}

/// Al soltarlo se pide la parada del hilo.
pub struct Health {
    stop: Arc<(Mutex<bool>, Condvar)>,
}

impl Health {
    pub fn stop(&self) {
        let (m, cv) = &*self.stop;
        *m.lock().unwrap_or_else(|e| e.into_inner()) = true;
        cv.notify_all();
    }
}

impl Drop for Health {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Sonda síncrona.
pub fn probe(sa: &SocketAddr, host: &str, cfg: &CfgHealth) -> bool {
    let to = Duration::from_millis(cfg.timeout_ms);
    let Ok(mut s) = TcpStream::connect_timeout(sa, to) else { return false };
    if cfg.kind == HealthType::Tcp {
        return true;
    }
    let _ = s.set_read_timeout(Some(to));
    let _ = s.set_write_timeout(Some(to));
    let req =
        format!("GET {} HTTP/1.1\r\nHost: {host}\r\nUser-Agent: proxy-health\r\nConnection: close\r\n\r\n", cfg.path);
    if s.write_all(req.as_bytes()).is_err() {
        return false;
    }
    let mut resp = [0u8; 64];
    let mut got = 0;
    while got < 12 {
        match s.read(&mut resp[got..]) {
            Ok(0) | Err(_) => return false,
            Ok(n) => got += n,
        }
    }
    &resp[..7] == b"HTTP/1." && resp[9] == b'2'
}

fn run(mut targets: Vec<Target>, stop: Arc<(Mutex<bool>, Condvar)>) {
    let (m, cv) = &*stop;
    let mut guard = m.lock().unwrap_or_else(|e| e.into_inner());
    while !*guard {
        let mut now = mono_ms();
        let mut next = now + 1000;
        for t in targets.iter_mut() {
            if t.next <= now {
                drop(guard);
                let ok = probe(&t.sa, &t.addr_str, &t.cfg);
                guard = m.lock().unwrap_or_else(|e| e.into_inner());
                if *guard {
                    return;
                }
                if t.shared.probe_result(&t.cfg, ok) {
                    logw!(
                        "backend '{}' servidor {} -> {} (sonda {})",
                        t.backend,
                        t.addr_str,
                        if ok { "UP" } else { "DOWN" },
                        if t.cfg.kind == HealthType::Http { "http" } else { "tcp" }
                    );
                }
                now = mono_ms();
                t.next = now + t.cfg.interval_ms;
            }
            next = next.min(t.next);
        }
        let wait = next.saturating_sub(mono_ms());
        if wait > 0 {
            guard = cv.wait_timeout(guard, Duration::from_millis(wait)).unwrap_or_else(|e| e.into_inner()).0;
        }
    }
}

/// None si ningún backend tiene health activo (no es un error).
pub fn start(backends: &[std::rc::Rc<Backend>]) -> Option<Health> {
    let now = mono_ms();
    let targets: Vec<Target> = backends
        .iter()
        .filter(|b| b.health.kind != HealthType::None)
        .flat_map(|b| {
            b.servers.iter().map(move |s| Target {
                backend: b.name.clone(),
                addr_str: s.addr_str.clone(),
                sa: s.sa,
                cfg: b.health.clone(),
                shared: s.shared.clone(),
                next: now,
            })
        })
        .collect();
    if targets.is_empty() {
        return None;
    }
    let stop = Arc::new((Mutex::new(false), Condvar::new()));
    let tag = log::thread_tag();
    let st = stop.clone();
    let name: String = format!("proxy-h{tag}").chars().take(15).collect();
    let spawned = std::thread::Builder::new().name(name).spawn(move || {
        log::set_thread_tag(&tag);
        run(targets, st)
    });
    match spawned {
        Ok(_) => Some(Health { stop }),
        Err(e) => {
            crate::loge!("no se puede crear el hilo de health: {e}");
            None
        }
    }
}
