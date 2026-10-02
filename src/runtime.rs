//! Generaciones de configuración.
//!
//! `Compiled` lo construye el master una sola vez por recarga (config
//! validada, routers y contextos TLS con los certificados ya cargados) y se
//! comparte entre todos los hilos worker con `Arc`: es inmutable.
//!
//! `Runtime` es la generación de un worker: añade los backends con su
//! estado local (pools, contadores, balanceo) y el hilo de health. Las
//! peticiones en vuelo guardan un `Rc<Runtime>`; al recargar el worker
//! cambia el puntero y la generación vieja se libera cuando ya nadie la usa
//! (esquema RCU).

use std::rc::Rc;
use std::sync::Arc;

use crate::balancer::Backend;
use crate::config::Config;
use crate::health::{self, Health};
use crate::router::Router;
use crate::tls::TlsFrontend;

pub struct Frontend {
    pub name: String,
    pub key: String,
    pub tls: Option<Arc<TlsFrontend>>,
    pub router: Router,
}

pub struct Compiled {
    pub cfg: Config,
    pub fes: Vec<Frontend>,
    /// Sockets de escucha abiertos por el master (solo fuera de Linux).
    pub listeners: Vec<(String, Arc<std::net::TcpListener>)>,
}

impl Compiled {
    /// Carga los certificados y construye los routers.
    pub fn build(cfg: Config) -> Result<Compiled, String> {
        let mut fes = Vec::with_capacity(cfg.frontends.len());
        for f in &cfg.frontends {
            let mut router = Router::new();
            for r in &f.routes {
                router.add(&r.host, cfg.find_backend(&r.backend).expect("validado"));
            }
            let tls = if f.tls { Some(Arc::new(TlsFrontend::new(f)?)) } else { None };
            fes.push(Frontend { name: f.name.clone(), key: f.key.clone(), tls, router });
        }
        Ok(Compiled { cfg, fes, listeners: Vec::new() })
    }
}

pub struct Runtime {
    pub c: Arc<Compiled>,
    pub backends: Vec<Rc<Backend>>,
    pub health: Option<Health>,
    pub generation: u64,
}

impl Runtime {
    pub fn new(c: Arc<Compiled>, generation: u64) -> Runtime {
        let backends: Vec<Rc<Backend>> = c.cfg.backends.iter().map(|b| Rc::new(Backend::new(b))).collect();
        let health = health::start(&backends);
        Runtime { c, backends, health, generation }
    }

    pub fn frontend(&self, key: &str) -> Option<&Frontend> {
        self.c.fes.iter().find(|f| f.key == key)
    }
}
