//! Configuración TOML: parseo y validación estricta (claves desconocidas son
//! un error). No comprueba que los certificados se puedan cargar (eso lo
//! hace `tls`).

use std::net::SocketAddr;

use toml::{Table, Value};

use crate::log;
use crate::util::addr_parse;

pub const NAME_MAX: usize = 64;
pub const HOST_MAX: usize = 256;
pub const ADDR_MAX: usize = 128;
pub const PATH_MAX: usize = 512;
/// Por backend (máscara de reintentos de 64 bits).
pub const MAX_SERVERS: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Strategy {
    RoundRobin,
    Weighted,
    LeastConn,
    LeastLoad,
}

const STRATEGIES: [(&str, Strategy); 4] = [
    ("round_robin", Strategy::RoundRobin),
    ("weighted", Strategy::Weighted),
    ("least_conn", Strategy::LeastConn),
    ("least_load", Strategy::LeastLoad),
];

impl Strategy {
    pub fn name(self) -> &'static str {
        STRATEGIES.iter().find(|(_, s)| *s == self).unwrap().0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthType {
    None,
    Tcp,
    Http,
}

#[derive(Debug, Clone)]
pub struct CfgHealth {
    pub kind: HealthType,
    pub path: String,
    pub interval_ms: u64,
    pub timeout_ms: u64,
    pub rise: i32,
    pub fall: i32,
}

#[derive(Debug, Clone)]
pub struct CfgCert {
    pub sni: String,
    pub cert: String,
    pub key: String,
}

#[derive(Debug, Clone)]
pub struct CfgRoute {
    /// "api.test", "*.example.com" o "default" (en minúsculas).
    pub host: String,
    pub backend: String,
}

#[derive(Debug, Clone)]
pub struct CfgFrontend {
    pub name: String,
    pub listen: String,
    /// Dirección canónica "ip:puerto".
    pub key: String,
    pub addr: SocketAddr,
    pub tls: bool,
    pub certs: Vec<CfgCert>,
    pub default_cert: usize,
    pub routes: Vec<CfgRoute>,
}

#[derive(Debug, Clone)]
pub struct CfgServer {
    pub addr: String,
    pub sa: SocketAddr,
    pub weight: i32,
}

#[derive(Debug, Clone)]
pub struct CfgBackend {
    pub name: String,
    pub strategy: Strategy,
    pub max_idle: usize,
    pub servers: Vec<CfgServer>,
    pub health: CfgHealth,
}

#[derive(Debug, Clone, Copy)]
pub struct Timeouts {
    pub client_header: u64,
    pub client_idle: u64,
    pub upstream_connect: u64,
    pub upstream_read: u64,
    pub upstream_idle: u64,
}

#[derive(Debug, Clone)]
pub struct Config {
    /// Ya resuelto ("auto" -> nº de CPUs).
    pub workers: usize,
    pub stats_socket: String,
    pub log_file: String,
    pub log_level: i32,
    pub access_log: bool,
    pub timeouts: Timeouts,
    pub frontends: Vec<CfgFrontend>,
    pub backends: Vec<CfgBackend>,
}

impl Config {
    pub fn find_backend(&self, name: &str) -> Option<usize> {
        self.backends.iter().position(|b| b.name == name)
    }
}

type R<T> = Result<T, String>;

fn type_name(v: &Value) -> &'static str {
    match v {
        Value::String(_) => "cadena",
        Value::Integer(_) => "entero",
        Value::Float(_) => "real",
        Value::Boolean(_) => "booleano",
        Value::Datetime(_) => "fecha",
        Value::Array(_) => "array",
        Value::Table(_) => "tabla",
    }
}

fn check_keys(t: &Table, where_: &str, allowed: &[&str]) -> R<()> {
    match t.keys().find(|k| !allowed.contains(&k.as_str())) {
        Some(k) => Err(format!("{where_}: clave desconocida '{k}'")),
        None => Ok(()),
    }
}

fn get_str(t: &Table, key: &str, max: usize, required: bool, where_: &str) -> R<Option<String>> {
    match t.get(key) {
        None if required => Err(format!("{where_}: falta '{key}'")),
        None => Ok(None),
        Some(Value::String(s)) if s.len() >= max => Err(format!("{where_}: '{key}' demasiado largo")),
        Some(Value::String(s)) => Ok(Some(s.clone())),
        Some(_) => Err(format!("{where_}: '{key}' debe ser una cadena")),
    }
}

fn get_int(t: &Table, key: &str, min: i64, max: i64, where_: &str) -> R<Option<i64>> {
    match t.get(key) {
        None => Ok(None),
        Some(Value::Integer(i)) if *i < min || *i > max => {
            Err(format!("{where_}: '{key}' fuera de rango [{min}, {max}]"))
        }
        Some(Value::Integer(i)) => Ok(Some(*i)),
        Some(_) => Err(format!("{where_}: '{key}' debe ser un entero")),
    }
}

fn get_bool(t: &Table, key: &str, where_: &str) -> R<Option<bool>> {
    match t.get(key) {
        None => Ok(None),
        Some(Value::Boolean(b)) => Ok(Some(*b)),
        Some(_) => Err(format!("{where_}: '{key}' debe ser booleano")),
    }
}

fn get_table<'a>(t: &'a Table, key: &str, where_: &str) -> R<Option<&'a Table>> {
    match t.get(key) {
        None => Ok(None),
        Some(Value::Table(x)) => Ok(Some(x)),
        Some(v) => Err(format!("{where_}: '{key}' debe ser una tabla ({})", type_name(v))),
    }
}

fn get_tables<'a>(t: &'a Table, key: &str, where_: &str) -> R<Option<Vec<&'a Table>>> {
    match t.get(key) {
        None => Ok(None),
        Some(Value::Array(a)) => a
            .iter()
            .enumerate()
            .map(|(i, v)| match v {
                Value::Table(x) => Ok(x),
                _ => Err(format!("{where_}: {key}[{i}]: debe ser una tabla")),
            })
            .collect::<R<Vec<_>>>()
            .map(Some),
        Some(_) => Err(format!("{where_}: '{key}' debe ser un array de tablas")),
    }
}

fn resolve_path(base_dir: &str, p: &str) -> String {
    if p.is_empty() || p.starts_with('/') || p == "-" || base_dir.is_empty() {
        p.to_string()
    } else {
        format!("{base_dir}/{p}")
    }
}

fn valid_host_pattern(h: &str) -> bool {
    if h == "default" {
        return true;
    }
    let h = match h.strip_prefix('*') {
        Some(rest) => match rest.strip_prefix('.') {
            Some(r) if !r.is_empty() => r,
            _ => return false,
        },
        None => h,
    };
    !h.is_empty() && h.bytes().all(|c| c.is_ascii_alphanumeric() || b"-._".contains(&c))
}

fn default_workers() -> usize {
    std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1).min(128)
}

fn parse_global(root: &Table, c: &mut Config, base_dir: &str) -> R<()> {
    let Some(g) = get_table(root, "global", "raíz")? else { return Ok(()) };
    let w = "[global]";
    check_keys(g, w, &["workers", "stats_socket", "log_file", "log_level", "access_log"])?;
    match g.get("workers") {
        None => {}
        Some(Value::Integer(i)) if (1..=128).contains(i) => c.workers = *i as usize,
        Some(Value::Integer(_)) => return Err(format!("{w}: 'workers' fuera de rango [1, 128]")),
        Some(Value::String(s)) if s == "auto" => {}
        Some(_) => return Err(format!("{w}: 'workers' debe ser \"auto\" o un entero")),
    }
    if let Some(s) = get_str(g, "stats_socket", PATH_MAX, false, w)? {
        c.stats_socket = s;
    }
    if let Some(s) = get_str(g, "log_file", PATH_MAX, false, w)? {
        c.log_file = resolve_path(base_dir, &s);
    }
    if let Some(b) = get_bool(g, "access_log", w)? {
        c.access_log = b;
    }
    if let Some(l) = get_str(g, "log_level", 16, false, w)? {
        c.log_level = log::level_from_str(&l).ok_or_else(|| format!("{w}: log_level '{l}' inválido"))?;
    }
    Ok(())
}

fn parse_timeouts(root: &Table, c: &mut Config) -> R<()> {
    let w = "[timeouts]";
    let Some(t) = get_table(root, "timeouts", "raíz")? else { return Ok(()) };
    check_keys(t, w, &["client_header", "client_idle", "upstream_connect", "upstream_read", "upstream_idle"])?;
    let mx = 3600 * 1000;
    let to = &mut c.timeouts;
    for (k, slot) in [
        ("client_header", &mut to.client_header),
        ("client_idle", &mut to.client_idle),
        ("upstream_connect", &mut to.upstream_connect),
        ("upstream_read", &mut to.upstream_read),
        ("upstream_idle", &mut to.upstream_idle),
    ] {
        if let Some(v) = get_int(t, k, 1, mx, w)? {
            *slot = v as u64;
        }
    }
    Ok(())
}

fn parse_frontend(t: &Table, idx: usize, base_dir: &str) -> R<CfgFrontend> {
    let w0 = format!("[[frontend]] #{}", idx + 1);
    check_keys(t, &w0, &["name", "listen", "tls", "certs", "default_cert", "route"])?;
    let name = get_str(t, "name", NAME_MAX, true, &w0)?.unwrap();
    let w = format!("frontend '{name}'");
    let listen = get_str(t, "listen", ADDR_MAX, true, &w)?.unwrap();
    let tls = get_bool(t, "tls", &w)?.unwrap_or(false);
    let default_cert = get_int(t, "default_cert", 0, 1000, &w)?.unwrap_or(0) as usize;
    let addr = addr_parse(&listen, true).map_err(|e| format!("{w}: listen: {e}"))?;

    let mut certs = Vec::new();
    match t.get("certs") {
        None => {}
        Some(Value::Array(a)) => {
            for (i, v) in a.iter().enumerate() {
                let cw = format!("{w}: certs[{i}]");
                let Value::Table(ct) = v else { return Err(format!("{cw}: debe ser una tabla")) };
                check_keys(ct, &cw, &["sni", "cert", "key"])?;
                let sni = get_str(ct, "sni", HOST_MAX, true, &cw)?.unwrap();
                let cert = get_str(ct, "cert", PATH_MAX, true, &cw)?.unwrap();
                let key = get_str(ct, "key", PATH_MAX, true, &cw)?.unwrap();
                if sni == "default" || !valid_host_pattern(&sni) {
                    return Err(format!("{cw}: sni '{sni}' inválido"));
                }
                certs.push(CfgCert {
                    sni: sni.to_ascii_lowercase(),
                    cert: resolve_path(base_dir, &cert),
                    key: resolve_path(base_dir, &key),
                });
            }
        }
        Some(_) => return Err(format!("{w}: 'certs' debe ser un array")),
    }
    if tls && certs.is_empty() {
        return Err(format!("{w}: tls = true requiere al menos un certificado"));
    }
    if !tls && !certs.is_empty() {
        return Err(format!("{w}: 'certs' definido pero tls = false"));
    }
    if tls && default_cert >= certs.len() {
        return Err(format!("{w}: default_cert {default_cert} fuera de rango"));
    }

    let Some(rts) = get_tables(t, "route", &w)? else {
        return Err(format!("{w}: sin rutas ([[frontend.route]])"));
    };
    let mut routes: Vec<CfgRoute> = Vec::new();
    for (i, rt) in rts.iter().enumerate() {
        let rw = format!("{w}: route[{i}]");
        check_keys(rt, &rw, &["host", "backend"])?;
        let host = get_str(rt, "host", HOST_MAX, true, &rw)?.unwrap();
        let backend = get_str(rt, "backend", NAME_MAX, true, &rw)?.unwrap();
        if !valid_host_pattern(&host) {
            return Err(format!("{rw}: host '{host}' inválido"));
        }
        let host = host.to_ascii_lowercase();
        if routes.iter().any(|r| r.host == host) {
            return Err(format!("{w}: host '{host}' duplicado"));
        }
        routes.push(CfgRoute { host, backend });
    }
    if routes.iter().filter(|r| r.host == "default").count() > 1 {
        return Err(format!("{w}: más de una ruta 'default'"));
    }
    if routes.is_empty() {
        return Err(format!("{w}: sin rutas"));
    }
    Ok(CfgFrontend { name, listen, key: addr.to_string(), addr, tls, certs, default_cert, routes })
}

fn parse_backend(t: &Table, idx: usize) -> R<CfgBackend> {
    let w0 = format!("[[backend]] #{}", idx + 1);
    check_keys(t, &w0, &["name", "strategy", "max_idle", "servers", "health"])?;
    let name = get_str(t, "name", NAME_MAX, true, &w0)?.unwrap();
    let w = format!("backend '{name}'");
    let strat = get_str(t, "strategy", 32, false, &w)?.unwrap_or_else(|| "round_robin".into());
    let strategy = STRATEGIES
        .iter()
        .find(|(n, _)| *n == strat)
        .map(|(_, s)| *s)
        .ok_or_else(|| format!("{w}: strategy '{strat}' desconocida"))?;
    let max_idle = get_int(t, "max_idle", 0, 100_000, &w)?.unwrap_or(64) as usize;

    let servers_t = get_tables(t, "servers", &w)?.unwrap_or_default();
    if servers_t.is_empty() {
        return Err(format!("{w}: necesita al menos un servidor"));
    }
    if servers_t.len() > MAX_SERVERS {
        return Err(format!("{w}: máximo {MAX_SERVERS} servidores"));
    }
    let mut servers = Vec::new();
    for (i, st) in servers_t.iter().enumerate() {
        let sw = format!("{w}: servers[{i}]");
        check_keys(st, &sw, &["addr", "weight"])?;
        let addr = get_str(st, "addr", ADDR_MAX, true, &sw)?.unwrap();
        let weight = get_int(st, "weight", 1, 1000, &sw)?.unwrap_or(1) as i32;
        let sa = addr_parse(&addr, true).map_err(|e| format!("{sw}: {e}"))?;
        servers.push(CfgServer { addr, sa, weight });
    }

    let mut h = CfgHealth {
        kind: HealthType::None,
        path: "/health".into(),
        interval_ms: 2000,
        timeout_ms: 500,
        rise: 2,
        fall: 3,
    };
    if let Some(ht) = get_table(t, "health", &w)? {
        let hw = format!("{w}: health");
        check_keys(ht, &hw, &["type", "path", "interval", "timeout", "rise", "fall"])?;
        let kind = get_str(ht, "type", 16, false, &hw)?.unwrap_or_else(|| "tcp".into());
        h.kind = match kind.as_str() {
            "tcp" => HealthType::Tcp,
            "http" => HealthType::Http,
            "none" => HealthType::None,
            _ => return Err(format!("{hw}: type '{kind}' desconocido")),
        };
        if let Some(p) = get_str(ht, "path", HOST_MAX, false, &hw)? {
            h.path = p;
        }
        if let Some(v) = get_int(ht, "interval", 100, 3_600_000, &hw)? {
            h.interval_ms = v as u64;
        }
        if let Some(v) = get_int(ht, "timeout", 10, 60_000, &hw)? {
            h.timeout_ms = v as u64;
        }
        if let Some(v) = get_int(ht, "rise", 1, 100, &hw)? {
            h.rise = v as i32;
        }
        if let Some(v) = get_int(ht, "fall", 1, 100, &hw)? {
            h.fall = v as i32;
        }
        if !h.path.starts_with('/') {
            return Err(format!("{hw}: path debe empezar por '/'"));
        }
    }
    Ok(CfgBackend { name, strategy, max_idle, servers, health: h })
}

fn validate(c: &Config) -> R<()> {
    if c.frontends.is_empty() {
        return Err("no hay ningún [[frontend]]".into());
    }
    if c.backends.is_empty() {
        return Err("no hay ningún [[backend]]".into());
    }
    for (i, f) in c.frontends.iter().enumerate() {
        for g in &c.frontends[..i] {
            if f.name == g.name {
                return Err(format!("frontend '{}' duplicado", f.name));
            }
            if f.key == g.key {
                return Err(format!("frontends '{}' y '{}' escuchan en el mismo puerto ({})", g.name, f.name, f.key));
            }
        }
        for r in &f.routes {
            if c.find_backend(&r.backend).is_none() {
                return Err(format!(
                    "frontend '{}': ruta '{}' apunta al backend inexistente '{}'",
                    f.name, r.host, r.backend
                ));
            }
        }
    }
    for (i, b) in c.backends.iter().enumerate() {
        if c.backends[..i].iter().any(|x| x.name == b.name) {
            return Err(format!("backend '{}' duplicado", b.name));
        }
    }
    Ok(())
}

/// Parsea y valida el texto TOML. `base_dir` resuelve rutas relativas
/// (certificados, log).
pub fn parse(text: &str, base_dir: &str) -> R<Config> {
    let root: Table = text.parse().map_err(|e: toml::de::Error| {
        format!("TOML inválido: {}", e.to_string().split_whitespace().collect::<Vec<_>>().join(" "))
    })?;
    let mut c = Config {
        workers: default_workers(),
        stats_socket: "/tmp/proxy.sock".into(),
        log_file: String::new(),
        log_level: log::INFO,
        access_log: true,
        timeouts: Timeouts {
            client_header: 10_000,
            client_idle: 60_000,
            upstream_connect: 2000,
            upstream_read: 30_000,
            upstream_idle: 30_000,
        },
        frontends: Vec::new(),
        backends: Vec::new(),
    };
    check_keys(&root, "raíz", &["global", "timeouts", "frontend", "backend"])?;
    parse_global(&root, &mut c, base_dir)?;
    parse_timeouts(&root, &mut c)?;
    for (i, t) in get_tables(&root, "frontend", "raíz")?.unwrap_or_default().iter().enumerate() {
        c.frontends.push(parse_frontend(t, i, base_dir)?);
    }
    for (i, t) in get_tables(&root, "backend", "raíz")?.unwrap_or_default().iter().enumerate() {
        c.backends.push(parse_backend(t, i)?);
    }
    validate(&c)?;
    Ok(c)
}

/// Directorio que contiene `path` ("." si no tiene).
pub fn base_dir(path: &str) -> String {
    match path.rfind('/') {
        Some(0) => "/".into(),
        Some(i) => path[..i].into(),
        None => ".".into(),
    }
}

pub fn load(path: &str) -> R<(Config, String)> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("no se puede leer '{path}': {e}"))?;
    let c = parse(&text, &base_dir(path))?;
    Ok((c, text))
}

#[cfg(test)]
mod tests {
    use super::*;

    const BACKEND_OK: &str = "[[backend]]\nname = \"b\"\nservers = [ { addr = \"127.0.0.1:9001\" } ]\n";
    const FRONTEND_OK: &str = "[[frontend]]\nname = \"f\"\nlisten = \"127.0.0.1:8080\"\n\
                               [[frontend.route]]\nhost = \"default\"\nbackend = \"b\"\n";

    fn p(t: &str) -> R<Config> {
        parse(t, "/etc/proxy")
    }

    fn expect_error(text: &str, needle: &str) {
        match p(text) {
            Ok(_) => panic!("se esperaba error '{needle}'"),
            Err(e) => assert!(e.contains(needle), "error '{e}' no contiene '{needle}'"),
        }
    }

    #[test]
    fn full_example() {
        let t = "[global]\nworkers = 4\nstats_socket = \"/tmp/x.sock\"\nlog_file = \"p.log\"\n\
                 log_level = \"warn\"\naccess_log = false\n\
                 [timeouts]\nclient_idle = 1234\n\
                 [[frontend]]\nname = \"tls\"\nlisten = \"0.0.0.0:8443\"\ntls = true\n\
                 certs = [ { sni = \"*.Example.com\", cert = \"c.pem\", key = \"/abs/k.pem\" } ]\n\
                 [[frontend.route]]\nhost = \"API.test\"\nbackend = \"api\"\n\
                 [[frontend.route]]\nhost = \"default\"\nbackend = \"api\"\n\
                 [[backend]]\nname = \"api\"\nstrategy = \"least_load\"\nmax_idle = 8\n\
                 servers = [ { addr = \"127.0.0.1:9001\", weight = 3 }, { addr = \"[::1]:9002\" } ]\n\
                 [backend.health]\ntype = \"http\"\npath = \"/hc\"\ninterval = 1000\n";
        let c = p(t).unwrap();
        assert_eq!(c.workers, 4);
        assert_eq!(c.log_file, "/etc/proxy/p.log");
        assert_eq!(c.log_level, log::WARN);
        assert!(!c.access_log);
        assert_eq!(c.timeouts.client_idle, 1234);
        assert_eq!(c.timeouts.client_header, 10_000);
        let f = &c.frontends[0];
        assert!(f.tls);
        assert_eq!(f.key, "0.0.0.0:8443");
        assert_eq!(f.certs[0].sni, "*.example.com");
        assert_eq!(f.certs[0].cert, "/etc/proxy/c.pem");
        assert_eq!(f.certs[0].key, "/abs/k.pem");
        assert_eq!(f.routes[0].host, "api.test");
        let b = &c.backends[0];
        assert_eq!(b.strategy, Strategy::LeastLoad);
        assert_eq!(b.max_idle, 8);
        assert_eq!(b.servers.len(), 2);
        assert_eq!(b.servers[0].weight, 3);
        assert_eq!(b.servers[1].weight, 1);
        assert_eq!(b.servers[1].sa.to_string(), "[::1]:9002");
        assert_eq!(b.health.kind, HealthType::Http);
        assert_eq!(b.health.path, "/hc");
        assert_eq!(b.health.fall, 3);
    }

    #[test]
    fn minimal_defaults() {
        let c = p(&format!("{FRONTEND_OK}{BACKEND_OK}")).unwrap();
        assert!(c.workers >= 1);
        assert_eq!(c.backends[0].strategy, Strategy::RoundRobin);
        assert_eq!(c.backends[0].health.kind, HealthType::None);
        assert_eq!(c.stats_socket, "/tmp/proxy.sock");
    }

    #[test]
    fn validation_rules() {
        let fo = FRONTEND_OK;
        let bo = BACKEND_OK;
        let fe = |extra: &str| format!("[[frontend]]\nname = \"f\"\nlisten = \"127.0.0.1:8080\"\n{extra}");
        expect_error("x = = 1", "TOML inválido");
        expect_error(bo, "no hay ningún [[frontend]]");
        expect_error(fo, "no hay ningún [[backend]]");
        expect_error(&format!("{fo}{bo}{bo}"), "backend 'b' duplicado");
        expect_error(&format!("{fo}{fo}{bo}"), "frontend 'f' duplicado");
        expect_error(
            &format!(
                "{fo}[[frontend]]\nname = \"g\"\nlisten = \"127.0.0.1:8080\"\n[[frontend.route]]\nhost = \"default\"\nbackend = \"b\"\n{bo}"
            ),
            "mismo puerto",
        );
        expect_error(
            &format!("{}{bo}", fe("[[frontend.route]]\nhost = \"a\"\nbackend = \"nope\"\n")),
            "backend inexistente 'nope'",
        );
        expect_error(
            &format!(
                "{}{bo}",
                fe(
                    "[[frontend.route]]\nhost = \"default\"\nbackend = \"b\"\n[[frontend.route]]\nhost = \"default\"\nbackend = \"b\"\n"
                )
            ),
            "duplicado",
        );
        expect_error(
            &format!(
                "[[frontend]]\nname = \"f\"\nlisten = \"127.0.0.1:99999\"\n[[frontend.route]]\nhost = \"default\"\nbackend = \"b\"\n{bo}"
            ),
            "puerto inválido",
        );
        expect_error(
            &format!("{}{bo}", fe("tls = true\n[[frontend.route]]\nhost = \"default\"\nbackend = \"b\"\n")),
            "requiere al menos un certificado",
        );
        expect_error(&format!("{fo}[[backend]]\nname = \"b\"\nservers = []\n"), "al menos un servidor");
        expect_error(
            &format!(
                "{fo}[[backend]]\nname = \"b\"\nstrategy = \"random\"\nservers = [ {{ addr = \"127.0.0.1:1\" }} ]\n"
            ),
            "strategy 'random'",
        );
        expect_error(
            &format!("{fo}[[backend]]\nname = \"b\"\nservers = [ {{ addr = \"127.0.0.1:1\", weight = 0 }} ]\n"),
            "fuera de rango",
        );
        expect_error(&format!("{fo}{bo}[global]\nworkerz = 2\n"), "clave desconocida 'workerz'");
        expect_error(
            &format!("{}{bo}", fe("[[frontend.route]]\nhost = \"bad host\"\nbackend = \"b\"\n")),
            "host 'bad host' inválido",
        );
        expect_error(&format!("{fo}{bo}[timeouts]\nclient_idle = 0\n"), "fuera de rango");
        expect_error(
            &format!("{fo}[[backend]]\nname = \"b\"\nservers = [ {{ addr = \"127.0.0.1\" }} ]\n"),
            "sin puerto",
        );
        expect_error(&format!("{fo}{bo}[global]\nworkers = \"many\"\n"), "\"auto\" o un entero");
        expect_error(&format!("{fo}{bo}[global]\nlog_level = 3\n"), "debe ser una cadena");
    }

    #[test]
    fn base_dirs() {
        assert_eq!(base_dir("/etc/proxy/p.toml"), "/etc/proxy");
        assert_eq!(base_dir("/p.toml"), "/");
        assert_eq!(base_dir("p.toml"), ".");
    }
}
