//! Utilidades: relojes, direcciones y comparación de cadenas.

use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::sync::OnceLock;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

static T0: OnceLock<Instant> = OnceLock::new();

/// Milisegundos de reloj monótono desde el arranque del proceso (> 0).
pub fn mono_ms() -> u64 {
    let t0 = *T0.get_or_init(Instant::now);
    t0.elapsed().as_millis() as u64 + 1
}

/// Milisegundos de reloj de pared (epoch).
pub fn wall_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

/// Igualdad sin distinguir mayúsculas (ASCII).
pub fn ieq(a: &[u8], b: &str) -> bool {
    a.eq_ignore_ascii_case(b.as_bytes())
}

/// Parsea "host:puerto" o "[v6]:puerto". Si `resolve` es false solo acepta
/// IP numérica. Un host vacío o "*" es 0.0.0.0.
pub fn addr_parse(s: &str, resolve: bool) -> Result<SocketAddr, String> {
    let (host, port) = if let Some(rest) = s.strip_prefix('[') {
        match rest.find("]:") {
            Some(end) => (&rest[..end], &rest[end + 2..]),
            None => return Err(format!("dirección inválida '{s}'")),
        }
    } else {
        match s.rfind(':') {
            Some(c) => (&s[..c], &s[c + 1..]),
            None => return Err(format!("dirección '{s}' sin puerto")),
        }
    };
    let port: u16 = match port.parse::<u32>() {
        Ok(p) if (1..=65535).contains(&p) => p as u16,
        _ => return Err(format!("puerto inválido en '{s}'")),
    };
    let host = if host.is_empty() || host == "*" { "0.0.0.0" } else { host };
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(SocketAddr::new(ip, port));
    }
    if !resolve {
        return Err(format!("'{host}' no es una IP numérica"));
    }
    match (host, port).to_socket_addrs() {
        Ok(mut it) => it.next().ok_or_else(|| format!("no se puede resolver '{host}'")),
        Err(e) => Err(format!("no se puede resolver '{host}': {e}")),
    }
}

/// xorshift64*
pub fn rng_next(s: &mut u64) -> u64 {
    let mut x = *s;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    *s = x;
    x.wrapping_mul(0x2545_F491_4F6C_DD1D)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn addresses() {
        assert_eq!(addr_parse("127.0.0.1:80", false).unwrap().to_string(), "127.0.0.1:80");
        assert_eq!(addr_parse("[::1]:9002", false).unwrap().to_string(), "[::1]:9002");
        assert_eq!(addr_parse("*:8080", false).unwrap().to_string(), "0.0.0.0:8080");
        assert_eq!(addr_parse(":8080", false).unwrap().to_string(), "0.0.0.0:8080");
        assert!(addr_parse("127.0.0.1", false).unwrap_err().contains("sin puerto"));
        assert!(addr_parse("127.0.0.1:99999", false).unwrap_err().contains("puerto inválido"));
        assert!(addr_parse("127.0.0.1:0", false).is_err());
        assert!(addr_parse("[::1:80", false).is_err());
        assert!(addr_parse("localhost:80", false).is_err());
        assert!(addr_parse("localhost:80", true).is_ok());
    }

    #[test]
    fn clocks() {
        let a = mono_ms();
        assert!(a > 0);
        assert!(mono_ms() >= a);
        assert!(wall_ms() > 1_600_000_000_000);
    }
}
