//! Terminación TLS (rustls). Cada frontend TLS tiene un resolvedor que elige
//! el certificado según el SNI (exacto, luego wildcard de una etiqueta,
//! luego default_cert). La configuración es inmutable y se comparte entre
//! todos los hilos worker.

use std::fmt;
use std::sync::Arc;

use rustls::ServerConfig;
use rustls::crypto::CryptoProvider;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;

use crate::config::{CfgCert, CfgFrontend, Config};

/// Buffer máximo de texto plano pendiente de cifrar por conexión
/// (contrapresión hacia el resto del proxy).
pub const TLS_BUFFER_LIMIT: usize = 64 * 1024;

/// Casa un nombre con un patrón de certificado: exacto o "*.dom" de una
/// sola etiqueta (RFC 6125).
pub fn pattern_match(pattern: &str, host: &[u8]) -> bool {
    if let Some(suffix) = pattern.strip_prefix('*').filter(|s| s.starts_with('.')) {
        let s = suffix.as_bytes();
        if host.len() <= s.len() || !host[host.len() - s.len()..].eq_ignore_ascii_case(s) {
            return false;
        }
        // el comodín cubre exactamente una etiqueta
        return !host[..host.len() - s.len()].contains(&b'.');
    }
    pattern.as_bytes().eq_ignore_ascii_case(host)
}

pub struct SniResolver {
    patterns: Vec<String>,
    keys: Vec<Arc<CertifiedKey>>,
    def: usize,
}

impl fmt::Debug for SniResolver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SniResolver").field("patterns", &self.patterns).field("def", &self.def).finish()
    }
}

impl SniResolver {
    /// Índice del certificado para un SNI (o el de por defecto).
    pub fn choose(&self, name: Option<&str>) -> usize {
        let Some(name) = name else { return self.def };
        let h = name.as_bytes();
        if let Some(i) = self.patterns.iter().position(|p| !p.starts_with('*') && pattern_match(p, h)) {
            return i;
        }
        self.patterns
            .iter()
            .enumerate()
            .filter(|(_, p)| p.starts_with('*') && pattern_match(p, h))
            .max_by_key(|(_, p)| p.len())
            .map(|(i, _)| i)
            .unwrap_or(self.def)
    }

    pub fn pattern(&self, i: usize) -> &str {
        &self.patterns[i]
    }
}

impl ResolvesServerCert for SniResolver {
    fn resolve(&self, hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(self.keys[self.choose(hello.server_name())].clone())
    }
}

pub struct TlsFrontend {
    pub resolver: Arc<SniResolver>,
    pub config: Arc<ServerConfig>,
}

/// Proveedor criptográfico elegido en la compilación (feature `aws-lc` o `ring`).
pub fn provider() -> Arc<CryptoProvider> {
    #[cfg(feature = "aws-lc")]
    let p = rustls::crypto::aws_lc_rs::default_provider();
    #[cfg(all(feature = "ring", not(feature = "aws-lc")))]
    let p = rustls::crypto::ring::default_provider();
    Arc::new(p)
}

fn load_key(c: &CfgCert, prov: &CryptoProvider) -> Result<Arc<CertifiedKey>, String> {
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(&c.cert)
        .and_then(|it| it.collect())
        .map_err(|e| format!("no se puede cargar el certificado '{}': {e}", c.cert))?;
    if certs.is_empty() {
        return Err(format!("no se puede cargar el certificado '{}': sin certificados PEM", c.cert));
    }
    let key =
        PrivateKeyDer::from_pem_file(&c.key).map_err(|e| format!("no se puede cargar la clave '{}': {e}", c.key))?;
    CertifiedKey::from_der(certs, key, prov)
        .map(Arc::new)
        .map_err(|e| format!("la clave no corresponde al certificado '{}': {e}", c.cert))
}

impl TlsFrontend {
    pub fn new(fe: &CfgFrontend) -> Result<TlsFrontend, String> {
        let prov = provider();
        let keys = fe
            .certs
            .iter()
            .map(|c| load_key(c, &prov))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("frontend '{}': {e}", fe.name))?;
        let resolver = Arc::new(SniResolver {
            patterns: fe.certs.iter().map(|c| c.sni.clone()).collect(),
            keys,
            def: fe.default_cert,
        });
        let mut config = ServerConfig::builder_with_provider(prov)
            .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
            .map_err(|e| format!("frontend '{}': {e}", fe.name))?
            .with_no_client_auth()
            .with_cert_resolver(resolver.clone());
        config.ignore_client_order = true; // preferencia de cifrados del servidor
        Ok(TlsFrontend { resolver, config: Arc::new(config) })
    }
}

/// Comprueba que todos los frontends TLS cargan sus certificados.
pub fn validate_config(c: &Config) -> Result<(), String> {
    for f in c.frontends.iter().filter(|f| f.tls) {
        TlsFrontend::new(f)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn patterns() {
        assert!(pattern_match("api.test", b"api.test"));
        assert!(pattern_match("api.test", b"API.TEST"));
        assert!(pattern_match("*.example.com", b"a.example.com"));
        assert!(!pattern_match("*.example.com", b"a.b.example.com"), "una etiqueta");
        assert!(!pattern_match("*.example.com", b"example.com"));
        assert!(!pattern_match("api.test", b"api.tes"));
    }

    #[test]
    fn choose_by_sni() {
        let r = SniResolver {
            patterns: vec![
                "default.test".into(),
                "*.example.com".into(),
                "*.eu.example.com".into(),
                "www.example.com".into(),
            ],
            keys: Vec::new(),
            def: 0,
        };
        assert_eq!(r.choose(None), 0);
        assert_eq!(r.choose(Some("zzz.test")), 0);
        assert_eq!(r.choose(Some("a.example.com")), 1);
        assert_eq!(r.choose(Some("x.eu.example.com")), 2);
        assert_eq!(r.choose(Some("www.example.com")), 3, "exacto antes que wildcard");
        assert_eq!(r.choose(Some("a.b.c.example.com")), 0, "wildcard de una etiqueta");
    }
}
