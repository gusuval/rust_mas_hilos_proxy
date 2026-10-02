//! Cliente WebSocket mínimo para los tests de integración.
//!
//!   ws_probe [--tls] [--ca FICHERO] [--sni NOMBRE] [--host HOST]
//!            IP PUERTO RUTA MENSAJE
//!
//! Hace el handshake, envía MENSAJE como frame de texto enmascarado,
//! comprueba que el eco es idéntico, envía un close y sale con 0.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::exit;
use std::sync::Arc;
use std::time::Duration;

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName};
use rustls::{ClientConfig, ClientConnection, RootCertStore, StreamOwned};

fn die(m: &str) -> ! {
    eprintln!("ws_probe: {m}");
    exit(1);
}

/// Verificador que acepta cualquier certificado (solo sin --ca).
#[derive(Debug)]
struct NoVerify(Arc<rustls::crypto::CryptoProvider>);

impl rustls::client::danger::ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _: &CertificateDer<'_>,
        _: &[CertificateDer<'_>],
        _: &ServerName<'_>,
        _: &[u8],
        _: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        m: &[u8],
        c: &CertificateDer<'_>,
        d: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(m, c, d, &self.0.signature_verification_algorithms)
    }
    fn verify_tls13_signature(
        &self,
        m: &[u8],
        c: &CertificateDer<'_>,
        d: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(m, c, d, &self.0.signature_verification_algorithms)
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

enum Io {
    Plain(TcpStream),
    Tls(Box<StreamOwned<ClientConnection, TcpStream>>),
}

impl Io {
    fn write_all(&mut self, b: &[u8]) -> std::io::Result<()> {
        match self {
            Io::Plain(s) => s.write_all(b),
            Io::Tls(s) => s.write_all(b).and_then(|_| s.flush()),
        }
    }
    fn read(&mut self, b: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Io::Plain(s) => s.read(b),
            Io::Tls(s) => s.read(b),
        }
    }
}

/// Lee exactamente n bytes, empezando por lo ya recibido en `pre`.
fn read_exact(io: &mut Io, n: usize, pre: &mut Vec<u8>) -> Vec<u8> {
    let mut out: Vec<u8> = pre.drain(..n.min(pre.len())).collect();
    let mut buf = [0u8; 4096];
    while out.len() < n {
        match io.read(&mut buf[..(n - out.len()).min(4096)]) {
            Ok(0) | Err(_) => die("lectura del eco"),
            Ok(k) => out.extend_from_slice(&buf[..k]),
        }
    }
    out
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let (mut tls, mut ca, mut sni, mut host) = (false, None, None, None);
    let mut pos = Vec::new();
    let mut it = args.iter().skip(1);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--tls" => tls = true,
            "--ca" => ca = it.next().cloned(),
            "--sni" => sni = it.next().cloned(),
            "--host" => host = it.next().cloned(),
            _ => pos.push(a.clone()),
        }
    }
    if pos.len() != 4 {
        eprintln!("uso: ws_probe [--tls] [--ca F] [--sni N] [--host H] IP PUERTO RUTA MENSAJE");
        exit(2);
    }
    let (ip, port, path, msg) = (&pos[0], &pos[1], &pos[2], &pos[3]);
    let host = host.or_else(|| sni.clone()).unwrap_or_else(|| ip.clone());

    let sock = TcpStream::connect(format!("{ip}:{port}")).unwrap_or_else(|_| die("connect"));
    let _ = sock.set_read_timeout(Some(Duration::from_secs(5)));
    let _ = sock.set_write_timeout(Some(Duration::from_secs(5)));
    let mut io = if tls {
        let prov = proxy::tls::provider();
        let builder = ClientConfig::builder_with_provider(prov.clone()).with_safe_default_protocol_versions().unwrap();
        let cfg = match &ca {
            Some(ca) => {
                let mut roots = RootCertStore::empty();
                for c in CertificateDer::pem_file_iter(ca).unwrap_or_else(|_| die("CA ilegible")) {
                    roots.add(c.unwrap_or_else(|_| die("CA inválida"))).unwrap_or_else(|_| die("CA inválida"));
                }
                builder.with_root_certificates(roots).with_no_client_auth()
            }
            None => {
                builder.dangerous().with_custom_certificate_verifier(Arc::new(NoVerify(prov))).with_no_client_auth()
            }
        };
        let name =
            ServerName::try_from(sni.clone().unwrap_or_else(|| ip.clone())).unwrap_or_else(|_| die("SNI inválido"));
        let conn = ClientConnection::new(Arc::new(cfg), name).unwrap_or_else(|e| die(&format!("TLS: {e}")));
        Io::Tls(Box::new(StreamOwned::new(conn, sock)))
    } else {
        Io::Plain(sock)
    };

    let req = format!(
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
         Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
    );
    if let Err(e) = io.write_all(req.as_bytes()) {
        die(&format!("envío del handshake: {e}"));
    }
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    let end = loop {
        if let Some(p) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break p + 4;
        }
        match io.read(&mut tmp) {
            Ok(0) | Err(_) => die("respuesta del handshake"),
            Ok(k) => buf.extend_from_slice(&tmp[..k]),
        }
    };
    let head = String::from_utf8_lossy(&buf[..end]).into_owned();
    if !head.starts_with("HTTP/1.1 101") {
        eprintln!("ws_probe: esperaba 101, recibido:\n{head}");
        exit(1);
    }
    if !head.contains("s3pPLMBiTxaQ9kYGzzhZRbK+xOo=") {
        die("Sec-WebSocket-Accept incorrecto");
    }
    let mut rest = buf[end..].to_vec();

    let m = msg.as_bytes();
    if m.len() > 65535 {
        die("mensaje demasiado largo");
    }
    let mut frame = vec![0x81u8];
    if m.len() < 126 {
        frame.push(0x80 | m.len() as u8);
    } else {
        frame.push(0x80 | 126);
        frame.extend_from_slice(&(m.len() as u16).to_be_bytes());
    }
    let mask = [0x12u8, 0x34, 0x56, 0x78];
    frame.extend_from_slice(&mask);
    frame.extend(m.iter().enumerate().map(|(i, b)| b ^ mask[i & 3]));
    if io.write_all(&frame).is_err() {
        die("envío del frame");
    }

    let h = read_exact(&mut io, 2, &mut rest);
    let mut pl = (h[1] & 0x7f) as usize;
    if pl == 126 {
        let e = read_exact(&mut io, 2, &mut rest);
        pl = u16::from_be_bytes([e[0], e[1]]) as usize;
    }
    let payload = read_exact(&mut io, pl, &mut rest);
    if h[0] & 0x0f != 1 || payload != m {
        eprintln!("ws_probe: eco distinto: '{}'", String::from_utf8_lossy(&payload));
        exit(1);
    }
    let _ = io.write_all(&[0x88, 0x80, 0, 0, 0, 0]);
    println!("ws ok: {}", String::from_utf8_lossy(&payload));
}
