//! Parser HTTP/1.x incremental.
//!
//! La cabecera se parsea cuando está completa (\r\n\r\n); mientras tanto se
//! guarda la posición ya escaneada para no volver a recorrer bytes, así que
//! tolera cualquier fragmentación. Los campos son rangos dentro del buffer
//! del llamante (sin copias).
//!
//! El cuerpo se delimita con `Body` (Content-Length, chunked o cierre), que
//! solo sigue los límites: los bytes se reenvían tal cual.

use crate::util::ieq;

pub const MAX_HEADERS: usize = 100;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Parse {
    Complete,
    Incomplete,
    Error,
    TooLarge,
}

/// Rango [start, end) dentro del buffer analizado.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub s: u32,
    pub e: u32,
}

impl Span {
    fn new(s: usize, e: usize) -> Span {
        Span { s: s as u32, e: e as u32 }
    }
    pub fn get<'a>(&self, buf: &'a [u8]) -> &'a [u8] {
        &buf[self.s as usize..self.e as usize]
    }
    pub fn len(&self) -> usize {
        (self.e - self.s) as usize
    }
    pub fn is_empty(&self) -> bool {
        self.e == self.s
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct Header {
    pub name: Span,
    pub value: Span,
}

#[derive(Debug, Clone)]
pub struct Msg {
    // petición
    pub method: Span,
    pub target: Span,
    // respuesta
    pub status: u16,
    pub reason: Span,

    pub version_minor: u8,
    /// Bytes de la cabecera, incluido \r\n\r\n.
    pub head_len: usize,

    pub headers: [Header; MAX_HEADERS],
    pub nheaders: usize,

    // derivados
    /// Host sin puerto.
    pub host: Span,
    pub has_host: bool,
    /// -1 si no hay.
    pub content_length: i64,
    pub chunked: bool,
    pub has_te: bool,
    pub conn_close: bool,
    pub conn_keepalive: bool,
    pub conn_upgrade: bool,
    pub upgrade: Span,
    /// X-Backend-Load (respuestas).
    pub backend_load: Option<Span>,
}

impl Default for Msg {
    fn default() -> Self {
        Msg {
            method: Span::default(),
            target: Span::default(),
            status: 0,
            reason: Span::default(),
            version_minor: 1,
            head_len: 0,
            headers: [Header::default(); MAX_HEADERS],
            nheaders: 0,
            host: Span::default(),
            has_host: false,
            content_length: -1,
            chunked: false,
            has_te: false,
            conn_close: false,
            conn_keepalive: false,
            conn_upgrade: false,
            upgrade: Span::default(),
            backend_load: None,
        }
    }
}

/// tchar de RFC 9110.
fn is_tchar(c: u8) -> bool {
    c.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&c)
}

/// Busca el final de cabecera. Devuelve su longitud o 0 si no está.
fn find_head_end(buf: &[u8], scan: &mut usize) -> usize {
    let len = buf.len();
    let mut i = scan.saturating_sub(3);
    while i + 3 < len {
        match buf[i..len - 3].iter().position(|&c| c == b'\r') {
            None => break,
            Some(p) => {
                i += p;
                if &buf[i..i + 4] == b"\r\n\r\n" {
                    return i + 4;
                }
                i += 1;
            }
        }
    }
    *scan = len;
    0
}

fn trim(buf: &[u8], mut s: usize, mut e: usize) -> Span {
    while s < e && (buf[s] == b' ' || buf[s] == b'\t') {
        s += 1;
    }
    while e > s && (buf[e - 1] == b' ' || buf[e - 1] == b'\t') {
        e -= 1;
    }
    Span::new(s, e)
}

/// ¿La lista separada por comas contiene tok (sin distinguir mayúsculas)?
pub fn list_has(v: &[u8], tok: &[u8]) -> bool {
    v.split(|&c| c == b',').any(|t| t.trim_ascii().eq_ignore_ascii_case(tok))
}

impl Msg {
    pub fn header_value<'a>(&self, buf: &'a [u8], name: &str) -> Option<&'a [u8]> {
        self.headers[..self.nheaders].iter().find(|h| ieq(h.name.get(buf), name)).map(|h| h.value.get(buf))
    }

    pub fn conn_has_token(&self, buf: &[u8], tok: &[u8]) -> bool {
        self.headers[..self.nheaders]
            .iter()
            .any(|h| ieq(h.name.get(buf), "connection") && list_has(h.value.get(buf), tok))
    }

    /// ¿Cabecera hop-by-hop que no debe reenviarse? Incluye las nombradas en
    /// Connection:, salvo Host/Content-Length/Transfer-Encoding.
    pub fn is_hop_by_hop(&self, buf: &[u8], name: &[u8]) -> bool {
        const HOP: [&str; 8] = [
            "connection",
            "keep-alive",
            "proxy-connection",
            "te",
            "trailer",
            "upgrade",
            "proxy-authenticate",
            "proxy-authorization",
        ];
        if HOP.iter().any(|h| ieq(name, h)) {
            return true;
        }
        // Nunca eliminar cabeceras de encuadre ni Host aunque se nombren en
        // Connection (evita smuggling vía hop-by-hop).
        if ieq(name, "content-length") || ieq(name, "transfer-encoding") || ieq(name, "host") {
            return false;
        }
        self.conn_has_token(buf, name)
    }

    fn parse_headers(&mut self, buf: &[u8], mut p: usize, end: usize, is_request: bool) -> Parse {
        self.nheaders = 0;
        self.content_length = -1;
        self.chunked = false;
        self.has_te = false;
        self.has_host = false;
        self.conn_close = false;
        self.conn_keepalive = false;
        self.conn_upgrade = false;
        self.host = Span::default();
        self.upgrade = Span::default();
        self.backend_load = None;

        while p < end {
            let eol = match buf[p..end].iter().position(|&c| c == b'\r') {
                Some(k) => p + k,
                None => return Parse::Error,
            };
            if eol + 1 >= end || buf[eol + 1] != b'\n' {
                return Parse::Error;
            }
            if eol == p {
                break; // línea vacía: fin
            }
            if buf[p] == b' ' || buf[p] == b'\t' {
                return Parse::Error; // obs-fold
            }
            let mut colon = p;
            while colon < eol && is_tchar(buf[colon]) {
                colon += 1;
            }
            if colon == p || colon >= eol || buf[colon] != b':' {
                return Parse::Error; // nombre vacío o espacio antes de ':'
            }
            if buf[colon + 1..eol].iter().any(|&c| c == b'\n' || c == 0 || (c < 0x20 && c != b'\t') || c == 0x7f) {
                return Parse::Error;
            }
            if self.nheaders == MAX_HEADERS {
                return Parse::TooLarge;
            }
            let name = Span::new(p, colon);
            let value = trim(buf, colon + 1, eol);
            self.headers[self.nheaders] = Header { name, value };
            self.nheaders += 1;
            let n = name.get(buf);
            let v = value.get(buf);

            if ieq(n, "content-length") {
                if v.is_empty() || v.len() > 18 || !v.iter().all(u8::is_ascii_digit) {
                    return Parse::Error;
                }
                let cl = v.iter().fold(0i64, |a, &c| a * 10 + (c - b'0') as i64);
                if self.content_length >= 0 && self.content_length != cl {
                    return Parse::Error; // CL duplicados distintos
                }
                self.content_length = cl;
            } else if ieq(n, "transfer-encoding") {
                self.has_te = true;
                let last = v.rsplit(|&c| c == b',').next().unwrap_or(b"").trim_ascii();
                self.chunked = last.eq_ignore_ascii_case(b"chunked");
            } else if ieq(n, "connection") {
                self.conn_close |= list_has(v, b"close");
                self.conn_keepalive |= list_has(v, b"keep-alive");
                self.conn_upgrade |= list_has(v, b"upgrade");
            } else if ieq(n, "upgrade") {
                self.upgrade = value;
            } else if is_request && ieq(n, "host") {
                if self.has_host {
                    return Parse::Error; // Host duplicado
                }
                self.has_host = true;
                // quitar el puerto: "host:port" o "[v6]:port"
                let (vs, mut ve) = (value.s as usize, value.e as usize);
                if vs < ve && buf[vs] == b'[' {
                    match buf[vs..ve].iter().position(|&c| c == b']') {
                        Some(k) => ve = vs + k + 1,
                        None => return Parse::Error,
                    }
                } else if let Some(k) = buf[vs..ve].iter().position(|&c| c == b':') {
                    ve = vs + k;
                }
                if buf[vs..ve].iter().any(|&c| c == b' ' || c == b'/' || c == b'@') {
                    return Parse::Error;
                }
                self.host = Span::new(vs, ve);
            } else if !is_request && ieq(n, "x-backend-load") {
                self.backend_load = Some(value);
            }
            p = eol + 2;
        }

        if is_request {
            // Protección frente a request smuggling (RFC 9112 §6.3).
            if self.has_te && (self.content_length >= 0 || !self.chunked) {
                return Parse::Error;
            }
            if self.version_minor == 1 && !self.has_host {
                return Parse::Error;
            }
        } else if self.has_te {
            self.content_length = -1; // TE gana a CL en respuestas
        }
        Parse::Complete
    }
}

fn parse_version(v: &[u8]) -> Option<u8> {
    if v.len() != 8 || &v[..7] != b"HTTP/1." {
        return None;
    }
    match v[7] {
        b'0' => Some(0),
        b'1' => Some(1),
        _ => None,
    }
}

/// `scan`: estado entre llamadas (empezar en 0 mientras el buffer crece).
pub fn parse_request(m: &mut Msg, buf: &[u8], max_head: usize, scan: &mut usize) -> Parse {
    // RFC 9112 §2.2: ignorar CRLF iniciales
    let mut skip = 0;
    while skip + 1 < buf.len() && buf[skip] == b'\r' && buf[skip + 1] == b'\n' {
        skip += 2;
    }
    let hl = find_head_end(buf, scan);
    if hl == 0 {
        return if buf.len() > max_head { Parse::TooLarge } else { Parse::Incomplete };
    }
    if hl > max_head {
        return Parse::TooLarge;
    }
    if hl <= skip {
        return Parse::Error;
    }
    m.head_len = hl;
    let p = skip;
    let eol = match buf[p..hl].iter().position(|&c| c == b'\r') {
        Some(k) => p + k,
        None => return Parse::Error,
    };
    if buf[eol + 1] != b'\n' {
        return Parse::Error;
    }
    let line = &buf[p..eol];
    let sp1 = match line.iter().position(|&c| c == b' ') {
        Some(k) if k > 0 => p + k,
        _ => return Parse::Error,
    };
    if !buf[p..sp1].iter().all(|&c| is_tchar(c)) {
        return Parse::Error;
    }
    let tgt = sp1 + 1;
    let sp2 = match buf[tgt..eol].iter().position(|&c| c == b' ') {
        Some(k) if k > 0 => tgt + k,
        _ => return Parse::Error,
    };
    if buf[tgt..sp2].iter().any(|&c| c <= 0x20 || c == 0x7f) {
        return Parse::Error;
    }
    m.version_minor = match parse_version(&buf[sp2 + 1..eol]) {
        Some(v) => v,
        None => return Parse::Error,
    };
    if sp1 - p > 32 || sp2 - tgt > 8192 {
        return Parse::TooLarge;
    }
    m.method = Span::new(p, sp1);
    m.target = Span::new(tgt, sp2);
    m.status = 0;
    m.reason = Span::default();
    m.parse_headers(buf, eol + 2, hl, true)
}

pub fn parse_response(m: &mut Msg, buf: &[u8], max_head: usize, scan: &mut usize) -> Parse {
    let hl = find_head_end(buf, scan);
    if hl == 0 {
        return if buf.len() > max_head { Parse::TooLarge } else { Parse::Incomplete };
    }
    if hl > max_head {
        return Parse::TooLarge;
    }
    m.head_len = hl;
    let eol = match buf[..hl].iter().position(|&c| c == b'\r') {
        Some(k) => k,
        None => return Parse::Error,
    };
    if buf[eol + 1] != b'\n' || eol < 12 {
        return Parse::Error;
    }
    m.version_minor = match parse_version(&buf[..8]) {
        Some(v) if buf[8] == b' ' => v,
        _ => return Parse::Error,
    };
    if !buf[9..12].iter().all(u8::is_ascii_digit) {
        return Parse::Error;
    }
    m.status = buf[9..12].iter().fold(0u16, |a, &c| a * 10 + (c - b'0') as u16);
    if m.status < 100 {
        return Parse::Error;
    }
    if eol > 12 {
        if buf[12] != b' ' {
            return Parse::Error;
        }
        m.reason = Span::new(13, eol);
    } else {
        m.reason = Span::new(12, 12);
    }
    m.method = Span::default();
    m.target = Span::default();
    m.parse_headers(buf, eol + 2, hl, false)
}

// ---------------- cuerpo ----------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BodyMode {
    #[default]
    None,
    Length,
    Chunked,
    /// Delimitado por cierre (solo respuestas).
    Eof,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum Ch {
    #[default]
    Size,
    Ext,
    SizeLf,
    Data,
    DataCr,
    DataLf,
    Trailer,
    TrailerLn,
    EndLf,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Body {
    pub mode: BodyMode,
    /// Length: bytes restantes; Chunked: del chunk actual.
    pub remaining: u64,
    cstate: Ch,
    ndigits: u32,
    pub done: bool,
}

impl Body {
    pub fn new(mode: BodyMode, len: u64) -> Body {
        Body {
            mode,
            remaining: len,
            cstate: Ch::Size,
            ndigits: 0,
            done: mode == BodyMode::None || (mode == BodyMode::Length && len == 0),
        }
    }

    /// Consume bytes del cuerpo. Devuelve cuántos pertenecen al cuerpo (el
    /// resto es el mensaje siguiente) o None si el chunked es inválido.
    pub fn feed(&mut self, p: &[u8]) -> Option<usize> {
        if self.done {
            return Some(0);
        }
        match self.mode {
            BodyMode::None => {
                self.done = true;
                return Some(0);
            }
            BodyMode::Eof => return Some(p.len()),
            BodyMode::Length => {
                let k = (p.len() as u64).min(self.remaining);
                self.remaining -= k;
                if self.remaining == 0 {
                    self.done = true;
                }
                return Some(k as usize);
            }
            BodyMode::Chunked => {}
        }
        let n = p.len();
        let mut i = 0;
        while i < n && !self.done {
            let c = p[i];
            match self.cstate {
                Ch::Size => {
                    if let Some(v) = (c as char).to_digit(16) {
                        self.ndigits += 1;
                        if self.ndigits > 15 {
                            return None;
                        }
                        self.remaining = (self.remaining << 4) | v as u64;
                    } else if self.ndigits == 0 {
                        return None;
                    } else if c == b';' || c == b' ' || c == b'\t' {
                        self.cstate = Ch::Ext;
                    } else if c == b'\r' {
                        self.cstate = Ch::SizeLf;
                    } else {
                        return None;
                    }
                    i += 1;
                }
                Ch::Ext => {
                    if c == b'\r' {
                        self.cstate = Ch::SizeLf;
                    } else if c == b'\n' {
                        return None;
                    }
                    i += 1;
                }
                Ch::SizeLf => {
                    if c != b'\n' {
                        return None;
                    }
                    i += 1;
                    self.cstate = if self.remaining > 0 { Ch::Data } else { Ch::Trailer };
                }
                Ch::Data => {
                    let k = ((n - i) as u64).min(self.remaining);
                    i += k as usize;
                    self.remaining -= k;
                    if self.remaining == 0 {
                        self.cstate = Ch::DataCr;
                    }
                }
                Ch::DataCr => {
                    if c != b'\r' {
                        return None;
                    }
                    i += 1;
                    self.cstate = Ch::DataLf;
                }
                Ch::DataLf => {
                    if c != b'\n' {
                        return None;
                    }
                    i += 1;
                    self.cstate = Ch::Size;
                    self.ndigits = 0;
                    self.remaining = 0;
                }
                Ch::Trailer => {
                    if c == b'\r' {
                        self.cstate = Ch::EndLf;
                    } else if c == b'\n' {
                        return None;
                    } else {
                        self.cstate = Ch::TrailerLn;
                    }
                    i += 1;
                }
                Ch::TrailerLn => {
                    if c == b'\n' {
                        self.cstate = Ch::Trailer;
                    }
                    i += 1;
                }
                Ch::EndLf => {
                    if c != b'\n' {
                        return None;
                    }
                    i += 1;
                    self.done = true;
                }
            }
        }
        Some(i)
    }
}

pub fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        408 => "Request Timeout",
        421 => "Misdirected Request",
        431 => "Request Header Fields Too Large",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        _ => "Unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(s: &str, m: &mut Msg) -> Parse {
        let mut scan = 0;
        parse_request(m, s.as_bytes(), 16384, &mut scan)
    }

    #[test]
    fn simple_get() {
        let mut m = Msg::default();
        let r = "GET /index.html HTTP/1.1\r\nHost: Example.COM:8080\r\nAccept: */*\r\n\r\n";
        assert_eq!(req(r, &mut m), Parse::Complete);
        let b = r.as_bytes();
        assert_eq!(m.head_len, r.len());
        assert_eq!(m.method.get(b), b"GET");
        assert_eq!(m.target.get(b), b"/index.html");
        assert_eq!(m.version_minor, 1);
        assert_eq!(m.host.get(b), b"Example.COM");
        assert_eq!(m.content_length, -1);
        assert!(!m.chunked);
        assert_eq!(m.nheaders, 2);
    }

    #[test]
    fn fragmented_byte_by_byte() {
        let r = b"POST /x HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\n\r\nhello";
        let head = r.len() - 5;
        let mut scan = 0;
        let mut m = Msg::default();
        for i in 1..head {
            assert_eq!(parse_request(&mut m, &r[..i], 16384, &mut scan), Parse::Incomplete);
        }
        assert_eq!(parse_request(&mut m, &r[..head], 16384, &mut scan), Parse::Complete);
        assert_eq!(m.content_length, 5);
        assert_eq!(m.head_len, head);
    }

    #[test]
    fn fragmented_random() {
        let r = b"GET /a HTTP/1.1\r\nHost: h\r\nX-A: 1\r\nX-B: 2\r\n\r\n";
        let mut seed = 42u64;
        for _ in 0..200 {
            let (mut scan, mut have) = (0, 0);
            let mut m = Msg::default();
            let mut rc = Parse::Incomplete;
            while rc == Parse::Incomplete {
                have = (have + 1 + (crate::util::rng_next(&mut seed) % 7) as usize).min(r.len());
                rc = parse_request(&mut m, &r[..have], 16384, &mut scan);
                if have < r.len() {
                    assert_eq!(rc, Parse::Incomplete);
                }
            }
            assert_eq!(rc, Parse::Complete);
            assert_eq!(m.nheaders, 3);
        }
    }

    #[test]
    fn limits() {
        let mut big = String::from("GET / HTTP/1.1\r\nHost: a\r\nX-Big: ");
        big.push_str(&"a".repeat(17000));
        let mut m = Msg::default();
        let mut scan = 0;
        assert_eq!(parse_request(&mut m, big.as_bytes(), 16384, &mut scan), Parse::TooLarge);
        big.push_str("\r\n\r\n");
        scan = 0;
        assert_eq!(parse_request(&mut m, big.as_bytes(), 16384, &mut scan), Parse::TooLarge);
        let mut many = String::from("GET / HTTP/1.1\r\nHost: a\r\n");
        for i in 0..MAX_HEADERS {
            many.push_str(&format!("X-{i}: v\r\n"));
        }
        many.push_str("\r\n");
        assert_eq!(req(&many, &mut m), Parse::TooLarge);
    }

    #[test]
    fn malformed() {
        let mut m = Msg::default();
        for bad in [
            "GARBAGE\r\n\r\n",
            "GET / HTTP/2.0\r\nHost: a\r\n\r\n",
            "GET  / HTTP/1.1\r\nHost: a\r\n\r\n",
            "GET / HTTP/1.1\r\nHost : a\r\n\r\n",
            "GET / HTTP/1.1\r\nHost: a\r\n folded\r\n\r\n",
            "GET / HTTP/1.1\r\nNoColon\r\n\r\n",
            "GET / HTTP/1.1\r\nHost: a\r\nX: a\nb\r\n\r\n",
            "GET / HTTP/1.1\r\nAccept: x\r\n\r\n",
            "GET / HTTP/1.1\r\nHost: a\r\nHost: b\r\n\r\n",
            "GET / HTTP/1.1\r\nHost: a b\r\n\r\n",
        ] {
            assert_eq!(req(bad, &mut m), Parse::Error, "{bad:?}");
        }
        assert_eq!(req("GET / HTTP/1.0\r\n\r\n", &mut m), Parse::Complete);
        assert!(!m.has_host);
    }

    #[test]
    fn smuggling() {
        let mut m = Msg::default();
        let p = "POST / HTTP/1.1\r\nHost: a\r\n";
        assert_eq!(req(&format!("{p}Content-Length: 4\r\nTransfer-Encoding: chunked\r\n\r\n"), &mut m), Parse::Error);
        assert_eq!(req(&format!("{p}Content-Length: 4\r\nContent-Length: 5\r\n\r\n"), &mut m), Parse::Error);
        assert_eq!(req(&format!("{p}Content-Length: 4\r\nContent-Length: 4\r\n\r\n"), &mut m), Parse::Complete);
        assert_eq!(req(&format!("{p}Content-Length: -1\r\n\r\n"), &mut m), Parse::Error);
        assert_eq!(req(&format!("{p}Transfer-Encoding: gzip\r\n\r\n"), &mut m), Parse::Error);
        assert_eq!(req(&format!("{p}Transfer-Encoding: gzip, chunked\r\n\r\n"), &mut m), Parse::Complete);
        assert!(m.chunked);
    }

    #[test]
    fn connection_tokens() {
        let mut m = Msg::default();
        let r = "GET / HTTP/1.1\r\nHost: a\r\nConnection: keep-alive, X-Foo, Upgrade\r\n\
                 Upgrade: websocket\r\nX-Foo: 1\r\nX-Bar: 2\r\n\r\n";
        assert_eq!(req(r, &mut m), Parse::Complete);
        let b = r.as_bytes();
        assert!(m.conn_keepalive && m.conn_upgrade && !m.conn_close);
        assert_eq!(m.upgrade.get(b), b"websocket");
        assert!(m.is_hop_by_hop(b, b"X-Foo"));
        assert!(m.is_hop_by_hop(b, b"keep-alive"));
        assert!(m.is_hop_by_hop(b, b"TE"));
        assert!(!m.is_hop_by_hop(b, b"X-Bar"));
        let r = "GET / HTTP/1.1\r\nHost: a\r\nConnection: Content-Length\r\n\r\n";
        assert_eq!(req(r, &mut m), Parse::Complete);
        assert!(!m.is_hop_by_hop(r.as_bytes(), b"Content-Length"));
    }

    #[test]
    fn leading_crlf() {
        let mut m = Msg::default();
        let r = "\r\nGET / HTTP/1.1\r\nHost: a\r\n\r\n";
        assert_eq!(req(r, &mut m), Parse::Complete);
        assert_eq!(m.method.get(r.as_bytes()), b"GET");
    }

    #[test]
    fn response() {
        let mut m = Msg::default();
        let resp = |s: &str, m: &mut Msg| {
            let mut scan = 0;
            parse_response(m, s.as_bytes(), 16384, &mut scan)
        };
        let r = "HTTP/1.1 200 OK\r\nContent-Length: 12\r\nX-Backend-Load: 0.42\r\n\r\n";
        assert_eq!(resp(r, &mut m), Parse::Complete);
        assert_eq!(m.status, 200);
        assert_eq!(m.content_length, 12);
        assert_eq!(m.backend_load.unwrap().get(r.as_bytes()), b"0.42");
        let r = "HTTP/1.1 200 OK\r\nContent-Length: 12\r\nTransfer-Encoding: chunked\r\n\r\n";
        assert_eq!(resp(r, &mut m), Parse::Complete);
        assert!(m.chunked);
        assert_eq!(m.content_length, -1);
        assert_eq!(resp("HTTP/1.1 204\r\n\r\n", &mut m), Parse::Complete);
        assert_eq!(m.status, 204);
        assert_eq!(resp("HTTP/1.1 20 OK\r\n\r\n", &mut m), Parse::Error);
    }

    #[test]
    fn body_length() {
        let mut b = Body::new(BodyMode::Length, 10);
        assert_eq!(b.feed(b"12345"), Some(5));
        assert!(!b.done);
        assert_eq!(b.feed(b"67890GET /"), Some(5));
        assert!(b.done);
        assert!(Body::new(BodyMode::None, 0).done);
    }

    #[test]
    fn body_chunked() {
        let c = b"5\r\nhello\r\n1A;ext=v\r\nabcdefghijklmnopqrstuvwxyz\r\n0\r\nX-T: 1\r\n\r\nNEXT";
        let body = c.len() - 4;
        let mut b = Body::new(BodyMode::Chunked, 0);
        assert_eq!(b.feed(c), Some(body));
        assert!(b.done);
        let mut b = Body::new(BodyMode::Chunked, 0);
        let mut total = 0;
        for i in 0..c.len() {
            if b.done {
                break;
            }
            total += b.feed(&c[i..i + 1]).unwrap();
        }
        assert!(b.done);
        assert_eq!(total, body);
        let mut b = Body::new(BodyMode::Chunked, 0);
        assert_eq!(b.feed(b"0\r\n\r\n"), Some(5));
        assert!(b.done);
    }

    #[test]
    fn body_chunked_invalid() {
        assert_eq!(Body::new(BodyMode::Chunked, 0).feed(b"zz\r\n"), None);
        assert_eq!(Body::new(BodyMode::Chunked, 0).feed(b"3\r\nabcX"), None);
        assert_eq!(Body::new(BodyMode::Chunked, 0).feed(b"ffffffffffffffff\r\n"), None);
    }
}
