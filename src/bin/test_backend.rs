//! Backend HTTP/1.1 de pruebas sobre mio (epoll en Linux, kqueue en macOS).
//!
//!   test_backend --port 9001 --name api-1 [--latency-ms N]
//!                [--load F | --load-auto] [--threads N] [--bind IP] [--drop-every N]
//!
//! Endpoints:
//!   /health          200 "ok"
//!   /echo            devuelve el cuerpo tal cual (Content-Length o chunked)
//!   /headers         devuelve la cabecera de la petición recibida
//!   /chunked         respuesta chunked en varios trozos (con trailer)
//!   /close           respuesta delimitada por cierre de conexión
//!   /nocontent       204
//!   /sleep/<ms>      responde tras <ms> milisegundos
//!   /big/<n>         cuerpo de n bytes
//!   /stats           JSON con conexiones y peticiones atendidas
//!   /ws              WebSocket eco
//!   cualquier otra   "hello from <name>" (100 B)
//! Todas las respuestas llevan X-Backend y, con --load/--load-auto,
//! X-Backend-Load.
//!
//! --drop-every N: en cada conexión, la petición N, 2N... (nunca la primera)
//! se descarta cerrando la conexión sin responder. Simula un backend que
//! cierra conexiones keep-alive justo cuando el proxy las reutiliza.

use std::io::{self, Read, Write};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering::Relaxed};
use std::time::Duration;

use base64::Engine;
use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};
use sha1::{Digest, Sha1};

use proxy::http::{self, Body, BodyMode, Msg, Parse};
use proxy::slab::{Slab, token, untoken};
use proxy::timer::{Pop, TimerState, Timers};
use proxy::util::{ieq, mono_ms};

struct Opt {
    port: u16,
    bind_ip: String,
    name: String,
    latency_ms: u64,
    load: Option<f64>,
    load_auto: bool,
    threads: usize,
    drop_every: u32,
}

static OPT: OnceLock<Opt> = OnceLock::new();
static N_CONNS: AtomicU64 = AtomicU64::new(0);
static N_REQUESTS: AtomicU64 = AtomicU64::new(0);
static N_ACTIVE: AtomicI64 = AtomicI64::new(0);

fn opt() -> &'static Opt {
    OPT.get().unwrap()
}

#[derive(PartialEq, Clone, Copy)]
enum St {
    Head,
    Body,
    Wait,
    Ws,
    Closing,
}

struct Conn {
    sock: TcpStream,
    st: St,
    rd: bool,
    wr: bool,
    eof: bool,
    closed: bool,
    keepalive: bool,
    head_req: bool,
    inb: Vec<u8>,
    in_off: usize,
    out: Vec<u8>,
    out_off: usize,
    scan: usize,
    body: Body,
    body_start: usize,
    path: String,
    chunked_req: bool,
    head_copy: Vec<u8>,
    ws_key: String,
    is_ws: bool,
    delay: TimerState,
    pending: Vec<u8>,
    nreq: u32,
    slot_gen: u32,
}

fn current_load() -> f64 {
    match opt().load {
        Some(l) => l,
        None => (N_ACTIVE.load(Relaxed) as f64 / 50.0).min(1.0),
    }
}

/// Cabecera de respuesta común. body_len: Some(n) => Content-Length,
/// None + chunked => chunked, None => sin longitud.
fn resp_head(
    keepalive: bool,
    o: &mut Vec<u8>,
    status: u16,
    reason: &str,
    ctype: Option<&str>,
    len: Option<usize>,
    chunked: bool,
) {
    let op = opt();
    let _ = write!(o, "HTTP/1.1 {status} {reason}\r\nX-Backend: {}\r\n", op.name);
    if op.load.is_some() || op.load_auto {
        let _ = write!(o, "X-Backend-Load: {:.3}\r\n", current_load());
    }
    if let Some(c) = ctype {
        let _ = write!(o, "Content-Type: {c}\r\n");
    }
    if let Some(n) = len {
        let _ = write!(o, "Content-Length: {n}\r\n");
    } else if chunked {
        o.extend_from_slice(b"Transfer-Encoding: chunked\r\n");
    }
    if !keepalive {
        o.extend_from_slice(b"Connection: close\r\n");
    }
    o.extend_from_slice(b"\r\n");
}

fn simple(c: &Conn, o: &mut Vec<u8>, status: u16, reason: &str, body: &[u8]) {
    resp_head(c.keepalive, o, status, reason, Some("text/plain"), Some(body.len()), false);
    if !c.head_req {
        o.extend_from_slice(body);
    }
}

fn ws_accept_key(key: &str) -> String {
    let mut h = Sha1::new();
    h.update(key.as_bytes());
    h.update(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
    base64::engine::general_purpose::STANDARD.encode(h.finalize())
}

struct Thread {
    poll: Poll,
    conns: Slab<Conn>,
    timers: Timers,
    dead: Vec<usize>,
    now: u64,
    /// Buffer de lectura reutilizado (no se pone a cero en cada evento).
    rbuf: Box<[u8]>,
}

impl Thread {
    fn close(&mut self, i: usize) {
        let c = &mut self.conns[i];
        if c.closed {
            return;
        }
        c.closed = true;
        Timers::cancel(&mut c.delay);
        let _ = self.poll.registry().deregister(&mut c.sock);
        N_ACTIVE.fetch_sub(1, Relaxed);
        self.dead.push(i);
    }

    /// Genera la respuesta a la petición completa (cabecera + cuerpo en inb).
    fn respond(&mut self, i: usize) {
        N_REQUESTS.fetch_add(1, Relaxed);
        let op = opt();
        let now = self.now;
        let c = &mut self.conns[i];
        let mut delay = op.latency_ms;
        let mut o = Vec::new();
        let body = c.inb[c.body_start..c.in_off].to_vec(); // cuerpo crudo
        let path = c.path.clone();

        if c.is_ws {
            let _ = write!(
                c.out,
                "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
                 Sec-WebSocket-Accept: {}\r\nX-Backend: {}\r\n\r\n",
                ws_accept_key(&c.ws_key),
                op.name
            );
            c.st = St::Ws;
            return;
        }
        if path == "/health" {
            simple(c, &mut o, 200, "OK", b"ok\n");
            delay = 0;
        } else if path == "/echo" {
            if c.chunked_req {
                resp_head(c.keepalive, &mut o, 200, "OK", Some("application/octet-stream"), None, true);
                o.extend_from_slice(if c.head_req { b"0\r\n\r\n" } else { &body });
            } else {
                resp_head(c.keepalive, &mut o, 200, "OK", Some("application/octet-stream"), Some(body.len()), false);
                if !c.head_req {
                    o.extend_from_slice(&body);
                }
            }
        } else if path == "/headers" {
            resp_head(c.keepalive, &mut o, 200, "OK", Some("text/plain"), Some(c.head_copy.len()), false);
            if !c.head_req {
                o.extend_from_slice(&c.head_copy);
            }
        } else if path == "/chunked" {
            resp_head(c.keepalive, &mut o, 200, "OK", Some("text/plain"), None, true);
            if !c.head_req {
                let _ = write!(o, "6\r\nhello \r\n{:x};ext=1\r\nfrom {}\r\n", op.name.len() + 5, op.name);
                o.extend_from_slice(b"1\r\n\n\r\n0\r\nX-Trailer: yes\r\n\r\n");
            }
        } else if path == "/close" {
            c.keepalive = false;
            let _ = write!(
                o,
                "HTTP/1.1 200 OK\r\nX-Backend: {}\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\n",
                op.name
            );
            if !c.head_req {
                let _ = writeln!(o, "closed-delimited body from {}", op.name);
            }
        } else if path == "/nocontent" {
            resp_head(c.keepalive, &mut o, 204, "No Content", None, None, false);
        } else if let Some(ms) = path.strip_prefix("/sleep/") {
            delay = ms.parse().unwrap_or(0);
            simple(c, &mut o, 200, "OK", format!("slept {delay} ms in {}\n", op.name).as_bytes());
            // siempre por pending, aunque --latency-ms sea 0
            c.pending = o;
            if delay > 0 {
                c.st = St::Wait;
                let tok = token(1, i, c.slot_gen);
                self.timers.set(&mut c.delay, now + delay, tok);
            } else {
                let p = std::mem::take(&mut c.pending);
                c.out.extend_from_slice(&p);
                c.st = if c.keepalive { St::Head } else { St::Closing };
            }
            return;
        } else if let Some(n) = path.strip_prefix("/big/") {
            let n: usize = n.parse().unwrap_or(0);
            resp_head(c.keepalive, &mut o, 200, "OK", Some("application/octet-stream"), Some(n), false);
            if !c.head_req {
                o.resize(o.len() + n, b'x');
            }
        } else if path == "/stats" {
            let s = format!(
                "{{\"name\":\"{}\",\"connections\":{},\"requests\":{},\"active\":{}}}\n",
                op.name,
                N_CONNS.load(Relaxed),
                N_REQUESTS.load(Relaxed),
                N_ACTIVE.load(Relaxed)
            );
            simple(c, &mut o, 200, "OK", s.as_bytes());
            delay = 0;
        } else {
            // 100 B, el tamaño de respuesta del benchmark (SPEC §11)
            let p: String = path.chars().take(60).collect();
            let mut b = format!("hello from {} path={p}\n", op.name).into_bytes();
            if b.len() < 99 {
                b.resize(99, b'.');
                b.push(b'\n');
            }
            simple(c, &mut o, 200, "OK", &b);
        }
        if delay > 0 {
            c.pending = o;
            c.st = St::Wait;
            let tok = token(1, i, c.slot_gen);
            self.timers.set(&mut c.delay, now + delay, tok);
        } else {
            c.out.extend_from_slice(&o);
            c.st = if c.keepalive { St::Head } else { St::Closing };
        }
    }

    fn process_ws(&mut self, i: usize) -> bool {
        let c = &mut self.conns[i];
        let p = &mut c.inb[c.in_off..];
        let n = p.len();
        if n < 2 {
            return false;
        }
        let opcode = p[0] & 0x0f;
        let masked = p[1] & 0x80 != 0;
        let mut len = (p[1] & 0x7f) as u64;
        let mut hl = 2;
        if len == 126 {
            if n < 4 {
                return false;
            }
            len = u16::from_be_bytes([p[2], p[3]]) as u64;
            hl = 4;
        } else if len == 127 {
            if n < 10 {
                return false;
            }
            len = u64::from_be_bytes(p[2..10].try_into().unwrap());
            hl = 10;
        }
        let mk = hl;
        if masked {
            hl += 4;
        }
        let len = len as usize;
        if n < hl + len {
            return false;
        }
        if masked {
            let mask = [p[mk], p[mk + 1], p[mk + 2], p[mk + 3]];
            for k in 0..len {
                p[hl + k] ^= mask[k & 3];
            }
        }
        let reply_op = if opcode == 9 { 10 } else { opcode };
        let mut h = vec![0x80 | reply_op];
        if len < 126 {
            h.push(len as u8);
        } else if len < 65536 {
            h.push(126);
            h.extend_from_slice(&(len as u16).to_be_bytes());
        } else {
            h.push(127);
            h.extend_from_slice(&(len as u64).to_be_bytes());
        }
        if opcode != 10 {
            // no se contesta a pong
            let payload = p[hl..hl + len].to_vec();
            c.out.extend_from_slice(&h);
            c.out.extend_from_slice(&payload);
        }
        c.in_off += hl + len;
        if opcode == 8 {
            c.st = St::Closing;
        }
        true
    }

    fn process(&mut self, i: usize) -> bool {
        let st = self.conns[i].st;
        if st == St::Ws {
            return self.process_ws(i);
        }
        let op = opt();
        if st == St::Head {
            let c = &mut self.conns[i];
            if c.in_off == c.inb.len() {
                c.in_off = 0;
                c.inb.clear();
            }
            if c.inb.len() == c.in_off {
                return false;
            }
            let mut m = Msg::default();
            let buf = &c.inb[c.in_off..];
            match http::parse_request(&mut m, buf, 65536, &mut c.scan) {
                Parse::Incomplete => return false,
                Parse::Complete => {}
                _ => {
                    c.keepalive = false;
                    let mut o = Vec::new();
                    simple(c, &mut o, 400, "Bad Request", b"bad request\n");
                    c.out.extend_from_slice(&o);
                    c.st = St::Closing;
                    return true;
                }
            }
            c.nreq += 1;
            if op.drop_every > 0 && c.nreq > 1 && c.nreq.is_multiple_of(op.drop_every) {
                self.close(i);
                return false;
            }
            c.keepalive = if m.version_minor == 1 { !m.conn_close } else { m.conn_keepalive };
            c.head_req = ieq(m.method.get(buf), "HEAD");
            c.path = String::from_utf8_lossy(m.target.get(buf)).chars().take(255).collect();
            c.head_copy = buf[..m.head_len].to_vec();
            c.is_ws = false;
            if let Some(k) = m.header_value(buf, "sec-websocket-key")
                && c.path == "/ws"
                && m.conn_upgrade
                && k.len() < 64
            {
                c.ws_key = String::from_utf8_lossy(k).into_owned();
                c.is_ws = true;
            }
            c.chunked_req = m.chunked;
            let mode = if m.chunked {
                BodyMode::Chunked
            } else if m.content_length > 0 {
                BodyMode::Length
            } else {
                BodyMode::None
            };
            c.body = Body::new(mode, m.content_length.max(0) as u64);
            c.in_off += m.head_len;
            c.body_start = c.in_off;
            c.scan = 0;
            c.st = St::Body;
        }
        if self.conns[i].st == St::Body {
            let c = &mut self.conns[i];
            let Some(k) = c.body.feed(&c.inb[c.in_off..]) else {
                c.keepalive = false;
                let mut o = Vec::new();
                simple(c, &mut o, 400, "Bad Request", b"bad chunked\n");
                c.out.extend_from_slice(&o);
                c.st = St::Closing;
                return true;
            };
            c.in_off += k;
            if !c.body.done {
                return false;
            }
            self.respond(i);
            let c = &mut self.conns[i];
            // Compactar: el cuerpo ya no hace falta.
            c.inb.drain(..c.in_off);
            c.in_off = 0;
            return true;
        }
        false
    }

    fn drive(&mut self, i: usize) {
        loop {
            let mut p = false;
            {
                let (c, buf) = (&mut self.conns[i], &mut self.rbuf);
                while c.rd && !c.eof {
                    match c.sock.read(buf) {
                        Ok(0) => {
                            c.eof = true;
                            p = true;
                        }
                        Ok(n) => {
                            c.inb.extend_from_slice(&buf[..n]);
                            p = true;
                        }
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => c.rd = false,
                        Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                        Err(_) => {
                            self.close(i);
                            return;
                        }
                    }
                }
            }
            while matches!(self.conns[i].st, St::Head | St::Body | St::Ws) && self.process(i) {
                p = true;
            }
            if self.conns[i].closed {
                return;
            }
            let c = &mut self.conns[i];
            while c.wr && c.out.len() > c.out_off {
                match c.sock.write(&c.out[c.out_off..]) {
                    Ok(n) if n > 0 => {
                        c.out_off += n;
                        p = true;
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => c.wr = false,
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                    _ => {
                        self.close(i);
                        return;
                    }
                }
            }
            if c.out_off == c.out.len() {
                c.out.clear();
                c.out_off = 0;
            }
            if c.out.is_empty() && (c.st == St::Closing || (c.eof && c.st != St::Wait)) {
                self.close(i);
                return;
            }
            if !p {
                return;
            }
        }
    }

    fn accept(&mut self, l: &TcpListener) {
        loop {
            let Ok((sock, _)) = l.accept() else { return };
            let _ = sock.set_nodelay(true);
            N_CONNS.fetch_add(1, Relaxed);
            N_ACTIVE.fetch_add(1, Relaxed);
            let (i, g) = self.conns.insert(Conn {
                sock,
                st: St::Head,
                rd: true,
                wr: true,
                eof: false,
                closed: false,
                keepalive: true,
                head_req: false,
                inb: Vec::new(),
                in_off: 0,
                out: Vec::new(),
                out_off: 0,
                scan: 0,
                body: Body::default(),
                body_start: 0,
                path: String::new(),
                chunked_req: false,
                head_copy: Vec::new(),
                ws_key: String::new(),
                is_ws: false,
                delay: TimerState::default(),
                pending: Vec::new(),
                nreq: 0,
                slot_gen: 0,
            });
            self.conns[i].slot_gen = g;
            let tok = Token(token(1, i, g) as usize);
            if self
                .poll
                .registry()
                .register(&mut self.conns[i].sock, tok, Interest::READABLE | Interest::WRITABLE)
                .is_err()
            {
                self.conns.remove(i);
                N_ACTIVE.fetch_sub(1, Relaxed);
                continue;
            }
            self.drive(i);
        }
    }
}

fn open_listen(reuseport: bool) -> io::Result<std::net::TcpListener> {
    let op = opt();
    let addr = proxy::util::addr_parse(&format!("{}:{}", op.bind_ip, op.port), false).map_err(io::Error::other)?;
    proxy::worker::listener_open(&addr, reuseport)
}

fn thread_main(l: std::net::TcpListener) {
    let mut t = Thread {
        poll: Poll::new().expect("poll"),
        conns: Slab::default(),
        timers: Timers::new(),
        dead: Vec::new(),
        now: mono_ms(),
        rbuf: vec![0u8; 65536].into_boxed_slice(),
    };
    let mut l = TcpListener::from_std(l);
    t.poll.registry().register(&mut l, Token(0), Interest::READABLE).expect("registro");
    let mut events = Events::with_capacity(1024);
    loop {
        t.now = mono_ms();
        let to = t.timers.next_in(t.now).map(|ms| Duration::from_millis(ms.min(60_000)));
        if t.poll.poll(&mut events, to).is_err() {
            continue;
        }
        t.now = mono_ms();
        for ev in events.iter() {
            if ev.token() == Token(0) {
                t.accept(&l);
                continue;
            }
            let (_, i, g) = untoken(ev.token().0 as u64);
            let Some(c) = t.conns.get_gen_mut(i, g) else { continue };
            if c.closed {
                continue;
            }
            c.rd |= ev.is_readable() || ev.is_read_closed() || ev.is_error();
            c.wr |= ev.is_writable() || ev.is_error();
            t.drive(i);
        }
        while let Pop::Due(tok, key) = t.timers.pop_due(t.now) {
            let (_, i, g) = untoken(tok);
            let now = t.now;
            let Some(c) = t.conns.get_gen_mut(i, g) else { continue };
            if c.closed || !t.timers.check_due(&mut c.delay, key, tok, now) {
                continue;
            }
            let p = std::mem::take(&mut c.pending);
            c.out.extend_from_slice(&p);
            c.st = if c.keepalive { St::Head } else { St::Closing };
            c.wr = true;
            t.drive(i);
        }
        for i in std::mem::take(&mut t.dead) {
            t.conns.remove(i);
        }
    }
}

fn usage(prog: &str) -> ! {
    eprintln!(
        "uso: {prog} --port N --name NOMBRE [--latency-ms N] [--load F|--load-auto] \
         [--threads N] [--bind IP] [--drop-every N]"
    );
    std::process::exit(2);
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut o = Opt {
        port: 9001,
        bind_ip: "127.0.0.1".into(),
        name: "backend".into(),
        latency_ms: 0,
        load: None,
        load_auto: false,
        threads: 1,
        drop_every: 0,
    };
    let mut it = args.iter().skip(1);
    while let Some(a) = it.next() {
        let mut val = || it.next().cloned().unwrap_or_else(|| usage(&args[0]));
        match a.as_str() {
            "--port" | "-p" => o.port = val().parse().unwrap_or_else(|_| usage(&args[0])),
            "--name" | "-n" => o.name = val(),
            "--latency-ms" | "-l" => o.latency_ms = val().parse().unwrap_or(0),
            "--load" | "-L" => o.load = val().parse().ok(),
            "--load-auto" | "-A" => o.load_auto = true,
            "--threads" | "-t" => o.threads = val().parse::<usize>().unwrap_or(1).clamp(1, 64),
            "--bind" | "-b" => o.bind_ip = val(),
            "--drop-every" | "-D" => o.drop_every = val().parse().unwrap_or(0),
            "--help" | "-h" => {
                eprintln!("ver la cabecera de test_backend.rs");
                return;
            }
            _ => usage(&args[0]),
        }
    }
    let nt = o.threads;
    OPT.set(o).ok();
    // Linux reparte entre sockets SO_REUSEPORT; en otros sistemas todos los
    // hilos comparten uno.
    let per_thread = cfg!(target_os = "linux") && nt > 1;
    let first = match open_listen(per_thread) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("test_backend {}: bind/listen {}:{}: {e}", opt().name, opt().bind_ip, opt().port);
            std::process::exit(1);
        }
    };
    eprintln!("test_backend {} escuchando en {}:{} ({nt} hilos)", opt().name, opt().bind_ip, opt().port);
    for _ in 1..nt {
        let l = if per_thread { open_listen(true) } else { first.try_clone() };
        match l {
            Ok(l) => {
                std::thread::spawn(move || thread_main(l));
            }
            Err(e) => {
                eprintln!("test_backend: {e}");
                std::process::exit(1);
            }
        }
    }
    thread_main(first);
}
