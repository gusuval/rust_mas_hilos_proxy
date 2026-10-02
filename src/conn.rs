//! Máquina de estados cliente <-> upstream.
//!
//! Cada sesión (conexión de cliente) tiene cuatro buffers de 16 KB:
//!   inb : bytes crudos del cliente (cabecera, cuerpo, peticiones pipelined)
//!   uo  : hacia el upstream (cabecera reescrita + cuerpo)
//!   ui  : bytes crudos del upstream
//!   out : hacia el cliente (cabecera reescrita + cuerpo)
//!
//! Todos los sockets son edge-triggered: `rd`/`wr` indican si el último
//! intento no terminó en WouldBlock. `sess_drive` repite leer/procesar/
//! escribir mientras haya progreso, así nunca queda trabajo pendiente sin un
//! evento que lo despierte. Si una sesión agota su presupuesto se vuelve a
//! planificar al final de la iteración para no acaparar el loop.

use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr};
use std::rc::Rc;
use std::sync::atomic::Ordering::Relaxed;

use mio::net::TcpStream;
use mio::{Interest, Token};
use rustls::ServerConnection;

use crate::balancer::{Backend, Server, parse_load};
use crate::buffer::{BUF_SIZE, Buf, BufPool, buf_move};
use crate::config::Timeouts;
use crate::http::{self, Body, BodyMode, Msg, Parse};
use crate::runtime::Runtime;
use crate::slab::token;
use crate::stats::{ST_NCLASS, inc};
use crate::timer::{TimerState, Timers};
use crate::tls::{TLS_BUFFER_LIMIT, pattern_match};
use crate::util::ieq;
use crate::worker::{K_SESS, K_UP, Worker};
use crate::{log_access, logd, loge, logw};

const HEAD_MAX: usize = BUF_SIZE;
const DRIVE_BUDGET: usize = 32;
const MAX_ATTEMPTS: u32 = 3;
const CLOSING_MS: u64 = 5000;
const LINGER_MS: u64 = 2000;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SState {
    Handshake,
    Head,
    Proxy,
    Tunnel,
    /// Vaciando `out` antes de cerrar.
    Closing,
    /// Escritura cerrada; descartando lo que llegue hasta EOF.
    Linger,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Uf {
    Connect,
    ConnectTimeout,
    Io,
}

pub struct Upstream {
    sock: TcpStream,
    be: Rc<Backend>,
    srv: usize,
    /// None si está ocioso en el pool.
    sess: Option<usize>,
    pub timer: TimerState,
    connecting: bool,
    rd: bool,
    wr: bool,
    eof: bool,
    pub closed: bool,
    reused: bool,
    in_pool: bool,
    slot_gen: u32,
}

impl Upstream {
    fn server(&self) -> &Server {
        &self.be.servers[self.srv]
    }
}

pub struct Session {
    sock: TcpStream,
    tls: Option<Box<ServerConnection>>,
    st: SState,
    rd: bool,
    wr: bool,
    eof: bool,
    pub closed: bool,
    pub redrive: bool,

    cli_keepalive: bool,
    client_v10: bool,
    head_req: bool,
    want_upgrade: bool,
    retry_ok: bool,
    req_done: bool,
    resp_head_done: bool,
    resp_done: bool,
    up_reusable: bool,
    resp_started: bool,
    head_timer: bool,
    status: u16,

    fe_key: Rc<str>,
    ip: String,
    sni: String,
    cert_pat: String,

    inb: Buf,
    out: Buf,
    uo: Buf,
    ui: Buf,
    scan_req: usize,
    scan_resp: usize,
    reqb: Body,
    respb: Body,

    up: Option<usize>,
    rt: Option<Rc<Runtime>>,
    be: Option<Rc<Backend>>,
    tried: u64,
    attempts: u32,
    force_new: bool,

    t_accept: u64,
    t_state: u64,
    t_head: u64,
    t_req: u64,
    t_conn: u64,
    t_progress: u64,
    log_method: String,
    log_host: String,
    log_path: String,
    log_server: String,

    pub timer: TimerState,
    slot_gen: u32,
}

impl Session {
    pub fn release_bufs(&mut self, bp: &mut BufPool) {
        self.inb.release(bp);
        self.out.release(bp);
        self.uo.release(bp);
        self.ui.release(bp);
    }
}

/// Escritor sobre un slice con detección de desbordamiento.
struct Wb<'a> {
    p: &'a mut [u8],
    len: usize,
    overflow: bool,
}

impl<'a> Wb<'a> {
    fn new(p: &'a mut [u8]) -> Wb<'a> {
        Wb { p, len: 0, overflow: false }
    }
    fn put(&mut self, s: &[u8]) {
        if self.overflow || self.len + s.len() > self.p.len() {
            self.overflow = true;
            return;
        }
        self.p[self.len..self.len + s.len()].copy_from_slice(s);
        self.len += s.len();
    }
}

fn is_idempotent(m: &[u8]) -> bool {
    ["GET", "HEAD", "PUT", "DELETE", "OPTIONS", "TRACE"].iter().any(|x| ieq(m, x))
}

fn lossy(b: &[u8], max: usize) -> String {
    let b = &b[..b.len().min(max)];
    String::from_utf8_lossy(b).into_owned()
}

impl Worker {
    fn to(&self) -> Timeouts {
        self.rt.as_ref().expect("runtime").c.cfg.timeouts
    }

    fn sess_token(&self, sid: usize) -> u64 {
        token(K_SESS, sid, self.sessions[sid].slot_gen)
    }

    // ---------------------------------------------------------------
    // upstreams y pool keep-alive
    // ---------------------------------------------------------------

    fn pool_unlink(&mut self, uid: usize) {
        let u = &mut self.ups[uid];
        u.be.servers[u.srv].idle.borrow_mut().retain(|&x| x != uid);
        u.in_pool = false;
    }

    pub fn up_close(&mut self, uid: usize) {
        if self.ups[uid].closed {
            return;
        }
        if self.ups[uid].in_pool {
            self.pool_unlink(uid);
        }
        let u = &mut self.ups[uid];
        u.closed = true;
        u.sess = None;
        Timers::cancel(&mut u.timer);
        let _ = self.poll.registry().deregister(&mut u.sock);
        let _ = u.sock.shutdown(Shutdown::Both);
        self.dead_ups.push(uid);
    }

    fn pool_put(&mut self, uid: usize) {
        let idle_to = self.to().upstream_idle;
        let now = self.now;
        let u = &mut self.ups[uid];
        if u.server().idle.borrow().len() >= u.be.max_idle {
            self.up_close(uid);
            return;
        }
        u.sess = None;
        u.reused = true;
        u.in_pool = true;
        u.be.servers[u.srv].idle.borrow_mut().push(uid);
        let tok = token(K_UP, uid, u.slot_gen);
        self.timers.set(&mut u.timer, now + idle_to, tok);
    }

    fn pool_get(&mut self, srv: &Server) -> Option<usize> {
        let uid = srv.idle.borrow_mut().pop()?;
        let u = &mut self.ups[uid];
        u.in_pool = false;
        Timers::cancel(&mut u.timer);
        Some(uid)
    }

    pub fn upstream_pool_drain(&mut self, srv: &Server) {
        loop {
            let last = srv.idle.borrow().last().copied();
            match last {
                Some(uid) => self.up_close(uid),
                None => return,
            }
        }
    }

    pub fn up_event(&mut self, uid: usize, g: u32, rd: bool, wr: bool, hup: bool) {
        let Some(u) = self.ups.get_gen_mut(uid, g) else { return };
        if u.closed {
            return;
        }
        u.rd |= rd;
        u.wr |= wr;
        match u.sess {
            Some(sid) => self.sess_drive(sid),
            None => {
                // Ocioso en el pool: cualquier dato o cierre invalida la
                // conexión. El evento puede ser tardío, así que se mira.
                if hup {
                    self.up_close(uid);
                } else if rd {
                    let mut c = [0u8; 1];
                    match u.sock.peek(&mut c) {
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => u.rd = false,
                        _ => self.up_close(uid),
                    }
                }
            }
        }
    }

    fn up_attach(&mut self, sid: usize, uid: usize) {
        let u = &mut self.ups[uid];
        u.sess = Some(sid);
        let srv = u.server();
        srv.active.set(srv.active.get() + 1);
        srv.requests.set(srv.requests.get() + 1);
        let s = &mut self.sessions[sid];
        s.up = Some(uid);
        s.log_server.clear();
        s.log_server.push_str(&srv.addr_str);
        s.up_reusable = true;
        s.resp_started = false;
        s.ui.r = 0;
        s.ui.w = 0;
        s.scan_resp = 0;
    }

    /// Suelta el upstream actual sin devolverlo al pool.
    fn up_detach_close(&mut self, sid: usize) {
        let Some(uid) = self.sessions[sid].up.take() else { return };
        let srv = self.ups[uid].server();
        srv.active.set(srv.active.get() - 1);
        self.ups[uid].sess = None;
        self.up_close(uid);
    }

    fn connect_upstream(&mut self, sid: usize) -> bool {
        let now = self.now;
        loop {
            let s = &mut self.sessions[sid];
            let be = s.be.clone().expect("backend");
            let Some(si) = be.pick(s.tried, now) else {
                if s.attempts == 0 {
                    logw!("backend '{}' sin servidores disponibles", be.name);
                }
                let code = if s.attempts > 0 { 502 } else { 503 };
                self.send_error(sid, code);
                return false;
            };
            if si < 64 {
                s.tried |= 1u64 << si;
            }
            s.attempts += 1;
            s.t_conn = now;
            let force_new = s.force_new;
            let srv = &be.servers[si];

            if !force_new && let Some(uid) = self.pool_get(srv) {
                inc(&self.ws().upstream_reuses);
                self.up_attach(sid, uid);
                return true;
            }
            match TcpStream::connect(srv.sa) {
                Err(e) => {
                    logw!("connect {} ({}): {e}", srv.addr_str, be.name);
                    if be.passive_fail(srv, now) {
                        logw!("backend '{}' servidor {} -> DOWN (fallos pasivos)", be.name, srv.addr_str);
                    }
                    if self.sessions[sid].attempts >= MAX_ATTEMPTS {
                        self.send_error(sid, 502);
                        return false;
                    }
                }
                Ok(sock) => {
                    let _ = sock.set_nodelay(true);
                    let (uid, g) = self.ups.insert(Upstream {
                        sock,
                        be: be.clone(),
                        srv: si,
                        sess: None,
                        timer: TimerState::default(),
                        connecting: true,
                        rd: false,
                        wr: false,
                        eof: false,
                        closed: false,
                        reused: false,
                        in_pool: false,
                        slot_gen: 0,
                    });
                    let u = &mut self.ups[uid];
                    u.slot_gen = g;
                    let tok = Token(token(K_UP, uid, g) as usize);
                    if let Err(e) =
                        self.poll.registry().register(&mut u.sock, tok, Interest::READABLE | Interest::WRITABLE)
                    {
                        loge!("registro del upstream: {e}");
                        self.ups.remove(uid);
                        self.send_error(sid, 502);
                        return false;
                    }
                    inc(&self.ws().upstream_connects);
                    self.up_attach(sid, uid);
                    return true;
                }
            }
        }
    }

    fn upstream_failed(&mut self, sid: usize, why: Uf) {
        let Some(uid) = self.sessions[sid].up else { return };
        let u = &self.ups[uid];
        let (be, si, reused) = (u.be.clone(), u.srv, u.reused);
        let srv = &be.servers[si];
        let got_nothing = !self.sessions[sid].resp_started;
        let now = self.now;
        self.up_detach_close(sid);

        if why != Uf::Io {
            logw!(
                "upstream {} ({}): {}",
                srv.addr_str,
                be.name,
                if why == Uf::Connect { "conexión rechazada" } else { "timeout de conexión" }
            );
            if be.passive_fail(srv, now) {
                logw!("backend '{}' servidor {} -> DOWN (fallos pasivos)", be.name, srv.addr_str);
            }
            // Nada se ha enviado todavía: se puede probar otro servidor.
            if self.sessions[sid].attempts < MAX_ATTEMPTS {
                self.sessions[sid].uo.r = 0;
                self.connect_upstream(sid);
                return;
            }
            self.send_error(sid, if why == Uf::Connect { 502 } else { 504 });
            return;
        }
        // Conexión reutilizada que el backend cerró mientras estaba ociosa:
        // reintento único en conexión nueva si es idempotente y sin cuerpo.
        let s = &mut self.sessions[sid];
        if reused && got_nothing && s.retry_ok {
            inc(&self.st.w[self.id].upstream_retries);
            s.retry_ok = false;
            s.force_new = true;
            s.uo.r = 0;
            s.tried = 0;
            s.attempts = 0;
            self.connect_upstream(sid);
            return;
        }
        if !reused && got_nothing && be.passive_fail(srv, now) {
            logw!("backend '{}' servidor {} -> DOWN (fallos pasivos)", be.name, srv.addr_str);
        }
        if self.sessions[sid].resp_head_done {
            self.sess_close(sid); // respuesta a medias: solo queda cortar
            return;
        }
        self.send_error(sid, 502);
    }

    /// E/S con el upstream: connect, escritura de uo y lectura a ui.
    fn up_io(&mut self, sid: usize) -> bool {
        let Some(uid) = self.sessions[sid].up else { return false };
        let mut p = false;
        let (s, u) = (&mut self.sessions[sid], &mut self.ups[uid]);
        if u.connecting {
            if !u.wr && !u.rd {
                return false;
            }
            match u.sock.take_error() {
                Ok(None) => {}
                _ => {
                    self.upstream_failed(sid, Uf::Connect);
                    return true;
                }
            }
            match u.sock.peer_addr() {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::NotConnected => {
                    // todavía conectando: esperar al siguiente evento
                    u.wr = false;
                    u.rd = false;
                    return false;
                }
                Err(_) => {
                    self.upstream_failed(sid, Uf::Connect);
                    return true;
                }
            }
            u.connecting = false;
            u.wr = true;
            p = true;
        }
        while u.wr && !s.uo.is_empty() {
            match u.sock.write(s.uo.rslice()) {
                Ok(n) if n > 0 => {
                    // Con reintento posible se conserva la cabecera enviada.
                    if s.retry_ok {
                        s.uo.r += n;
                    } else {
                        s.uo.consume(n);
                    }
                    p = true;
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => u.wr = false,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                _ => {
                    self.upstream_failed(sid, Uf::Io);
                    return true;
                }
            }
        }
        while u.rd && !u.eof {
            if s.ui.space() == 0 {
                s.ui.compact();
                if s.ui.space() == 0 {
                    break;
                }
            }
            match u.sock.read(s.ui.wslice()) {
                Ok(0) => {
                    u.eof = true;
                    p = true;
                }
                Ok(n) => {
                    s.ui.w += n;
                    s.resp_started = true;
                    p = true;
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => u.rd = false,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(_) => {
                    // ECONNRESET y similares
                    if !s.resp_started {
                        self.upstream_failed(sid, Uf::Io);
                        return true;
                    }
                    u.eof = true;
                    s.up_reusable = false;
                    p = true;
                }
            }
        }
        p
    }

    // ---------------------------------------------------------------
    // cliente
    // ---------------------------------------------------------------

    fn account(&mut self, sid: usize, status: u16) {
        let ws = &self.st.w[self.id];
        inc(&ws.requests);
        let c = (status / 100) as usize;
        if (1..=ST_NCLASS).contains(&c) {
            inc(&ws.resp[c - 1]);
        }
        if self.rt.as_ref().is_some_and(|rt| rt.c.cfg.access_log) {
            let s = &self.sessions[sid];
            let ms = if s.t_req > 0 { self.now.saturating_sub(s.t_req) } else { 0 };
            let d = |x: &str| if x.is_empty() { "-".to_string() } else { x.to_string() };
            log_access!(
                "{} {} \"{} {}\" {} {} {}ms",
                s.ip,
                d(&s.log_host),
                d(&s.log_method),
                d(&s.log_path),
                status,
                d(&s.log_server),
                ms
            );
        }
    }

    fn release_up_bufs(&mut self, sid: usize) {
        let s = &mut self.sessions[sid];
        s.uo.release(&mut self.bp);
        s.ui.release(&mut self.bp);
    }

    fn enter_head(&mut self, sid: usize) {
        let now = self.now;
        self.release_up_bufs(sid);
        let s = &mut self.sessions[sid];
        s.st = SState::Head;
        s.t_state = now;
        s.scan_req = 0;
        s.head_timer = false;
        s.t_req = 0;
        s.log_method.clear();
        s.log_host.clear();
        s.log_path.clear();
        s.log_server.clear();
        if s.inb.has() && s.inb.is_empty() {
            s.inb.release(&mut self.bp);
        }
        if s.out.has() && s.out.is_empty() {
            s.out.release(&mut self.bp);
        }
    }

    fn enter_closing(&mut self, sid: usize) {
        let now = self.now;
        self.release_up_bufs(sid);
        let s = &mut self.sessions[sid];
        s.st = SState::Closing;
        s.t_state = now;
        s.cli_keepalive = false;
        s.inb.r = 0;
        s.inb.w = 0;
        s.rt = None;
        s.be = None;
    }

    fn send_error(&mut self, sid: usize, code: u16) {
        self.up_detach_close(sid);
        let s = &mut self.sessions[sid];
        if s.resp_head_done {
            self.sess_close(sid);
            return;
        }
        s.status = code;
        let reason = http::reason(code);
        let msg = format!(
            "HTTP/1.1 {code} {reason}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{reason}\n",
            reason.len() + 1
        );
        s.out.ensure(&mut self.bp);
        s.out.compact();
        if s.out.space() < msg.len() {
            self.sess_close(sid);
            return;
        }
        s.out.put(msg.as_bytes());
        self.account(sid, code);
        self.enter_closing(sid);
    }

    pub fn sess_close(&mut self, sid: usize) {
        if self.sessions[sid].closed {
            return;
        }
        self.sessions[sid].closed = true;
        Timers::cancel(&mut self.sessions[sid].timer);
        self.up_detach_close(sid);
        let s = &mut self.sessions[sid];
        s.rt = None;
        s.be = None;
        s.tls = None;
        let _ = self.poll.registry().deregister(&mut s.sock);
        s.release_bufs(&mut self.bp);
        self.nsessions -= 1;
        self.st.w[self.id].active_conns.fetch_sub(1, Relaxed);
        self.dead_sessions.push(sid);
    }

    fn cli_read(&mut self, sid: usize) -> bool {
        let s = &mut self.sessions[sid];
        if !s.rd || s.eof || s.st == SState::Closing {
            return false;
        }
        s.inb.ensure(&mut self.bp);
        let mut p = false;
        loop {
            if s.inb.space() == 0 {
                s.inb.compact();
                if s.inb.space() == 0 {
                    break;
                }
            }
            if let Some(tls) = s.tls.as_mut() {
                match tls.reader().read(s.inb.wslice()) {
                    Ok(0) => {
                        // close_notify
                        s.eof = true;
                        p = true;
                        break;
                    }
                    Ok(n) => {
                        s.inb.w += n;
                        p = true;
                        continue;
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                    Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                        s.eof = true;
                        p = true;
                        break;
                    }
                    Err(_) => {
                        self.sess_close(sid);
                        return true;
                    }
                }
                // Sin texto plano pendiente: leer registros del socket.
                match tls.read_tls(&mut s.sock) {
                    Ok(0) => {
                        s.eof = true;
                        p = true;
                        break;
                    }
                    Ok(_) => {
                        if tls.process_new_packets().is_err() {
                            let _ = tls.write_tls(&mut s.sock); // alerta
                            self.sess_close(sid);
                            return true;
                        }
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        s.rd = false;
                        break;
                    }
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                    Err(_) => {
                        self.sess_close(sid);
                        return true;
                    }
                }
            } else {
                match s.sock.read(s.inb.wslice()) {
                    Ok(0) => {
                        s.eof = true;
                        p = true;
                        break;
                    }
                    Ok(n) => {
                        s.inb.w += n;
                        p = true;
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        s.rd = false;
                        break;
                    }
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                    Err(_) => {
                        self.sess_close(sid);
                        return true;
                    }
                }
            }
        }
        p
    }

    fn cli_write(&mut self, sid: usize) -> bool {
        let s = &mut self.sessions[sid];
        let mut p = false;
        if let Some(tls) = s.tls.as_mut() {
            loop {
                // primero, el texto cifrado pendiente
                while tls.wants_write() {
                    if !s.wr {
                        return p;
                    }
                    match tls.write_tls(&mut s.sock) {
                        Ok(_) => p = true,
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                            s.wr = false;
                            return p;
                        }
                        Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                        Err(_) => {
                            self.sess_close(sid);
                            return true;
                        }
                    }
                }
                if !s.wr || s.out.is_empty() {
                    break;
                }
                match tls.writer().write(s.out.rslice()) {
                    Ok(n) if n > 0 => {
                        s.out.consume(n);
                        p = true;
                    }
                    _ => break,
                }
            }
        } else {
            while s.wr && !s.out.is_empty() {
                match s.sock.write(s.out.rslice()) {
                    Ok(n) if n > 0 => {
                        s.out.consume(n);
                        p = true;
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => s.wr = false,
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                    _ => {
                        self.sess_close(sid);
                        return true;
                    }
                }
            }
        }
        let s = &mut self.sessions[sid];
        if s.out.has() && s.out.is_empty() && s.st == SState::Head && s.inb.is_empty() {
            s.out.release(&mut self.bp);
        }
        p
    }

    fn do_handshake(&mut self, sid: usize) -> bool {
        let s = &mut self.sessions[sid];
        let tls = s.tls.as_mut().expect("tls");
        let mut p = false;
        loop {
            while tls.wants_write() && s.wr {
                match tls.write_tls(&mut s.sock) {
                    Ok(_) => p = true,
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => s.wr = false,
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                    Err(_) => {
                        self.sess_close(sid);
                        return true;
                    }
                }
            }
            if !tls.is_handshaking() {
                break;
            }
            if !s.rd {
                return p;
            }
            match tls.read_tls(&mut s.sock) {
                Ok(0) => {
                    logd!("handshake TLS fallido desde {}: EOF", s.ip);
                    self.sess_close(sid);
                    return true;
                }
                Ok(_) => {
                    p = true;
                    if let Err(e) = tls.process_new_packets() {
                        let _ = tls.write_tls(&mut s.sock); // alerta
                        logd!("handshake TLS fallido desde {}: {e}", s.ip);
                        self.sess_close(sid);
                        return true;
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => s.rd = false,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(_) => {
                    self.sess_close(sid);
                    return true;
                }
            }
        }
        // handshake completo
        let sni = tls.server_name().map(str::to_string);
        if let Some(rt) = &s.rt
            && let Some(tf) = rt.frontend(&s.fe_key).and_then(|f| f.tls.as_ref())
        {
            let i = tf.resolver.choose(sni.as_deref());
            s.cert_pat = tf.resolver.pattern(i).to_string();
        }
        s.sni = sni.unwrap_or_default();
        s.rt = None;
        inc(&self.st.w[self.id].tls_handshakes);
        self.enter_head(sid);
        let s = &mut self.sessions[sid];
        s.rd = true;
        s.wr = true;
        true
    }

    // ---------------------------------------------------------------
    // reescritura de cabeceras
    // ---------------------------------------------------------------

    fn build_request_head(s: &mut Session, m: &Msg, fallback_host: &str) -> bool {
        let src = s.inb.rslice();
        let tls = s.tls.is_some();
        let ip = s.ip.as_bytes();
        let mut w = Wb::new(match &mut s.uo.data {
            Some(d) => &mut d[s.uo.w..],
            None => return false,
        });
        w.put(m.method.get(src));
        w.put(b" ");
        w.put(m.target.get(src));
        w.put(b" HTTP/1.1\r\n");
        let hs = &m.headers[..m.nheaders];
        let last_xff = hs.iter().rposition(|h| ieq(h.name.get(src), "x-forwarded-for"));
        for (i, h) in hs.iter().enumerate() {
            let n = h.name.get(src);
            if m.is_hop_by_hop(src, n) || ieq(n, "x-forwarded-proto") || ieq(n, "x-real-ip") {
                continue;
            }
            w.put(n);
            w.put(b": ");
            w.put(h.value.get(src));
            if Some(i) == last_xff {
                w.put(b", ");
                w.put(ip);
            }
            w.put(b"\r\n");
        }
        if !m.has_host {
            w.put(b"Host: ");
            w.put(fallback_host.as_bytes());
            w.put(b"\r\n");
        }
        if last_xff.is_none() {
            w.put(b"X-Forwarded-For: ");
            w.put(ip);
            w.put(b"\r\n");
        }
        w.put(if tls { b"X-Forwarded-Proto: https\r\n" } else { b"X-Forwarded-Proto: http\r\n" });
        w.put(b"X-Real-IP: ");
        w.put(ip);
        w.put(b"\r\n");
        if s.want_upgrade {
            w.put(b"Connection: Upgrade\r\nUpgrade: ");
            w.put(m.upgrade.get(src));
            w.put(b"\r\n");
        }
        w.put(b"\r\n");
        if w.overflow {
            return false;
        }
        let n = w.len;
        s.uo.w += n;
        true
    }

    /// Escribe la cabecera de respuesta reescrita en out.
    /// Ok(true) escrita, Ok(false) sin sitio por ahora, Err: no cabe nunca.
    fn build_response_head(s: &mut Session, bp: &mut BufPool, m: &Msg, interim: bool) -> Result<bool, ()> {
        s.out.ensure(bp);
        s.out.compact();
        let pending = s.out.len();
        let src = s.ui.rslice();
        let mut w = Wb::new(&mut s.out.data.as_mut().unwrap()[s.out.w..]);
        w.put(format!("HTTP/1.1 {:03} ", m.status).as_bytes());
        w.put(m.reason.get(src));
        w.put(b"\r\n");
        let upgrade = m.status == 101;
        for h in &m.headers[..m.nheaders] {
            let n = h.name.get(src);
            if ieq(n, "x-backend-load") {
                continue;
            }
            let keep_101 = upgrade && (ieq(n, "connection") || ieq(n, "upgrade"));
            if !keep_101 && m.is_hop_by_hop(src, n) {
                continue;
            }
            w.put(n);
            w.put(b": ");
            w.put(h.value.get(src));
            w.put(b"\r\n");
        }
        if !interim && !upgrade {
            if !s.cli_keepalive {
                w.put(b"Connection: close\r\n");
            } else if s.client_v10 {
                w.put(b"Connection: keep-alive\r\n");
            }
        }
        w.put(b"\r\n");
        if w.overflow {
            return if pending > 0 { Ok(false) } else { Err(()) };
        }
        let n = w.len;
        s.out.w += n;
        Ok(true)
    }

    // ---------------------------------------------------------------
    // procesamiento por estado
    // ---------------------------------------------------------------

    fn dispatch(&mut self, sid: usize, m: &Msg) -> bool {
        let now = self.now;
        let stopping = self.stopping;
        let rt = self.rt.clone().expect("runtime");
        let s = &mut self.sessions[sid];
        let src = s.inb.rslice();
        s.t_req = now;
        s.status = 0;
        s.log_method = lossy(m.method.get(src), 15);
        s.log_path = lossy(m.target.get(src), 159);
        let host = String::from_utf8_lossy(m.host.get(src)).to_ascii_lowercase();
        s.log_host = host.chars().take(63).collect();

        s.client_v10 = m.version_minor == 0;
        s.cli_keepalive = if m.version_minor == 1 { !m.conn_close } else { m.conn_keepalive } && !stopping;
        s.head_req = ieq(m.method.get(src), "HEAD");
        s.want_upgrade = m.conn_upgrade && !m.upgrade.is_empty();
        s.rt = Some(rt.clone());

        let Some(fe) = rt.frontend(&s.fe_key) else {
            self.send_error(sid, 503); // frontend eliminado en una recarga
            return true;
        };
        if fe.tls.is_some() != s.tls.is_some() {
            self.send_error(sid, 503);
            return true;
        }
        if s.tls.is_some()
            && !s.sni.is_empty()
            && !host.is_empty()
            && !host.eq_ignore_ascii_case(&s.sni)
            && !(!s.cert_pat.is_empty() && pattern_match(&s.cert_pat, host.as_bytes()))
        {
            self.send_error(sid, 421);
            return true;
        }
        let Some(bi) = fe.router.lookup(host.as_bytes()) else {
            self.send_error(sid, 404);
            return true;
        };
        s.be = Some(rt.backends[bi].clone());

        let (mode, cl) = if m.chunked {
            (BodyMode::Chunked, 0)
        } else if m.content_length > 0 {
            (BodyMode::Length, m.content_length as u64)
        } else {
            (BodyMode::None, 0)
        };
        s.reqb = Body::new(mode, cl);
        s.req_done = s.reqb.done;
        s.retry_ok = mode == BodyMode::None && is_idempotent(m.method.get(src));

        s.uo.ensure(&mut self.bp);
        s.ui.ensure(&mut self.bp);
        if !Self::build_request_head(s, m, &fe.key) {
            self.send_error(sid, 431);
            return true;
        }
        s.inb.consume(m.head_len);
        s.scan_req = 0;
        s.st = SState::Proxy;
        s.t_state = now;
        s.resp_head_done = false;
        s.resp_done = false;
        s.tried = 0;
        s.attempts = 0;
        s.force_new = false;
        self.connect_upstream(sid);
        true
    }

    fn proc_head(&mut self, sid: usize) -> bool {
        let now = self.now;
        let stopping = self.stopping;
        let s = &mut self.sessions[sid];
        let len = s.inb.len();
        if len == 0 {
            if s.eof || stopping {
                if !s.out.is_empty() {
                    self.enter_closing(sid);
                } else {
                    self.sess_close(sid);
                }
                return true;
            }
            return false;
        }
        if !s.head_timer {
            s.head_timer = true;
            s.t_head = now;
        }
        let mut m = Msg::default();
        let mut r = http::parse_request(&mut m, s.inb.rslice(), HEAD_MAX, &mut s.scan_req);
        if r == Parse::Incomplete {
            if len >= BUF_SIZE {
                r = Parse::TooLarge;
            } else {
                if s.eof {
                    self.sess_close(sid);
                }
                return false;
            }
        }
        match r {
            Parse::TooLarge => {
                self.send_error(sid, 431);
                true
            }
            Parse::Error => {
                self.send_error(sid, 400);
                true
            }
            _ => self.dispatch(sid, &m),
        }
    }

    fn finish_request(&mut self, sid: usize) {
        let stopping = self.stopping;
        let s = &mut self.sessions[sid];
        if let Some(uid) = s.up.take() {
            let u = &self.ups[uid];
            let reusable =
                s.up_reusable && !u.eof && s.ui.is_empty() && s.req_done && s.respb.mode != BodyMode::Eof && !stopping;
            let srv = u.server();
            Backend::passive_ok(srv);
            srv.active.set(srv.active.get() - 1);
            if reusable {
                self.pool_put(uid);
            } else {
                self.up_close(uid);
            }
        }
        let status = self.sessions[sid].status;
        self.account(sid, status);
        let s = &mut self.sessions[sid];
        s.rt = None;
        s.be = None;
        if !s.cli_keepalive || !s.req_done {
            self.enter_closing(sid);
        } else {
            self.enter_head(sid);
        }
    }

    fn proc_response_head(&mut self, sid: usize) -> bool {
        let now = self.now;
        let Some(uid) = self.sessions[sid].up else { return false };
        let u_eof = self.ups[uid].eof;
        let s = &mut self.sessions[sid];
        let len = s.ui.len();
        if len == 0 {
            if u_eof {
                self.upstream_failed(sid, Uf::Io);
                return true;
            }
            return false;
        }
        let mut m = Msg::default();
        let mut r = http::parse_response(&mut m, s.ui.rslice(), HEAD_MAX, &mut s.scan_resp);
        if r == Parse::Incomplete {
            if len >= BUF_SIZE {
                r = Parse::TooLarge;
            } else if u_eof {
                r = Parse::Error;
            } else {
                return false;
            }
        }
        if r != Parse::Complete {
            let u = &self.ups[uid];
            logw!("respuesta inválida de {} ({})", u.server().addr_str, u.be.name);
            s.resp_started = true; // no reintentar
            self.up_detach_close(sid);
            self.send_error(sid, 502);
            return true;
        }
        s.retry_ok = false;
        if s.uo.r == s.uo.w {
            s.uo.r = 0;
            s.uo.w = 0;
        }

        if (100..200).contains(&m.status) && m.status != 101 {
            // respuesta intermedia (100 Continue...): se reenvía salvo a HTTP/1.0
            if !s.client_v10 {
                match Self::build_response_head(s, &mut self.bp, &m, true) {
                    Ok(true) => {}
                    Ok(false) => return false,
                    Err(()) => {
                        self.send_error(sid, 502);
                        return true;
                    }
                }
            }
            s.ui.consume(m.head_len);
            s.scan_resp = 0;
            return true;
        }
        if m.status == 101 {
            if !s.want_upgrade {
                self.up_detach_close(sid);
                self.send_error(sid, 502);
                return true;
            }
            match Self::build_response_head(s, &mut self.bp, &m, false) {
                Ok(true) => {}
                Ok(false) => return false,
                Err(()) => {
                    self.send_error(sid, 502);
                    return true;
                }
            }
            s.ui.consume(m.head_len);
            s.status = 101;
            s.resp_head_done = true;
            s.st = SState::Tunnel;
            s.t_state = now;
            return true;
        }

        if let Some(bl) = m.backend_load
            && let Some(v) = parse_load(bl.get(s.ui.rslice()))
        {
            let u = &self.ups[uid];
            u.be.report_load(u.server(), v, now);
        }
        let (mode, cl) = if s.head_req || m.status == 204 || m.status == 304 {
            (BodyMode::None, 0)
        } else if m.chunked {
            (BodyMode::Chunked, 0)
        } else if m.content_length >= 0 {
            (if m.content_length > 0 { BodyMode::Length } else { BodyMode::None }, m.content_length as u64)
        } else {
            (BodyMode::Eof, 0)
        };
        if mode == BodyMode::Eof {
            s.cli_keepalive = false;
            s.up_reusable = false;
        }
        if m.conn_close || (m.version_minor == 0 && !m.conn_keepalive) {
            s.up_reusable = false;
        }
        if !s.req_done {
            s.cli_keepalive = false; // respuesta antes de terminar el cuerpo
        }
        match Self::build_response_head(s, &mut self.bp, &m, false) {
            Ok(true) => {}
            Ok(false) => return false,
            Err(()) => {
                self.send_error(sid, 502);
                return true;
            }
        }
        s.ui.consume(m.head_len);
        s.status = m.status;
        s.resp_head_done = true;
        s.respb = Body::new(mode, cl);
        s.resp_done = s.respb.done;
        true
    }

    fn proc_proxy(&mut self, sid: usize) -> bool {
        let mut p = false;
        let s = &mut self.sessions[sid];

        // cuerpo de la petición: inb -> uo
        if !s.req_done && !s.inb.is_empty() {
            if s.uo.space() == 0 {
                s.uo.compact();
            }
            let n = s.inb.len().min(s.uo.space());
            if n > 0 {
                let Some(k) = s.reqb.feed(&s.inb.rslice()[..n]) else {
                    self.send_error(sid, 400);
                    return true;
                };
                let (inb, uo) = (&mut s.inb, &mut s.uo);
                uo.put(&inb.rslice()[..k]);
                inb.consume(k);
                if s.reqb.done {
                    s.req_done = true;
                }
                if k > 0 || s.req_done {
                    p = true;
                }
            }
        }
        if !s.req_done && s.eof && s.inb.is_empty() {
            self.sess_close(sid); // el cliente cortó a mitad del cuerpo
            return true;
        }
        if s.up.is_none() {
            return p;
        }

        if !s.resp_head_done {
            if !self.proc_response_head(sid) {
                return p;
            }
            p = true;
            let s = &self.sessions[sid];
            if s.closed || s.st != SState::Proxy || !s.resp_head_done {
                return p;
            }
        }

        // cuerpo de la respuesta: ui -> out
        let s = &mut self.sessions[sid];
        let Some(uid) = s.up else { return p };
        if !s.resp_done && !s.ui.is_empty() {
            if s.out.space() == 0 {
                s.out.compact();
            }
            let n = s.ui.len().min(s.out.space());
            if n > 0 {
                let Some(k) = s.respb.feed(&s.ui.rslice()[..n]) else {
                    logw!("chunked inválido desde {}", self.ups[uid].server().addr_str);
                    s.up_reusable = false;
                    self.sess_close(sid);
                    return true;
                };
                let (ui, out) = (&mut s.ui, &mut s.out);
                out.put(&ui.rslice()[..k]);
                ui.consume(k);
                if s.respb.done {
                    s.resp_done = true;
                }
                if k > 0 || s.resp_done {
                    p = true;
                }
            }
        }
        if !s.resp_done && self.ups[uid].eof && s.ui.is_empty() {
            if s.respb.mode == BodyMode::Eof {
                s.resp_done = true;
            } else {
                // respuesta truncada: se entrega lo que hay y se cierra
                self.up_detach_close(sid);
                self.enter_closing(sid);
                return true;
            }
        }
        if s.resp_done {
            if !s.req_done {
                // El backend respondió sin esperar al cuerpo: no se puede
                // reutilizar ninguna de las dos conexiones.
                s.up_reusable = false;
            }
            self.finish_request(sid);
            return true;
        }
        p
    }

    fn proc_tunnel(&mut self, sid: usize) -> bool {
        let mut p = false;
        let s = &mut self.sessions[sid];
        if !s.inb.is_empty() && buf_move(&mut s.uo, &mut s.inb) > 0 {
            p = true;
        }
        if !s.ui.is_empty() {
            s.out.ensure(&mut self.bp);
            if buf_move(&mut s.out, &mut s.ui) > 0 {
                p = true;
            }
        }
        let cli_done = s.eof && s.inb.is_empty() && s.uo.is_empty();
        let up_done = match s.up {
            None => true,
            Some(uid) => self.ups[uid].eof && s.ui.is_empty(),
        };
        if cli_done || up_done {
            self.account(sid, 101);
            self.up_detach_close(sid);
            self.enter_closing(sid);
            return true;
        }
        p
    }

    fn process(&mut self, sid: usize) -> bool {
        match self.sessions[sid].st {
            SState::Head => self.proc_head(sid),
            SState::Proxy => self.proc_proxy(sid),
            SState::Tunnel => self.proc_tunnel(sid),
            SState::Linger => {
                let s = &mut self.sessions[sid];
                s.inb.r = 0;
                s.inb.w = 0;
                if s.eof {
                    self.sess_close(sid);
                    return true;
                }
                false
            }
            SState::Handshake | SState::Closing => false,
        }
    }

    fn start_linger(&mut self, sid: usize) {
        let now = self.now;
        let s = &mut self.sessions[sid];
        if let Some(tls) = s.tls.as_mut() {
            tls.send_close_notify(); // no se espera respuesta
            while tls.wants_write() {
                if tls.write_tls(&mut s.sock).is_err() {
                    break;
                }
            }
        }
        let _ = s.sock.shutdown(Shutdown::Write);
        s.st = SState::Linger;
        s.t_state = now;
        if s.eof {
            self.sess_close(sid);
        }
    }

    fn update_timer(&mut self, sid: usize) {
        let t = self.to();
        let tok = self.sess_token(sid);
        let s = &mut self.sessions[sid];
        let connecting = s.up.is_some_and(|u| self.ups[u].connecting);
        let dl = match s.st {
            SState::Handshake => s.t_accept + t.client_header,
            SState::Head if s.head_timer => s.t_head + t.client_header,
            SState::Head if !s.out.is_empty() => s.t_progress + t.client_idle,
            SState::Head => s.t_state + t.client_idle,
            SState::Proxy if connecting => s.t_conn + t.upstream_connect,
            SState::Proxy => s.t_progress + t.upstream_read,
            SState::Tunnel => s.t_progress + t.client_idle,
            SState::Closing => s.t_state + CLOSING_MS,
            SState::Linger => s.t_state + LINGER_MS,
        };
        self.timers.set(&mut s.timer, dl, tok);
    }

    pub fn sess_timeout(&mut self, sid: usize) {
        let s = &self.sessions[sid];
        if s.closed {
            return;
        }
        match s.st {
            SState::Head => {
                if !s.head_timer {
                    self.sess_close(sid);
                    return;
                }
                self.send_error(sid, 408);
            }
            SState::Proxy => {
                let connecting = s.up.is_some_and(|u| self.ups[u].connecting);
                if connecting {
                    self.upstream_failed(sid, Uf::ConnectTimeout);
                } else if !s.resp_head_done {
                    logw!("timeout esperando respuesta de {}", s.log_server);
                    self.send_error(sid, 504);
                } else {
                    self.sess_close(sid);
                    return;
                }
            }
            _ => {
                self.sess_close(sid);
                return;
            }
        }
        if !self.sessions[sid].closed {
            self.sess_drive(sid);
        }
    }

    pub fn sess_drive(&mut self, sid: usize) {
        for _ in 0..DRIVE_BUDGET {
            let mut p = false;
            if self.sessions[sid].st == SState::Handshake {
                p = self.do_handshake(sid);
                let s = &self.sessions[sid];
                if s.closed {
                    return;
                }
                if s.st == SState::Handshake {
                    if !p {
                        break;
                    }
                    continue;
                }
            }
            p |= self.cli_read(sid);
            if self.sessions[sid].closed {
                return;
            }
            p |= self.process(sid);
            if self.sessions[sid].closed {
                return;
            }
            p |= self.up_io(sid);
            if self.sessions[sid].closed {
                return;
            }
            p |= self.process(sid);
            if self.sessions[sid].closed {
                return;
            }
            p |= self.cli_write(sid);
            let s = &self.sessions[sid];
            if s.closed {
                return;
            }
            if s.st == SState::Closing && s.out.is_empty() && !s.tls.as_ref().is_some_and(|t| t.wants_write()) {
                self.start_linger(sid);
                if self.sessions[sid].closed {
                    return;
                }
                p = true;
            }
            if !p {
                self.update_timer(sid);
                return;
            }
            self.sessions[sid].t_progress = self.now;
        }
        self.update_timer(sid);
        let s = &mut self.sessions[sid];
        if !s.redrive && s.st != SState::Handshake {
            s.redrive = true;
            let g = s.slot_gen;
            self.redrive.push((sid, g));
        }
    }

    pub fn cli_event(&mut self, sid: usize, g: u32, rd: bool, wr: bool) {
        let Some(s) = self.sessions.get_gen_mut(sid, g) else { return };
        if s.closed {
            return;
        }
        s.rd |= rd;
        s.wr |= wr;
        self.sess_drive(sid);
    }

    pub fn session_accept(&mut self, sock: TcpStream, peer: SocketAddr, fe_key: &str) {
        let Some(rt) = self.rt.clone() else { return };
        let Some(fe) = rt.frontend(fe_key) else { return };
        let _ = sock.set_nodelay(true);
        let now = self.now;
        let tls = match &fe.tls {
            Some(t) => match ServerConnection::new(t.config.clone()) {
                Ok(mut c) => {
                    c.set_buffer_limit(Some(TLS_BUFFER_LIMIT));
                    Some(Box::new(c))
                }
                Err(e) => {
                    loge!("TLS: {e}");
                    return;
                }
            },
            None => None,
        };
        let is_tls = tls.is_some();
        let (sid, g) = self.sessions.insert(Session {
            sock,
            tls,
            st: if is_tls { SState::Handshake } else { SState::Head },
            rd: true, // optimista: puede haber datos ya
            wr: true,
            eof: false,
            closed: false,
            redrive: false,
            cli_keepalive: false,
            client_v10: false,
            head_req: false,
            want_upgrade: false,
            retry_ok: false,
            req_done: false,
            resp_head_done: false,
            resp_done: false,
            up_reusable: false,
            resp_started: false,
            head_timer: false,
            status: 0,
            fe_key: Rc::from(fe_key),
            ip: peer.ip().to_string(),
            sni: String::new(),
            cert_pat: String::new(),
            inb: Buf::default(),
            out: Buf::default(),
            uo: Buf::default(),
            ui: Buf::default(),
            scan_req: 0,
            scan_resp: 0,
            reqb: Body::default(),
            respb: Body::default(),
            up: None,
            // el resolvedor SNI usa datos de esta generación
            rt: if is_tls { Some(rt.clone()) } else { None },
            be: None,
            tried: 0,
            attempts: 0,
            force_new: false,
            t_accept: now,
            t_state: now,
            t_head: 0,
            t_req: 0,
            t_conn: 0,
            t_progress: now,
            log_method: String::new(),
            log_host: String::new(),
            log_path: String::new(),
            log_server: String::new(),
            timer: TimerState::default(),
            slot_gen: 0,
        });
        self.sessions[sid].slot_gen = g;
        let tok = Token(token(K_SESS, sid, g) as usize);
        if let Err(e) =
            self.poll.registry().register(&mut self.sessions[sid].sock, tok, Interest::READABLE | Interest::WRITABLE)
        {
            loge!("registro del cliente: {e}");
            self.sessions.remove(sid);
            return;
        }
        self.nsessions += 1;
        let ws = &self.st.w[self.id];
        inc(&ws.accepted);
        ws.active_conns.fetch_add(1, Relaxed);
        self.sess_drive(sid);
    }

    /// Cierra las sesiones sin petición en curso (parada ordenada).
    pub fn sessions_close_idle(&mut self) {
        for sid in self.sessions.keys() {
            let s = &self.sessions[sid];
            if s.closed {
                continue;
            }
            if (s.st == SState::Head && s.inb.is_empty() && s.out.is_empty()) || s.st == SState::Handshake {
                self.sess_close(sid);
            }
        }
    }

    pub fn sessions_close_all(&mut self) {
        for sid in self.sessions.keys() {
            self.sess_close(sid);
        }
    }
}
