//! Hilo worker: un event loop (mio: epoll/kqueue, edge-triggered) con sus
//! listeners, sesiones, upstreams y timers. Todo su estado es local al hilo;
//! con el master solo habla por un canal (config, parada) y con el resto del
//! proceso por las estadísticas atómicas.

use std::io;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::mpsc::{Receiver, TryRecvError};
use std::time::Duration;

use mio::{Events, Interest, Poll, Token};

use crate::buffer::BufPool;
use crate::conn::{Session, Upstream};
use crate::runtime::{Compiled, Runtime};
use crate::slab::{Slab, token, untoken};
use crate::stats::{ServerStat, Stats, WorkerStats};
use crate::timer::{Pop, TimerState, Timers};
use crate::util::mono_ms;
use crate::{log, logd, loge, logi, logw};

const TICK_MS: u64 = 250;
const STOP_GRACE_MS: u64 = 10_000;

pub const K_CTRL: u64 = 0;
pub const K_LISTEN: u64 = 1;
pub const K_SESS: u64 = 2;
pub const K_UP: u64 = 3;

/// Token del Waker del canal.
pub const WAKE: Token = Token(0);
const TICK_TOKEN: u64 = 1; // K_CTRL, índice 1

pub enum WorkerMsg {
    Config(Arc<Compiled>),
    Stop,
    /// Solo en builds de depuración: provoca un pánico (tests de relanzado).
    Panic,
}

pub struct Listener {
    pub key: String,
    pub sock: mio::net::TcpListener,
    pub retry: TimerState,
    pub keep: bool,
    pub slot_gen: u32,
    pub closed: bool,
}

pub struct Worker {
    pub id: usize,
    pub poll: Poll,
    pub now: u64,
    pub bp: BufPool,
    pub st: Arc<Stats>,
    pub rt: Option<Rc<Runtime>>,
    pub retired: Vec<Rc<Runtime>>,
    pub generation: u64,
    pub listeners: Slab<Listener>,
    pub sessions: Slab<Session>,
    pub nsessions: usize,
    pub ups: Slab<Upstream>,
    pub timers: Timers,
    /// Sesiones a re-planificar al final de la iteración (índice, generación).
    pub redrive: Vec<(usize, u32)>,
    pub dead_sessions: Vec<usize>,
    pub dead_ups: Vec<usize>,
    pub dead_listeners: Vec<usize>,
    pub stopping: bool,
    pub stop_deadline: u64,
    pub stop_loop: bool,
    tick: TimerState,
    rx: Receiver<WorkerMsg>,
    exit_code: i32,
}

/// Socket de escucha con SO_REUSEPORT (Linux reparte las conexiones entre
/// los sockets del grupo, uno por worker).
pub fn listener_open(addr: &std::net::SocketAddr, reuseport: bool) -> io::Result<std::net::TcpListener> {
    use socket2::{Domain, Socket, Type};
    let s = Socket::new(Domain::for_address(*addr), Type::STREAM, None)?;
    s.set_reuse_address(true)?;
    #[cfg(unix)]
    if reuseport {
        s.set_reuse_port(true)?;
    }
    if addr.is_ipv6() {
        s.set_only_v6(true)?;
    }
    s.bind(&(*addr).into())?;
    s.listen(4096)?;
    s.set_nonblocking(true)?;
    Ok(s.into())
}

/// Comprueba que la dirección se puede enlazar (sin quedarse el socket).
pub fn listener_test_bind(addr: &std::net::SocketAddr) -> io::Result<()> {
    use socket2::{Domain, Socket, Type};
    let s = Socket::new(Domain::for_address(*addr), Type::STREAM, None)?;
    s.set_reuse_address(true)?;
    s.bind(&(*addr).into())
}

impl Worker {
    pub fn ws(&self) -> &WorkerStats {
        &self.st.w[self.id]
    }

    // ---------------------------------------------------------------
    // generaciones de configuración
    // ---------------------------------------------------------------

    fn reap_retired(&mut self) {
        let mut i = 0;
        while i < self.retired.len() {
            if Rc::strong_count(&self.retired[i]) == 1 {
                let rt = self.retired.swap_remove(i);
                for b in &rt.backends {
                    for s in &b.servers {
                        self.upstream_pool_drain(s);
                    }
                }
                logd!("generación {} liberada", rt.generation);
            } else {
                i += 1;
            }
        }
    }

    fn handle_config(&mut self, c: Arc<Compiled>) {
        self.generation += 1;
        let rt = Rc::new(Runtime::new(c, self.generation));
        log::set_level(rt.c.cfg.log_level);
        self.apply_listeners(&rt);
        if let Some(old) = self.rt.replace(rt) {
            // swap: las peticiones nuevas ven la nueva generación
            if let Some(h) = &old.health {
                h.stop();
            }
            self.retired.push(old);
            self.ws().reloads.fetch_add(1, Relaxed);
            logi!("configuración aplicada (generación {})", self.generation);
        }
    }

    // ---------------------------------------------------------------
    // listeners
    // ---------------------------------------------------------------

    /// Mantiene los listeners existentes, abre los nuevos y cierra los
    /// eliminados.
    fn apply_listeners(&mut self, rt: &Runtime) {
        for i in self.listeners.keys() {
            self.listeners[i].keep = false;
        }
        for (fi, fe) in rt.c.fes.iter().enumerate() {
            if let Some(i) = self.listeners.keys().into_iter().find(|&i| self.listeners[i].key == fe.key) {
                self.listeners[i].keep = true;
                continue;
            }
            let addr = rt.c.cfg.frontends[fi].addr;
            let opened = if cfg!(target_os = "linux") {
                listener_open(&addr, true)
            } else {
                match rt.c.listeners.iter().find(|(k, _)| *k == fe.key) {
                    Some((_, l)) => l.try_clone(),
                    None => Err(io::Error::other("el master no abrió el socket")),
                }
            };
            let std_l = match opened {
                Ok(l) => l,
                Err(e) => {
                    loge!("no se puede escuchar en {} ({}): {e}", fe.key, fe.name);
                    continue;
                }
            };
            let _ = std_l.set_nonblocking(true);
            let sock = mio::net::TcpListener::from_std(std_l);
            let (idx, g) = self.listeners.insert(Listener {
                key: fe.key.clone(),
                sock,
                retry: TimerState::default(),
                keep: true,
                slot_gen: 0,
                closed: false,
            });
            self.listeners[idx].slot_gen = g;
            let tok = Token(token(K_LISTEN, idx, g) as usize);
            if let Err(e) = self.poll.registry().register(&mut self.listeners[idx].sock, tok, Interest::READABLE) {
                loge!("registro del listener {}: {e}", fe.key);
                self.listeners.remove(idx);
                continue;
            }
            logi!("escuchando en {} ({}{})", fe.key, fe.name, if fe.tls.is_some() { ", tls" } else { "" });
            // Puede haber conexiones en cola desde antes del registro.
            self.listener_event(idx);
        }
        for i in self.listeners.keys() {
            if !self.listeners[i].keep && !self.listeners[i].closed {
                logi!("dejando de escuchar en {}", self.listeners[i].key);
                self.listener_close(i);
            }
        }
    }

    fn listener_close(&mut self, i: usize) {
        let l = &mut self.listeners[i];
        if l.closed {
            return;
        }
        l.closed = true;
        Timers::cancel(&mut l.retry);
        let _ = self.poll.registry().deregister(&mut l.sock);
        self.dead_listeners.push(i);
    }

    fn close_all_listeners(&mut self) {
        for i in self.listeners.keys() {
            self.listener_close(i);
        }
    }

    fn listener_event(&mut self, i: usize) {
        loop {
            let l = &mut self.listeners[i];
            if l.closed {
                return;
            }
            match l.sock.accept() {
                Ok((sock, peer)) => {
                    let key = l.key.clone();
                    self.session_accept(sock, peer, &key);
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return,
                Err(e) if matches!(e.kind(), io::ErrorKind::Interrupted | io::ErrorKind::ConnectionAborted) => {}
                Err(e) => {
                    // EMFILE, ENFILE, ENOBUFS...: reintento en 100 ms
                    loge!("accept en {}: {e} (reintento en 100 ms)", l.key);
                    let tok = token(K_LISTEN, i, l.slot_gen);
                    self.timers.set(&mut l.retry, self.now + 100, tok);
                    return;
                }
            }
        }
    }

    // ---------------------------------------------------------------
    // canal con el master
    // ---------------------------------------------------------------

    fn begin_stop(&mut self) {
        if self.stopping {
            return;
        }
        self.stopping = true;
        self.stop_deadline = self.now + STOP_GRACE_MS;
        logi!("parada ordenada: {} conexiones abiertas", self.nsessions);
        self.close_all_listeners();
        self.sessions_close_idle();
    }

    fn chan_event(&mut self) {
        loop {
            match self.rx.try_recv() {
                Ok(WorkerMsg::Config(c)) => self.handle_config(c),
                Ok(WorkerMsg::Stop) => self.begin_stop(),
                Ok(WorkerMsg::Panic) => {
                    if cfg!(debug_assertions) {
                        panic!("pánico provocado en el worker {} (SIGUSR1, solo depuración)", self.id);
                    }
                }
                Err(TryRecvError::Empty) => return,
                Err(TryRecvError::Disconnected) => {
                    if !self.stopping {
                        logw!("canal con el master cerrado");
                    }
                    self.begin_stop();
                    return;
                }
            }
        }
    }

    // ---------------------------------------------------------------
    // tick periódico
    // ---------------------------------------------------------------

    fn publish_stats(&mut self) {
        let ws = &self.st.w[self.id];
        ws.buffers_in_use.store(self.bp.in_use() as u64, Relaxed);
        let Some(rt) = &self.rt else { return };
        let now = self.now;
        let mut v = Vec::new();
        for b in &rt.backends {
            for s in &b.servers {
                let fresh = s.load_known.get() && now.saturating_sub(s.load_ts.get()) <= crate::balancer::LOAD_STALE_MS;
                v.push(ServerStat {
                    backend: b.name.clone(),
                    server: s.addr_str.clone(),
                    up: b.is_up(s, now),
                    active: s.active.get() as i64,
                    idle: s.idle.borrow().len() as i64,
                    load: fresh.then(|| s.load.get()),
                    requests: s.requests.get(),
                    failures: s.failures.get(),
                });
            }
        }
        *ws.servers.lock().unwrap_or_else(|e| e.into_inner()) = v;
    }

    fn on_tick(&mut self) {
        self.reap_retired();
        self.publish_stats();
        if self.stopping {
            if self.nsessions == 0 || self.now >= self.stop_deadline {
                self.stop_loop = true;
                return;
            }
            self.sessions_close_idle();
        }
        let now = self.now;
        self.timers.set(&mut self.tick, now + TICK_MS, TICK_TOKEN);
    }

    // ---------------------------------------------------------------
    // event loop
    // ---------------------------------------------------------------

    fn run_timers(&mut self) {
        let now = self.now;
        while let Pop::Due(tok, key) = self.timers.pop_due(now) {
            let (kind, idx, g) = untoken(tok);
            match kind {
                K_CTRL => {
                    if self.timers.check_due(&mut self.tick, key, tok, now) {
                        self.on_tick();
                    }
                }
                K_LISTEN => {
                    let Some(l) = self.listeners.get_gen_mut(idx, g) else { continue };
                    if self.timers.check_due(&mut l.retry, key, tok, now) {
                        self.listener_event(idx);
                    }
                }
                K_SESS => {
                    let Some(s) = self.sessions.get_gen_mut(idx, g) else { continue };
                    if !s.closed && self.timers.check_due(&mut s.timer, key, tok, now) {
                        self.sess_timeout(idx);
                    }
                }
                _ => {
                    let Some(u) = self.ups.get_gen_mut(idx, g) else { continue };
                    if !u.closed && self.timers.check_due(&mut u.timer, key, tok, now) {
                        self.up_close(idx); // ocioso demasiado tiempo en el pool
                    }
                }
            }
        }
    }

    /// Re-planificaciones y liberaciones diferidas (fin de iteración).
    fn run_deferred(&mut self) {
        let redrive = std::mem::take(&mut self.redrive);
        for (i, g) in redrive {
            if let Some(s) = self.sessions.get_gen_mut(i, g) {
                s.redrive = false;
                if !s.closed {
                    self.sess_drive(i);
                }
            }
        }
        for i in std::mem::take(&mut self.dead_sessions) {
            if let Some(mut s) = self.sessions.remove(i) {
                s.release_bufs(&mut self.bp);
            }
        }
        for i in std::mem::take(&mut self.dead_ups) {
            self.ups.remove(i);
        }
        for i in std::mem::take(&mut self.dead_listeners) {
            self.listeners.remove(i);
        }
    }

    fn run(&mut self) {
        let mut events = Events::with_capacity(1024);
        self.stop_loop = false;
        while !self.stop_loop {
            self.now = mono_ms();
            let timeout = if !self.redrive.is_empty() {
                Some(Duration::ZERO)
            } else {
                self.timers.next_in(self.now).map(|ms| Duration::from_millis(ms.min(60_000)))
            };
            if let Err(e) = self.poll.poll(&mut events, timeout)
                && e.kind() != io::ErrorKind::Interrupted
            {
                loge!("poll: {e}");
                return;
            }
            self.now = mono_ms();
            for ev in events.iter() {
                let (kind, idx, g) = untoken(ev.token().0 as u64);
                let rd = ev.is_readable() || ev.is_read_closed() || ev.is_error();
                match kind {
                    K_CTRL => self.chan_event(),
                    K_LISTEN => {
                        if self.listeners.get_gen_mut(idx, g).is_some() {
                            self.listener_event(idx);
                        }
                    }
                    K_SESS => {
                        let wr = ev.is_writable() || ev.is_write_closed() || ev.is_error() || ev.is_read_closed();
                        self.cli_event(idx, g, rd, wr);
                    }
                    _ => {
                        let wr = ev.is_writable() || ev.is_error();
                        let hup = ev.is_read_closed() || ev.is_error();
                        self.up_event(idx, g, rd, wr, hup);
                    }
                }
            }
            self.run_timers();
            self.run_deferred();
        }
    }
}

/// Cuerpo del hilo worker; vuelve tras Stop o al cerrarse el canal.
pub fn worker_main(id: usize, poll: Poll, rx: Receiver<WorkerMsg>, st: Arc<Stats>) -> i32 {
    log::set_thread_tag(&format!("w{id}"));
    let ws = &st.w[id];
    ws.active_conns.store(0, Relaxed);
    ws.alive.store(true, Relaxed);
    let mut w = Worker {
        id,
        poll,
        now: mono_ms(),
        bp: BufPool::new(),
        st: st.clone(),
        rt: None,
        retired: Vec::new(),
        generation: 0,
        listeners: Slab::default(),
        sessions: Slab::default(),
        nsessions: 0,
        ups: Slab::default(),
        timers: Timers::new(),
        redrive: Vec::new(),
        dead_sessions: Vec::new(),
        dead_ups: Vec::new(),
        dead_listeners: Vec::new(),
        stopping: false,
        stop_deadline: 0,
        stop_loop: false,
        tick: TimerState::default(),
        rx,
        exit_code: 0,
    };
    w.chan_event(); // la config inicial ya está en el canal
    if w.rt.is_none() {
        loge!("sin configuración válida, el worker termina");
        w.exit_code = 2;
    } else {
        let now = w.now;
        w.timers.set(&mut w.tick, now + TICK_MS, TICK_TOKEN);
        logi!("worker {id} arrancado (hilo)");
        w.run();
    }

    // salida ordenada
    w.close_all_listeners();
    w.sessions_close_all();
    if let Some(rt) = w.rt.take() {
        if let Some(h) = &rt.health {
            h.stop();
        }
        w.retired.push(rt);
    }
    w.run_deferred();
    w.reap_retired();
    w.run_deferred();
    logi!("worker {id} terminado");
    w.exit_code
}
