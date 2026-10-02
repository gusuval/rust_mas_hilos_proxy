//! Hilo master: valida la config, lanza N hilos worker (cada uno con su
//! event loop), atiende las señales del proceso, vigila el fichero de
//! config, reparte las recargas y sirve las estadísticas.
//!
//! Los workers son hilos del mismo proceso, no procesos hijos: cada uno tiene
//! su estado y solo habla con el master por su canal. Un `panic` en un
//! worker termina solo ese hilo; el master lo detecta (el guardia del hilo
//! avisa al soltarse, también durante el unwinding) y lo relanza. Un fallo
//! de memoria (SIGSEGV) sí terminaría todo el proceso, pero el código del
//! proxy no usa `unsafe` fuera de las llamadas al sistema de este módulo.

use std::io::{self, Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::net::UnixListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering::Relaxed};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::thread::JoinHandle;
use std::time::Duration;

use mio::unix::SourceFd;
use mio::{Events, Interest, Poll, Token, Waker};

use crate::config::{self, Config};
use crate::runtime::Compiled;
use crate::stats::Stats;
use crate::util::mono_ms;
use crate::worker::{self, WorkerMsg};
use crate::{log, loge, logi, logw};

const RELOAD_DEBOUNCE_MS: u64 = 200;
const RESPAWN_DELAY_MS: u64 = 500;
const SHUTDOWN_GRACE_MS: u64 = 12_000;

const T_SIG: Token = Token(1);
const T_WAKE: Token = Token(2);
const T_STATS: Token = Token(3);
const T_WATCH: Token = Token(4);

// ------------------------------------------------------------------
// señales: self-pipe trick
// ------------------------------------------------------------------

static SIG_FD: AtomicI32 = AtomicI32::new(-1);

#[cfg(target_os = "linux")]
unsafe fn errno_ptr() -> *mut libc::c_int {
    unsafe { libc::__errno_location() }
}
#[cfg(not(target_os = "linux"))]
unsafe fn errno_ptr() -> *mut libc::c_int {
    unsafe { libc::__error() }
}

extern "C" fn on_signal(sig: libc::c_int) {
    // Solo llamadas async-signal-safe; se conserva errno.
    unsafe {
        let saved = *errno_ptr();
        let b = sig as u8;
        libc::write(SIG_FD.load(Relaxed), &b as *const u8 as *const libc::c_void, 1);
        *errno_ptr() = saved;
    }
}

fn install_signals(fd: RawFd) {
    SIG_FD.store(fd, Relaxed);
    let mut sigs = vec![libc::SIGHUP, libc::SIGTERM, libc::SIGINT, libc::SIGQUIT];
    if cfg!(debug_assertions) {
        sigs.push(libc::SIGUSR1); // provoca un pánico en un worker (tests)
    }
    for sig in sigs {
        unsafe {
            let mut sa: libc::sigaction = std::mem::zeroed();
            sa.sa_sigaction = on_signal as *const () as libc::sighandler_t;
            sa.sa_flags = libc::SA_RESTART;
            libc::sigemptyset(&mut sa.sa_mask);
            libc::sigaction(sig, &sa, std::ptr::null_mut());
        }
    }
}

/// Bloquea todas las señales en el hilo actual; devuelve la máscara previa.
fn block_all_signals() -> libc::sigset_t {
    unsafe {
        let mut all: libc::sigset_t = std::mem::zeroed();
        let mut old: libc::sigset_t = std::mem::zeroed();
        libc::sigfillset(&mut all);
        libc::pthread_sigmask(libc::SIG_SETMASK, &all, &mut old);
        old
    }
}

fn restore_signals(old: &libc::sigset_t) {
    unsafe {
        libc::pthread_sigmask(libc::SIG_SETMASK, old, std::ptr::null_mut());
    }
}

// ------------------------------------------------------------------
// vigilancia del fichero de config
// ------------------------------------------------------------------

/// inotify sobre el directorio (soporta el guardado atómico por rename).
#[cfg(target_os = "linux")]
struct Watch {
    fd: OwnedFd,
    name: Vec<u8>,
}

#[cfg(target_os = "linux")]
impl Watch {
    fn open(path: &str) -> Option<Watch> {
        let dir = config::base_dir(path);
        let name = path.rsplit('/').next().unwrap_or(path).as_bytes().to_vec();
        let cdir = std::ffi::CString::new(dir).ok()?;
        unsafe {
            let fd = libc::inotify_init1(libc::IN_NONBLOCK | libc::IN_CLOEXEC);
            if fd < 0 {
                return None;
            }
            let fd = OwnedFd::from_raw_fd(fd);
            let mask = libc::IN_CLOSE_WRITE
                | libc::IN_MOVED_TO
                | libc::IN_CREATE
                | libc::IN_DELETE
                | libc::IN_MODIFY
                | libc::IN_ATTRIB;
            if libc::inotify_add_watch(fd.as_raw_fd(), cdir.as_ptr(), mask) < 0 {
                return None;
            }
            Some(Watch { fd, name })
        }
    }

    fn raw_fd(&self) -> Option<RawFd> {
        Some(self.fd.as_raw_fd())
    }

    /// true si algún evento afecta al fichero vigilado.
    fn consume(&mut self) -> bool {
        let mut buf = [0u8; 8192];
        let mut hit = false;
        let hdr = std::mem::size_of::<libc::inotify_event>();
        loop {
            let n = unsafe { libc::read(self.fd.as_raw_fd(), buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
            if n <= 0 {
                break;
            }
            let mut p = 0;
            while p + hdr <= n as usize {
                let len = u32::from_ne_bytes(buf[p + 12..p + 16].try_into().unwrap()) as usize;
                let name = &buf[p + hdr..(p + hdr + len).min(n as usize)];
                let name = &name[..name.iter().position(|&c| c == 0).unwrap_or(name.len())];
                if len > 0 && name == self.name.as_slice() {
                    hit = true;
                }
                p += hdr + len;
            }
        }
        hit
    }
}

/// Fuera de Linux: sondeo de mtime/tamaño cada 500 ms (no probado en macOS).
#[cfg(not(target_os = "linux"))]
struct Watch {
    path: String,
    last: Option<(std::time::SystemTime, u64)>,
}

#[cfg(not(target_os = "linux"))]
impl Watch {
    fn open(path: &str) -> Option<Watch> {
        let mut w = Watch { path: path.to_string(), last: None };
        w.last = w.stamp();
        Some(w)
    }
    fn stamp(&self) -> Option<(std::time::SystemTime, u64)> {
        let m = std::fs::metadata(&self.path).ok()?;
        Some((m.modified().ok()?, m.len()))
    }
    fn raw_fd(&self) -> Option<RawFd> {
        None
    }
    fn consume(&mut self) -> bool {
        let now = self.stamp();
        let changed = now != self.last;
        self.last = now;
        changed
    }
}

// ------------------------------------------------------------------
// master
// ------------------------------------------------------------------

#[derive(Default)]
struct WSlot {
    tx: Option<Sender<WorkerMsg>>,
    waker: Option<Arc<Waker>>,
    join: Option<JoinHandle<i32>>,
    /// Hilo lanzado y aún sin join.
    running: bool,
    started: u64,
    quick_crashes: u32,
    pending_respawn: bool,
}

/// Avisa al master cuando el hilo worker termina (también por pánico).
struct ExitGuard {
    id: usize,
    st: Arc<Stats>,
    tx: Sender<usize>,
    waker: Arc<Waker>,
}

impl Drop for ExitGuard {
    fn drop(&mut self) {
        let w = &self.st.w[self.id];
        w.alive.store(false, Relaxed);
        w.active_conns.store(0, Relaxed);
        let _ = self.tx.send(self.id);
        let _ = self.waker.wake();
    }
}

struct Master {
    poll: Poll,
    cfg_path: String,
    base_dir: String,
    text: String,
    compiled: Arc<Compiled>,
    nworkers: usize,
    w: Vec<WSlot>,
    stats: Arc<Stats>,
    stopping: bool,
    forced_exit: bool,
    stop: bool,
    stop_deadline: u64,
    debounce: Option<u64>,
    respawn: Option<u64>,
    stop_check: Option<u64>,
    watch_poll: Option<u64>,
    sig_rx: mio::unix::pipe::Receiver,
    watch: Option<Watch>,
    stats_l: Option<UnixListener>,
    stats_path: String,
    exit_tx: Sender<usize>,
    exit_rx: Receiver<usize>,
    waker: Arc<Waker>,
}

/// Sockets de escucha del master (solo sin SO_REUSEPORT con reparto):
/// reutiliza los de la generación anterior y abre los nuevos.
fn sync_listeners(prev: Option<&Compiled>, c: &mut Compiled) -> Result<(), String> {
    if cfg!(target_os = "linux") {
        return Ok(());
    }
    for f in &c.cfg.frontends {
        let l = match prev.and_then(|p| p.listeners.iter().find(|(k, _)| *k == f.key)) {
            Some((_, l)) => l.clone(),
            None => Arc::new(worker::listener_open(&f.addr, false).map_err(|e| format!("listen {}: {e}", f.key))?),
        };
        c.listeners.push((f.key.clone(), l));
    }
    Ok(())
}

impl Master {
    fn send(&self, i: usize, m: WorkerMsg) -> bool {
        let s = &self.w[i];
        match (&s.tx, &s.waker) {
            (Some(tx), Some(wk)) => tx.send(m).is_ok() && wk.wake().is_ok(),
            _ => false,
        }
    }

    fn spawn(&mut self, i: usize) -> io::Result<()> {
        let poll = Poll::new()?;
        let waker = Arc::new(Waker::new(poll.registry(), worker::WAKE)?);
        let (tx, rx) = channel();
        // La config va antes de lanzar el hilo: la encuentra al arrancar.
        let _ = tx.send(WorkerMsg::Config(self.compiled.clone()));
        let guard = ExitGuard { id: i, st: self.stats.clone(), tx: self.exit_tx.clone(), waker: self.waker.clone() };
        let st = self.stats.clone();
        // El hilo hereda la máscara: todas las señales bloqueadas, así solo
        // las recibe el hilo master.
        let old = block_all_signals();
        let r = std::thread::Builder::new().name(format!("proxy-w{i}")).spawn(move || {
            let _g = guard;
            worker::worker_main(i, poll, rx, st)
        });
        restore_signals(&old);
        let join = r?;
        let s = &mut self.w[i];
        s.tx = Some(tx);
        s.waker = Some(waker);
        s.join = Some(join);
        s.running = true;
        s.started = mono_ms();
        s.pending_respawn = false;
        Ok(())
    }

    fn reload(&mut self, forced: bool) {
        let text = match std::fs::read_to_string(&self.cfg_path) {
            Ok(t) => t,
            Err(e) => {
                loge!("recarga: no se puede leer {}: {e}", self.cfg_path);
                self.stats.reloads_failed.fetch_add(1, Relaxed);
                return;
            }
        };
        if !forced && text == self.text {
            return; // sin cambios reales
        }
        let built = config::parse(&text, &self.base_dir).and_then(Compiled::build).and_then(|mut c| {
            sync_listeners(Some(&self.compiled), &mut c)?;
            Ok(c)
        });
        let c = match built {
            Ok(c) => c,
            Err(e) => {
                loge!("recarga rechazada, se mantiene la configuración actual: {e}");
                self.stats.reloads_failed.fetch_add(1, Relaxed);
                return;
            }
        };
        if c.cfg.workers != self.nworkers {
            logw!("el cambio de 'workers' ({} -> {}) requiere reiniciar el proxy", self.nworkers, c.cfg.workers);
        }
        log::set_level(c.cfg.log_level);
        self.text = text;
        self.compiled = Arc::new(c);
        for i in 0..self.nworkers {
            if self.w[i].running && !self.send(i, WorkerMsg::Config(self.compiled.clone())) {
                loge!("no se pudo enviar la config al worker {i}");
            }
        }
        self.stats.reloads_ok.fetch_add(1, Relaxed);
        logi!("configuración recargada ({})", if forced { "SIGHUP" } else { "cambio en fichero" });
    }

    /// El hilo del worker i ha terminado: join y relanzar.
    fn worker_exited(&mut self, i: usize) {
        let s = &mut self.w[i];
        if !s.running {
            return;
        }
        s.running = false;
        s.tx = None;
        s.waker = None;
        let res = s.join.take().map(|j| j.join());
        if self.stopping {
            return;
        }
        match res {
            Some(Ok(code)) => loge!("worker {i} terminó con código {code}"),
            _ => loge!("worker {i} terminó por un pánico"),
        }
        let alive = mono_ms() - s.started;
        s.quick_crashes = if alive < 1000 { s.quick_crashes + 1 } else { 0 };
        if s.quick_crashes > 10 {
            loge!("worker {i} falla al arrancar repetidamente; se abandona");
            return;
        }
        s.pending_respawn = true;
        let delay = if s.quick_crashes > 0 { RESPAWN_DELAY_MS * s.quick_crashes as u64 } else { 50 };
        let at = mono_ms() + delay;
        self.respawn = Some(self.respawn.map_or(at, |r| r.min(at)));
    }

    fn respawn_fire(&mut self) {
        if self.stopping {
            return;
        }
        for i in 0..self.nworkers {
            if self.w[i].pending_respawn {
                match self.spawn(i) {
                    Ok(()) => {
                        self.stats.worker_restarts.fetch_add(1, Relaxed);
                        logi!("worker {i} relanzado");
                    }
                    Err(e) => {
                        loge!("no se puede relanzar el worker {i}: {e}");
                        self.respawn = Some(mono_ms() + RESPAWN_DELAY_MS);
                    }
                }
            }
        }
    }

    fn alive_workers(&self) -> usize {
        self.w.iter().filter(|s| s.running).count()
    }

    fn begin_shutdown(&mut self, sig: i32) {
        if self.stopping {
            return;
        }
        self.stopping = true;
        self.stop_deadline = mono_ms() + SHUTDOWN_GRACE_MS;
        logi!("señal {sig}: parando workers");
        for i in 0..self.nworkers {
            if self.w[i].running && !self.send(i, WorkerMsg::Stop) {
                loge!("no se pudo pedir la parada al worker {i}");
            }
        }
        self.stop_check = Some(mono_ms() + 20);
    }

    fn stop_check_fire(&mut self) {
        if self.alive_workers() == 0 {
            self.stop = true;
            return;
        }
        if mono_ms() >= self.stop_deadline {
            // Un hilo no se puede matar por separado: se sale del proceso
            // con los workers restantes aún vivos.
            logw!("{} workers no terminaron a tiempo; se fuerza la salida", self.alive_workers());
            self.forced_exit = true;
            self.stop = true;
            return;
        }
        self.stop_check = Some(mono_ms() + 50);
    }

    fn sig_event(&mut self) {
        let mut buf = [0u8; 64];
        loop {
            let n = match self.sig_rx.read(&mut buf) {
                Ok(0) | Err(_) => return,
                Ok(n) => n,
            };
            for &c in &buf[..n] {
                match c as i32 {
                    libc::SIGHUP => {
                        if !self.stopping {
                            self.reload(true);
                        }
                    }
                    libc::SIGTERM | libc::SIGINT | libc::SIGQUIT => self.begin_shutdown(c as i32),
                    libc::SIGUSR1 => {
                        if let Some(i) = (0..self.nworkers).find(|&i| self.w[i].running) {
                            logw!("SIGUSR1: provocando un pánico en el worker {i}");
                            self.send(i, WorkerMsg::Panic);
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    fn stats_event(&mut self) {
        let Some(l) = &self.stats_l else { return };
        while let Ok((mut s, _)) = l.accept() {
            let _ = s.set_nonblocking(false);
            let _ = s.set_write_timeout(Some(Duration::from_secs(1)));
            let _ = s.write_all(self.stats.json(log::dropped()).as_bytes());
        }
    }

    fn open_stats_socket(&mut self, path: &str) -> Option<UnixListener> {
        if path.is_empty() {
            return None;
        }
        let _ = std::fs::remove_file(path);
        match UnixListener::bind(path) {
            Ok(l) => {
                let _ = l.set_nonblocking(true);
                self.stats_path = path.to_string();
                Some(l)
            }
            Err(e) => {
                loge!("stats_socket {path}: {e}");
                None
            }
        }
    }

    fn run(&mut self) {
        let mut events = Events::with_capacity(64);
        while !self.stop {
            let now = mono_ms();
            let next = [self.debounce, self.respawn, self.stop_check, self.watch_poll].into_iter().flatten().min();
            let timeout = next.map(|t| Duration::from_millis(t.saturating_sub(now)));
            if let Err(e) = self.poll.poll(&mut events, timeout)
                && e.kind() != io::ErrorKind::Interrupted
            {
                loge!("poll del master: {e}");
                return;
            }
            for ev in events.iter() {
                match ev.token() {
                    T_SIG => self.sig_event(),
                    T_WAKE => {
                        while let Ok(i) = self.exit_rx.try_recv() {
                            self.worker_exited(i);
                        }
                    }
                    T_STATS => self.stats_event(),
                    T_WATCH if self.watch.as_mut().is_some_and(|w| w.consume()) => {
                        self.debounce = Some(mono_ms() + RELOAD_DEBOUNCE_MS);
                    }
                    _ => {}
                }
            }
            let now = mono_ms();
            if self.watch_poll.is_some_and(|t| t <= now) {
                self.watch_poll = Some(now + 500);
                if self.watch.as_mut().is_some_and(|w| w.consume()) {
                    self.debounce = Some(now + RELOAD_DEBOUNCE_MS);
                }
            }
            if self.debounce.is_some_and(|t| t <= now) {
                self.debounce = None;
                if !self.stopping {
                    self.reload(false);
                }
            }
            if self.respawn.is_some_and(|t| t <= now) {
                self.respawn = None;
                self.respawn_fire();
            }
            if self.stop_check.is_some_and(|t| t <= now) {
                self.stop_check = None;
                self.stop_check_fire();
            }
        }
    }
}

/// Arranca los workers y atiende señales/recargas hasta SIGTERM/SIGINT.
pub fn master_run(cfg_path: &str, cfg: Config, text: String) -> i32 {
    let base_dir = config::base_dir(cfg_path);
    if cfg!(target_os = "linux") {
        for f in &cfg.frontends {
            if let Err(e) = worker::listener_test_bind(&f.addr) {
                loge!("frontend '{}': bind {}: {e}", f.name, f.key);
                return 1;
            }
        }
    }
    let nworkers = cfg.workers;
    let stats_socket = cfg.stats_socket.clone();
    let compiled = match Compiled::build(cfg).and_then(|mut c| sync_listeners(None, &mut c).map(|_| c)) {
        Ok(c) => c,
        Err(e) => {
            loge!("{e}");
            return 1;
        }
    };
    let setup = || -> io::Result<(Poll, Arc<Waker>, mio::unix::pipe::Receiver, mio::unix::pipe::Sender)> {
        let poll = Poll::new()?;
        let waker = Arc::new(Waker::new(poll.registry(), T_WAKE)?);
        let (stx, srx) = mio::unix::pipe::new()?;
        Ok((poll, waker, srx, stx))
    };
    let (poll, waker, sig_rx, sig_tx) = match setup() {
        Ok(x) => x,
        Err(e) => {
            loge!("no se puede inicializar el master: {e}");
            return 1;
        }
    };
    let (exit_tx, exit_rx) = channel();
    let mut m = Master {
        poll,
        cfg_path: cfg_path.to_string(),
        base_dir,
        text,
        compiled: Arc::new(compiled),
        nworkers,
        w: (0..nworkers).map(|_| WSlot::default()).collect(),
        stats: Arc::new(Stats::new(nworkers)),
        stopping: false,
        forced_exit: false,
        stop: false,
        stop_deadline: 0,
        debounce: None,
        respawn: None,
        stop_check: None,
        watch_poll: None,
        sig_rx,
        watch: None,
        stats_l: None,
        stats_path: String::new(),
        exit_tx,
        exit_rx,
        waker,
    };
    install_signals(sig_tx.as_raw_fd());
    let reg = m.poll.registry();
    if let Err(e) = reg.register(&mut m.sig_rx, T_SIG, Interest::READABLE) {
        loge!("registro de señales: {e}");
        return 1;
    }
    m.watch = Watch::open(cfg_path);
    match m.watch.as_ref().map(|w| w.raw_fd()) {
        Some(Some(fd)) => {
            let _ = reg.register(&mut SourceFd(&fd), T_WATCH, Interest::READABLE);
        }
        Some(None) => m.watch_poll = Some(mono_ms() + 500),
        None => logw!("no se puede vigilar {cfg_path}: solo recarga por SIGHUP"),
    }
    m.stats_l = m.open_stats_socket(&stats_socket);
    if let Some(l) = &m.stats_l {
        let fd = l.as_raw_fd();
        let _ = m.poll.registry().register(&mut SourceFd(&fd), T_STATS, Interest::READABLE);
    }

    logi!("proxy pid {}, {} hilos worker, config {cfg_path}", std::process::id(), nworkers);
    for i in 0..nworkers {
        if let Err(e) = m.spawn(i) {
            loge!("no se puede lanzar el worker {i}: {e}");
            return 1;
        }
    }

    m.run();

    if !m.stats_path.is_empty() {
        let _ = std::fs::remove_file(&m.stats_path);
    }
    if m.forced_exit {
        logi!("master terminado (salida forzada)");
        return 0;
    }
    for s in &mut m.w {
        if let Some(j) = s.join.take() {
            let _ = j.join();
        }
    }
    drop(sig_tx);
    logi!("master terminado");
    0
}
