//! Logging asíncrono: los productores (master, hilos worker, hilos de
//! health) dejan cada línea en una cola MPSC acotada de LOG_QUEUE_SLOTS y un
//! único hilo consumidor la escribe al fichero. Nunca se bloquea al
//! productor: si la cola está llena el mensaje se descarta y se cuenta.

use std::cell::RefCell;
use std::fmt::{self, Write as _};
use std::fs::OpenOptions;
use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::JoinHandle;
use std::time::Duration;

pub const LOG_QUEUE_SLOTS: usize = 4096;
pub const LOG_LINE_MAX: usize = 512;

pub const DEBUG: i32 = 0;
pub const INFO: i32 = 1;
pub const WARN: i32 = 2;
pub const ERROR: i32 = 3;
pub const ACCESS: i32 = 4;

const LEVEL_NAME: [&str; 5] = ["debug", "info", "warn", "error", "access"];

static LEVEL: AtomicI32 = AtomicI32::new(INFO);
static DROPPED: AtomicU64 = AtomicU64::new(0);
static LOGGER: OnceLock<Logger> = OnceLock::new();

type Sink = Arc<Mutex<Box<dyn Write + Send>>>;

struct Logger {
    queue: LogQueue,
    sink: Sink,
    tag: String,
    stop: Arc<AtomicBool>,
    running: AtomicBool,
    thread: Mutex<Option<JoinHandle<()>>>,
}

thread_local! {
    /// Etiqueta del hilo actual ("w3"); vacía = la del proceso.
    static TAG: RefCell<String> = const { RefCell::new(String::new()) };
    /// Segundo y texto de la marca de tiempo cacheados por hilo.
    static TS: RefCell<(u64, String)> = const { RefCell::new((u64::MAX, String::new())) };
}

/// Cola MPSC acotada y no bloqueante para el productor.
pub struct LogQueue {
    tx: SyncSender<Vec<u8>>,
}

impl LogQueue {
    pub fn new(slots: usize) -> (LogQueue, Receiver<Vec<u8>>) {
        let (tx, rx) = sync_channel(slots);
        (LogQueue { tx }, rx)
    }

    /// false si la cola está llena (el mensaje se descarta).
    pub fn push(&self, line: Vec<u8>) -> bool {
        match self.tx.try_send(line) {
            Ok(()) => true,
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => false,
        }
    }
}

pub fn level_from_str(s: &str) -> Option<i32> {
    LEVEL_NAME[..=ERROR as usize].iter().position(|n| *n == s).map(|i| i as i32)
}

pub fn set_level(level: i32) {
    LEVEL.store(level, Ordering::Relaxed);
}

pub fn dropped() -> u64 {
    DROPPED.load(Ordering::Relaxed)
}

pub fn set_thread_tag(tag: &str) {
    TAG.with(|t| {
        let mut t = t.borrow_mut();
        t.clear();
        t.push_str(tag);
    });
}

/// Etiqueta efectiva del hilo actual.
pub fn thread_tag() -> String {
    let own = TAG.with(|t| t.borrow().clone());
    if !own.is_empty() {
        return own;
    }
    LOGGER.get().map(|l| l.tag.clone()).unwrap_or_default()
}

fn consumer(rx: Receiver<Vec<u8>>, sink: Sink, stop: Arc<AtomicBool>) {
    let mut batch: Vec<u8> = Vec::with_capacity(65536);
    loop {
        let first = rx.recv_timeout(Duration::from_millis(5));
        match first {
            Ok(line) => {
                batch.extend_from_slice(&line);
                while batch.len() < 60000 {
                    match rx.try_recv() {
                        Ok(l) => batch.extend_from_slice(&l),
                        Err(_) => break,
                    }
                }
                let mut s = sink.lock().unwrap_or_else(|e| e.into_inner());
                let _ = s.write_all(&batch);
                let _ = s.flush();
                batch.clear();
            }
            Err(RecvTimeoutError::Timeout) => {
                if stop.load(Ordering::Acquire) {
                    break;
                }
            }
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }
}

/// path vacío o "-" = stderr. tag es la etiqueta por defecto ("master").
pub fn init(path: &str, level: i32, tag: &str) -> Result<(), String> {
    set_level(level);
    let out: Box<dyn Write + Send> = if path.is_empty() || path == "-" {
        Box::new(io::stderr())
    } else {
        let f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| format!("no se puede abrir log '{path}': {e}"))?;
        Box::new(f)
    };
    let sink: Sink = Arc::new(Mutex::new(out));
    let (queue, rx) = LogQueue::new(LOG_QUEUE_SLOTS);
    let stop = Arc::new(AtomicBool::new(false));
    let (s2, st2) = (sink.clone(), stop.clone());
    let th = std::thread::Builder::new()
        .name("proxy-log".into())
        .spawn(move || consumer(rx, s2, st2))
        .map_err(|e| format!("no se puede crear el hilo de log: {e}"))?;
    let logger = Logger {
        queue,
        sink,
        tag: tag.to_string(),
        stop,
        running: AtomicBool::new(true),
        thread: Mutex::new(Some(th)),
    };
    LOGGER.set(logger).map_err(|_| "logger ya inicializado".to_string())
}

/// Vacía la cola y para el hilo consumidor. Lo que se registre después se
/// escribe directamente.
pub fn shutdown() {
    if let Some(l) = LOGGER.get() {
        l.running.store(false, Ordering::Release);
        l.stop.store(true, Ordering::Release);
        if let Some(t) = l.thread.lock().unwrap_or_else(|e| e.into_inner()).take() {
            let _ = t.join();
        }
    }
}

/// Días desde 1970-01-01 -> (año, mes, día) (algoritmo de H. Hinnant).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// "AAAA-MM-DDTHH:MM:SS" (UTC) de unos segundos epoch.
pub fn format_ts(secs: u64) -> String {
    let (y, mo, d) = civil_from_days((secs / 86_400) as i64);
    let s = secs % 86_400;
    format!("{y:04}-{mo:02}-{d:02}T{:02}:{:02}:{:02}", s / 3600, s / 60 % 60, s % 60)
}

pub fn log_msg(level: i32, args: fmt::Arguments<'_>) {
    if level < LEVEL.load(Ordering::Relaxed) {
        return;
    }
    let now = crate::util::wall_ms();
    let mut line = String::with_capacity(160);
    TS.with(|c| {
        let mut c = c.borrow_mut();
        if c.0 != now / 1000 {
            *c = (now / 1000, format_ts(now / 1000));
        }
        line.push_str(&c.1);
    });
    let tag = thread_tag();
    let _ = write!(line, ".{:03}Z [{}] [{}] ", now % 1000, LEVEL_NAME[level as usize], tag);
    let _ = line.write_fmt(args);
    if line.len() > LOG_LINE_MAX - 2 {
        let mut cut = LOG_LINE_MAX - 2;
        while !line.is_char_boundary(cut) {
            cut -= 1;
        }
        line.truncate(cut);
    }
    line.push('\n');
    match LOGGER.get() {
        Some(l) if l.running.load(Ordering::Acquire) => {
            if !l.queue.push(line.into_bytes()) {
                DROPPED.fetch_add(1, Ordering::Relaxed);
            }
        }
        Some(l) => {
            let mut s = l.sink.lock().unwrap_or_else(|e| e.into_inner());
            let _ = s.write_all(line.as_bytes());
        }
        None => {
            let _ = io::stderr().write_all(line.as_bytes());
        }
    }
}

#[macro_export]
macro_rules! logd { ($($a:tt)*) => { $crate::log::log_msg($crate::log::DEBUG, format_args!($($a)*)) } }
#[macro_export]
macro_rules! logi { ($($a:tt)*) => { $crate::log::log_msg($crate::log::INFO, format_args!($($a)*)) } }
#[macro_export]
macro_rules! logw { ($($a:tt)*) => { $crate::log::log_msg($crate::log::WARN, format_args!($($a)*)) } }
#[macro_export]
macro_rules! loge { ($($a:tt)*) => { $crate::log::log_msg($crate::log::ERROR, format_args!($($a)*)) } }
#[macro_export]
macro_rules! log_access { ($($a:tt)*) => { $crate::log::log_msg($crate::log::ACCESS, format_args!($($a)*)) } }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamps() {
        assert_eq!(format_ts(0), "1970-01-01T00:00:00");
        assert_eq!(format_ts(951_782_400), "2000-02-29T00:00:00");
        assert_eq!(format_ts(1_790_000_000), "2026-09-21T14:13:20");
    }

    #[test]
    fn levels() {
        assert_eq!(level_from_str("debug"), Some(DEBUG));
        assert_eq!(level_from_str("error"), Some(ERROR));
        assert_eq!(level_from_str("access"), None);
        assert_eq!(level_from_str("loud"), None);
    }

    #[test]
    fn queue_full_drops() {
        let (q, rx) = LogQueue::new(4);
        for i in 0..4 {
            assert!(q.push(vec![i]));
        }
        assert!(!q.push(vec![9]), "lleno: se descarta");
        assert_eq!(rx.try_recv().unwrap(), vec![0]);
        assert!(q.push(vec![4]));
        let got: Vec<u8> = rx.try_iter().map(|v| v[0]).collect();
        assert_eq!(got, vec![1, 2, 3, 4]);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn queue_concurrent_producers() {
        let (q, rx) = LogQueue::new(1024);
        let q = Arc::new(q);
        let mut hs = Vec::new();
        for p in 0..4u8 {
            let q = q.clone();
            hs.push(std::thread::spawn(move || {
                let mut ok = 0;
                for i in 0..10_000u32 {
                    let mut v = vec![p];
                    v.extend_from_slice(&i.to_le_bytes());
                    while !q.push(v.clone()) {
                        std::thread::yield_now();
                    }
                    ok += 1;
                }
                ok
            }));
        }
        let mut last = [None::<u32>; 4];
        let mut n = 0;
        while n < 40_000 {
            if let Ok(v) = rx.recv_timeout(Duration::from_secs(5)) {
                let p = v[0] as usize;
                let i = u32::from_le_bytes([v[1], v[2], v[3], v[4]]);
                // FIFO por productor
                assert!(last[p].is_none_or(|l| i == l + 1));
                last[p] = Some(i);
                n += 1;
            } else {
                panic!("timeout");
            }
        }
        for h in hs {
            assert_eq!(h.join().unwrap(), 10_000);
        }
    }

    #[test]
    fn thread_tags() {
        set_thread_tag("w7");
        assert_eq!(thread_tag(), "w7");
        std::thread::spawn(|| assert_ne!(thread_tag(), "w7")).join().unwrap();
    }
}
