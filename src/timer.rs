//! Timers del event loop: min-heap con actualización perezosa.
//!
//! Cada objeto con timer guarda un `TimerState`. Retrasar un timer ya
//! armado es O(1) (solo cambia `deadline`; al vencer la clave del heap se
//! reprograma). Adelantarlo inserta una entrada nueva; las entradas viejas
//! quedan obsoletas y se descartan al salir del heap.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

#[derive(Default, Clone, Copy, Debug)]
pub struct TimerState {
    /// Deadline pedido (puede ser posterior a `key`).
    pub deadline: u64,
    /// Clave vigente dentro del heap; 0 = no armado.
    pub key: u64,
}

impl TimerState {
    pub fn armed(&self) -> bool {
        self.key != 0
    }
}

#[derive(Default)]
pub struct Timers {
    heap: BinaryHeap<Reverse<(u64, u64)>>,
}

/// Resultado de mirar la cima del heap.
pub enum Pop {
    /// Entrada vencida para `token` con clave `key`.
    Due(u64, u64),
    /// No hay nada vencido.
    Idle,
}

impl Timers {
    pub fn new() -> Timers {
        Timers::default()
    }

    /// Arma el timer de `token` para `deadline` (ms monótonos, > 0).
    pub fn set(&mut self, ts: &mut TimerState, deadline: u64, token: u64) {
        ts.deadline = deadline;
        if ts.key != 0 && deadline >= ts.key {
            return; // perezoso: se reprograma al vencer
        }
        ts.key = deadline;
        self.heap.push(Reverse((deadline, token)));
    }

    pub fn cancel(ts: &mut TimerState) {
        ts.key = 0;
    }

    /// Milisegundos hasta el próximo vencimiento (cota superior).
    pub fn next_in(&self, now: u64) -> Option<u64> {
        self.heap.peek().map(|Reverse((k, _))| k.saturating_sub(now))
    }

    /// Saca la siguiente entrada vencida (o obsoleta) a `now`.
    pub fn pop_due(&mut self, now: u64) -> Pop {
        match self.heap.peek() {
            Some(Reverse((k, _))) if *k <= now => {
                let Reverse((k, t)) = self.heap.pop().unwrap();
                Pop::Due(t, k)
            }
            _ => Pop::Idle,
        }
    }

    /// Decide qué hacer con una entrada vencida. true = disparar el timer.
    /// Si el deadline real es posterior, se reprograma.
    pub fn check_due(&mut self, ts: &mut TimerState, key: u64, token: u64, now: u64) -> bool {
        if ts.key != key {
            return false; // obsoleta (cancelado o adelantado)
        }
        if ts.deadline > now {
            ts.key = ts.deadline;
            self.heap.push(Reverse((ts.deadline, token)));
            return false;
        }
        ts.key = 0;
        true
    }

    pub fn len(&self) -> usize {
        self.heap.len()
    }

    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fire_all(t: &mut Timers, states: &mut [TimerState], now: u64) -> Vec<u64> {
        let mut fired = Vec::new();
        while let Pop::Due(tok, key) = t.pop_due(now) {
            if t.check_due(&mut states[tok as usize], key, tok, now) {
                fired.push(tok);
            }
        }
        fired
    }

    #[test]
    fn order_cancel_lazy() {
        let mut t = Timers::new();
        let mut s = [TimerState::default(); 4];
        t.set(&mut s[0], 300, 0);
        t.set(&mut s[1], 100, 1);
        t.set(&mut s[2], 200, 2);
        t.set(&mut s[3], 150, 3);
        Timers::cancel(&mut s[3]);
        assert_eq!(t.next_in(50), Some(50));
        assert_eq!(fire_all(&mut t, &mut s, 250), vec![1, 2]);
        // retrasar es perezoso: no dispara a 300 sino a 500
        t.set(&mut s[0], 500, 0);
        assert_eq!(fire_all(&mut t, &mut s, 300), Vec::<u64>::new());
        assert!(s[0].armed());
        assert_eq!(fire_all(&mut t, &mut s, 499), Vec::<u64>::new());
        assert_eq!(fire_all(&mut t, &mut s, 500), vec![0]);
        assert!(!s[0].armed());
        // adelantar reordena
        t.set(&mut s[1], 1000, 1);
        t.set(&mut s[1], 600, 1);
        assert_eq!(fire_all(&mut t, &mut s, 600), vec![1]);
        assert_eq!(fire_all(&mut t, &mut s, 2000), Vec::<u64>::new(), "la entrada vieja es obsoleta");
        assert!(t.is_empty());
    }

    #[test]
    fn rearm_after_fire() {
        let mut t = Timers::new();
        let mut s = [TimerState::default(); 1];
        t.set(&mut s[0], 10, 0);
        assert_eq!(fire_all(&mut t, &mut s, 10), vec![0]);
        t.set(&mut s[0], 20, 0);
        assert_eq!(fire_all(&mut t, &mut s, 20), vec![0]);
    }
}
