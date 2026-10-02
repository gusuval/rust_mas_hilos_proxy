//! Pool de slots de BUF_SIZE bytes con freelist (uno por hilo worker, sin
//! locks) y buffer lineal sobre un slot.

pub const BUF_SIZE: usize = 16384;

pub type Slot = Box<[u8; BUF_SIZE]>;

#[derive(Default)]
pub struct BufPool {
    free: Vec<Slot>,
    allocated: usize,
    in_use: usize,
}

impl BufPool {
    pub fn new() -> BufPool {
        BufPool::default()
    }

    pub fn get(&mut self) -> Slot {
        self.in_use += 1;
        self.free.pop().unwrap_or_else(|| {
            self.allocated += 1;
            vec![0u8; BUF_SIZE].into_boxed_slice().try_into().expect("tamaño de slot")
        })
    }

    pub fn put(&mut self, s: Slot) {
        self.in_use -= 1;
        self.free.push(s);
    }

    pub fn in_use(&self) -> usize {
        self.in_use
    }

    pub fn allocated(&self) -> usize {
        self.allocated
    }
}

/// Buffer lineal sobre un slot: datos válidos en [r, w).
#[derive(Default)]
pub struct Buf {
    pub data: Option<Slot>,
    pub r: usize,
    pub w: usize,
}

impl Buf {
    pub fn has(&self) -> bool {
        self.data.is_some()
    }

    pub fn len(&self) -> usize {
        self.w - self.r
    }

    pub fn is_empty(&self) -> bool {
        self.w == self.r
    }

    pub fn space(&self) -> usize {
        if self.data.is_some() { BUF_SIZE - self.w } else { 0 }
    }

    pub fn rslice(&self) -> &[u8] {
        match &self.data {
            Some(d) => &d[self.r..self.w],
            None => &[],
        }
    }

    pub fn wslice(&mut self) -> &mut [u8] {
        match &mut self.data {
            Some(d) => &mut d[self.w..],
            None => &mut [],
        }
    }

    pub fn consume(&mut self, n: usize) {
        self.r += n;
        if self.r == self.w {
            self.r = 0;
            self.w = 0;
        }
    }

    /// Mueve los datos al inicio para liberar espacio al final.
    pub fn compact(&mut self) {
        if self.r == 0 {
            return;
        }
        let n = self.len();
        if let Some(d) = &mut self.data {
            d.copy_within(self.r..self.w, 0);
        }
        self.r = 0;
        self.w = n;
    }

    pub fn ensure(&mut self, p: &mut BufPool) {
        if self.data.is_none() {
            self.data = Some(p.get());
            self.r = 0;
            self.w = 0;
        }
    }

    pub fn release(&mut self, p: &mut BufPool) {
        if let Some(d) = self.data.take() {
            p.put(d);
        }
        self.r = 0;
        self.w = 0;
    }

    /// Añade bytes al final (compactando si hace falta). Devuelve cuántos caben.
    pub fn put(&mut self, src: &[u8]) -> usize {
        if self.space() < src.len() {
            self.compact();
        }
        let n = src.len().min(self.space());
        if n > 0 {
            let w = self.w;
            self.data.as_mut().unwrap()[w..w + n].copy_from_slice(&src[..n]);
            self.w += n;
        }
        n
    }
}

/// Copia de src a dst todo lo que quepa. Devuelve bytes movidos.
pub fn buf_move(dst: &mut Buf, src: &mut Buf) -> usize {
    if dst.space() == 0 {
        dst.compact();
    }
    let n = src.len().min(dst.space());
    if n > 0 {
        let (r, w) = (src.r, dst.w);
        dst.data.as_mut().unwrap()[w..w + n].copy_from_slice(&src.data.as_ref().unwrap()[r..r + n]);
        dst.w += n;
        src.consume(n);
    }
    n
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_reuses_slots() {
        let mut p = BufPool::new();
        let a = p.get();
        let b = p.get();
        assert_eq!(p.in_use(), 2);
        assert_eq!(p.allocated(), 2);
        p.put(a);
        let c = p.get();
        assert_eq!(p.allocated(), 2, "reutiliza el slot liberado");
        p.put(b);
        p.put(c);
        assert_eq!(p.in_use(), 0);
    }

    #[test]
    fn buf_ops() {
        let mut p = BufPool::new();
        let mut b = Buf::default();
        assert_eq!(b.space(), 0);
        b.ensure(&mut p);
        assert_eq!(b.space(), BUF_SIZE);
        assert_eq!(b.put(b"hello world"), 11);
        b.consume(6);
        assert_eq!(b.rslice(), b"world");
        b.compact();
        assert_eq!((b.r, b.w), (0, 5));
        b.consume(5);
        assert_eq!((b.r, b.w), (0, 0), "vacío: vuelve al inicio");

        let mut d = Buf::default();
        d.ensure(&mut p);
        b.put(&vec![7u8; BUF_SIZE]);
        d.put(b"xyz");
        assert_eq!(buf_move(&mut d, &mut b), BUF_SIZE - 3);
        assert_eq!(b.len(), 3);
        assert_eq!(d.space(), 0);
        b.release(&mut p);
        d.release(&mut p);
        assert_eq!(p.in_use(), 0);
    }
}
