//! Slab con generaciones. El índice + generación forman el token de mio y
//! de los timers: un evento o timer de un objeto ya liberado (y cuyo hueco
//! se ha reutilizado) no casa con la generación nueva y se ignora.

pub struct Slab<T> {
    entries: Vec<(u32, Option<T>)>,
    free: Vec<usize>,
    len: usize,
}

impl<T> Default for Slab<T> {
    fn default() -> Self {
        Slab { entries: Vec::new(), free: Vec::new(), len: 0 }
    }
}

impl<T> Slab<T> {
    pub fn insert(&mut self, v: T) -> (usize, u32) {
        self.len += 1;
        if let Some(i) = self.free.pop() {
            let e = &mut self.entries[i];
            e.0 = e.0.wrapping_add(1) & 0x3fff_ffff;
            e.1 = Some(v);
            (i, e.0)
        } else {
            self.entries.push((1, Some(v)));
            (self.entries.len() - 1, 1)
        }
    }

    pub fn remove(&mut self, i: usize) -> Option<T> {
        let v = self.entries.get_mut(i)?.1.take();
        if v.is_some() {
            self.len -= 1;
            self.free.push(i);
        }
        v
    }

    pub fn get(&self, i: usize) -> Option<&T> {
        self.entries.get(i).and_then(|e| e.1.as_ref())
    }

    pub fn get_mut(&mut self, i: usize) -> Option<&mut T> {
        self.entries.get_mut(i).and_then(|e| e.1.as_mut())
    }

    /// Solo si la generación coincide.
    pub fn get_gen_mut(&mut self, i: usize, generation: u32) -> Option<&mut T> {
        match self.entries.get_mut(i) {
            Some((g, Some(v))) if *g == generation => Some(v),
            _ => None,
        }
    }

    pub fn generation(&self, i: usize) -> u32 {
        self.entries[i].0
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn keys(&self) -> Vec<usize> {
        self.entries.iter().enumerate().filter(|(_, e)| e.1.is_some()).map(|(i, _)| i).collect()
    }
}

impl<T> std::ops::Index<usize> for Slab<T> {
    type Output = T;
    fn index(&self, i: usize) -> &T {
        self.get(i).expect("slab: índice libre")
    }
}

impl<T> std::ops::IndexMut<usize> for Slab<T> {
    fn index_mut(&mut self, i: usize) -> &mut T {
        self.get_mut(i).expect("slab: índice libre")
    }
}

/// Token = clase (2 bits) | generación (30 bits) | índice (32 bits).
pub fn token(kind: u64, idx: usize, generation: u32) -> u64 {
    (kind << 62) | ((generation as u64) << 32) | idx as u64
}

pub fn untoken(t: u64) -> (u64, usize, u32) {
    (t >> 62, (t & 0xffff_ffff) as usize, ((t >> 32) & 0x3fff_ffff) as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generations() {
        let mut s = Slab::default();
        let (a, ga) = s.insert("a");
        s.remove(a);
        let (b, gb) = s.insert("b");
        assert_eq!(a, b, "reutiliza el hueco");
        assert_ne!(ga, gb);
        assert!(s.get_gen_mut(a, ga).is_none(), "token viejo no casa");
        assert_eq!(s.get_gen_mut(b, gb), Some(&mut "b"));
        assert_eq!(s.len(), 1);
        let t = token(3, b, gb);
        assert_eq!(untoken(t), (3, b, gb));
    }
}
