//! Tabla de rutas de un frontend: dominio exacto (hash djb2, direccionamiento
//! abierto) -> wildcard "*.dom" (gana el sufijo más largo) -> "default".
//! Las búsquedas no distinguen mayúsculas. Inmutable una vez construida:
//! se comparte entre hilos.

pub fn djb2(s: &[u8]) -> u64 {
    s.iter().fold(5381u64, |h, &c| (h << 5).wrapping_add(h).wrapping_add(c.to_ascii_lowercase() as u64))
}

#[derive(Clone)]
struct Exact {
    host: Box<[u8]>,
    hash: u64,
    target: usize,
}

#[derive(Default)]
pub struct Router {
    /// Tamaño potencia de 2, carga <= 1/2.
    tab: Vec<Option<Exact>>,
    n: usize,
    /// (".example.com", destino), ordenados por longitud descendente.
    wild: Vec<(Box<[u8]>, usize)>,
    def: Option<usize>,
}

impl Router {
    pub fn new() -> Router {
        Router { tab: vec![None; 16], ..Default::default() }
    }

    fn insert(tab: &mut [Option<Exact>], e: Exact) {
        let mask = tab.len() - 1;
        let mut i = e.hash as usize & mask;
        while tab[i].is_some() {
            i = (i + 1) & mask;
        }
        tab[i] = Some(e);
    }

    /// pattern: "host", "*.dominio" o "default".
    pub fn add(&mut self, pattern: &str, target: usize) {
        let p = pattern.to_ascii_lowercase();
        if p == "default" {
            self.def = Some(target);
        } else if let Some(suffix) = p.strip_prefix('*') {
            self.wild.push((suffix.as_bytes().into(), target));
            // sufijo más largo primero (orden estable)
            self.wild.sort_by_key(|w| std::cmp::Reverse(w.0.len()));
        } else {
            if (self.n + 1) * 2 > self.tab.len() {
                let mut nt = vec![None; self.tab.len() * 2];
                for e in self.tab.drain(..).flatten() {
                    Self::insert(&mut nt, e);
                }
                self.tab = nt;
            }
            let host: Box<[u8]> = p.as_bytes().into();
            let hash = djb2(&host);
            Self::insert(&mut self.tab, Exact { host, hash, target });
            self.n += 1;
        }
    }

    /// host sin puerto.
    pub fn lookup(&self, host: &[u8]) -> Option<usize> {
        if !host.is_empty() {
            let h = djb2(host);
            let mask = self.tab.len() - 1;
            let mut i = h as usize & mask;
            while let Some(e) = &self.tab[i] {
                if e.hash == h && e.host.eq_ignore_ascii_case(host) {
                    return Some(e.target);
                }
                i = (i + 1) & mask;
            }
            // "*.example.com" casa "a.example.com" (y "a.b.example.com"),
            // no "example.com".
            for (suf, t) in &self.wild {
                if host.len() > suf.len() && host[host.len() - suf.len()..].eq_ignore_ascii_case(suf) {
                    return Some(*t);
                }
            }
        }
        self.def
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_wildcard_default() {
        let mut r = Router::new();
        r.add("api.test", 1);
        r.add("*.example.com", 2);
        r.add("*.eu.example.com", 3);
        r.add("www.example.com", 4);
        let l = |r: &Router, h: &str| r.lookup(h.as_bytes());
        assert_eq!(l(&r, "api.test"), Some(1));
        assert_eq!(l(&r, "API.Test"), Some(1));
        assert_eq!(l(&r, "a.example.com"), Some(2));
        assert_eq!(l(&r, "a.b.example.com"), Some(2));
        assert_eq!(l(&r, "x.eu.example.com"), Some(3), "sufijo más largo gana");
        assert_eq!(l(&r, "www.example.com"), Some(4), "exacto antes que wildcard");
        assert_eq!(l(&r, "example.com"), None, "el wildcard no cubre el ápex");
        assert_eq!(l(&r, "other.test"), None);
        assert_eq!(l(&r, ""), None);
        r.add("default", 9);
        assert_eq!(l(&r, "other.test"), Some(9));
        assert_eq!(l(&r, "example.com"), Some(9));
        assert_eq!(l(&r, ""), Some(9));
    }

    #[test]
    fn many_hosts() {
        let mut r = Router::new();
        for i in 0..2000 {
            r.add(&format!("host{i}.test"), i);
        }
        for i in 0..2000 {
            assert_eq!(r.lookup(format!("HOST{i}.test").as_bytes()), Some(i));
        }
        assert_eq!(r.lookup(b"host2000.test"), None);
    }
}
