//! Il bancone: due politiche con capacità in byte, LRU e S3-FIFO.
//! S3-FIFO segue i parametri di serie di libCacheSim (S3FIFO.c): coda piccola 10%, fantasmi 90% dei byte,
//! passaggio alla coda grande con freq >= 2, coda grande con reinserimento e contatore a 2 bit.
//! Le due politiche si confrontano con libCacheSim nel passo di verifica (stesso flusso, stessa taglia).

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};

#[derive(Default, Clone, Copy)]
pub struct Fx(u64);
impl Hasher for Fx {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        for b in bytes {
            self.write_u64(*b as u64);
        }
    }
    fn write_u64(&mut self, x: u64) {
        self.0 = (self.0.rotate_left(5) ^ x).wrapping_mul(0x51_7c_c1_b7_27_22_0a_95);
    }
}
pub type FxMap<K, V> = HashMap<K, V, BuildHasherDefault<Fx>>;
pub fn fxmap<K, V>() -> FxMap<K, V> {
    HashMap::with_hasher(BuildHasherDefault::<Fx>::default())
}

const NIL: u32 = u32::MAX;

#[derive(Clone, Copy)]
struct Node {
    id: u64,
    size: u32,
    prev: u32,
    next: u32,
    freq: u8,
    q: u8, // 0 piccola (o LRU), 1 grande
}

/// Liste doppie dentro un vettore, con lista dei liberi.
struct Slab {
    n: Vec<Node>,
    free: Vec<u32>,
}
impl Slab {
    fn new() -> Self {
        Slab { n: Vec::new(), free: Vec::new() }
    }
    fn alloc(&mut self, id: u64, size: u32, q: u8) -> u32 {
        let node = Node { id, size, prev: NIL, next: NIL, freq: 0, q };
        if let Some(i) = self.free.pop() {
            self.n[i as usize] = node;
            i
        } else {
            self.n.push(node);
            (self.n.len() - 1) as u32
        }
    }
}

/// Testa = più recente; coda = la prossima a uscire.
#[derive(Clone, Copy)]
struct List {
    head: u32,
    tail: u32,
    bytes: u64,
}
impl List {
    fn new() -> Self {
        List { head: NIL, tail: NIL, bytes: 0 }
    }
    fn push_head(&mut self, s: &mut Slab, i: u32) {
        let sz = s.n[i as usize].size as u64;
        s.n[i as usize].prev = NIL;
        s.n[i as usize].next = self.head;
        if self.head != NIL {
            s.n[self.head as usize].prev = i;
        } else {
            self.tail = i;
        }
        self.head = i;
        self.bytes += sz;
    }
    fn unlink(&mut self, s: &mut Slab, i: u32) {
        let Node { prev, next, size, .. } = s.n[i as usize];
        if prev != NIL {
            s.n[prev as usize].next = next;
        } else {
            self.head = next;
        }
        if next != NIL {
            s.n[next as usize].prev = prev;
        } else {
            self.tail = prev;
        }
        self.bytes -= size as u64;
    }
}

pub trait Policy {
    /// Accesso con promozione; se manca, inserisce. Ritorna true se c'era.
    fn get(&mut self, id: u64, size: u32, ev: &mut Vec<u64>) -> bool;
    /// Solo guardare, senza toccare lo stato.
    fn contains(&self, id: u64) -> bool;
    fn remove(&mut self, id: u64);
    fn used(&self) -> u64;
}

pub struct Lru {
    cap: u64,
    s: Slab,
    l: List,
    m: FxMap<u64, u32>,
}
impl Lru {
    pub fn new(cap: u64) -> Self {
        Lru { cap, s: Slab::new(), l: List::new(), m: fxmap() }
    }
}
impl Policy for Lru {
    fn get(&mut self, id: u64, size: u32, ev: &mut Vec<u64>) -> bool {
        if let Some(&i) = self.m.get(&id) {
            self.l.unlink(&mut self.s, i);
            self.l.push_head(&mut self.s, i);
            return true;
        }
        if size as u64 > self.cap {
            return false;
        }
        while self.l.bytes + size as u64 > self.cap && self.l.tail != NIL {
            let t = self.l.tail;
            self.l.unlink(&mut self.s, t);
            let tid = self.s.n[t as usize].id;
            self.m.remove(&tid);
            self.s.free.push(t);
            ev.push(tid);
        }
        let i = self.s.alloc(id, size, 0);
        self.l.push_head(&mut self.s, i);
        self.m.insert(id, i);
        false
    }
    fn contains(&self, id: u64) -> bool {
        self.m.contains_key(&id)
    }
    fn remove(&mut self, id: u64) {
        if let Some(i) = self.m.remove(&id) {
            self.l.unlink(&mut self.s, i);
            self.s.free.push(i);
        }
    }
    fn used(&self) -> u64 {
        self.l.bytes
    }
}

pub struct S3Fifo {
    cap: u64,
    cap_s: u64,
    cap_g: u64,
    s: Slab,
    small: List,
    main: List,
    m: FxMap<u64, u32>,
    gs: Slab,
    ghost: List,
    gm: FxMap<u64, u32>,
}
impl S3Fifo {
    pub fn new(cap: u64) -> Self {
        S3Fifo {
            cap,
            cap_s: (cap as f64 * 0.10) as u64,
            cap_g: (cap as f64 * 0.90) as u64,
            s: Slab::new(),
            small: List::new(),
            main: List::new(),
            m: fxmap(),
            gs: Slab::new(),
            ghost: List::new(),
            gm: fxmap(),
        }
    }
    fn ghost_add(&mut self, id: u64, size: u32) {
        if self.cap_g == 0 {
            return;
        }
        if let Some(i) = self.gm.remove(&id) {
            self.ghost.unlink(&mut self.gs, i);
            self.gs.free.push(i);
        }
        while self.ghost.bytes + size as u64 > self.cap_g && self.ghost.tail != NIL {
            let t = self.ghost.tail;
            self.ghost.unlink(&mut self.gs, t);
            let tid = self.gs.n[t as usize].id;
            self.gm.remove(&tid);
            self.gs.free.push(t);
        }
        let i = self.gs.alloc(id, size, 2);
        self.ghost.push_head(&mut self.gs, i);
        self.gm.insert(id, i);
    }
    fn evict_small(&mut self, ev: &mut Vec<u64>) {
        while self.small.tail != NIL {
            let t = self.small.tail;
            self.small.unlink(&mut self.s, t);
            let nd = self.s.n[t as usize];
            if nd.freq >= 2 {
                self.s.n[t as usize].freq = 0;
                self.s.n[t as usize].q = 1;
                self.main.push_head(&mut self.s, t);
            } else {
                self.m.remove(&nd.id);
                self.s.free.push(t);
                self.ghost_add(nd.id, nd.size);
                ev.push(nd.id);
                return;
            }
        }
    }
    fn evict_main(&mut self, ev: &mut Vec<u64>) {
        while self.main.tail != NIL {
            let t = self.main.tail;
            self.main.unlink(&mut self.s, t);
            let f = self.s.n[t as usize].freq;
            if f >= 1 {
                self.s.n[t as usize].freq = f.min(3) - 1;
                self.main.push_head(&mut self.s, t);
            } else {
                let id = self.s.n[t as usize].id;
                self.m.remove(&id);
                self.s.free.push(t);
                ev.push(id);
                return;
            }
        }
    }
}
impl Policy for S3Fifo {
    fn get(&mut self, id: u64, size: u32, ev: &mut Vec<u64>) -> bool {
        if let Some(&i) = self.m.get(&id) {
            let f = &mut self.s.n[i as usize].freq;
            *f = f.saturating_add(1);
            return true;
        }
        let from_ghost = if let Some(g) = self.gm.remove(&id) {
            self.ghost.unlink(&mut self.gs, g);
            self.gs.free.push(g);
            true
        } else {
            false
        };
        if size as u64 > self.cap || (!from_ghost && size as u64 >= self.cap_s) {
            return false;
        }
        while self.small.bytes + self.main.bytes + size as u64 > self.cap {
            // come S3FIFO_evict di libCacheSim: dalla grande solo se sfora la sua quota o la piccola è vuota
            if self.main.bytes > self.cap - self.cap_s || self.small.bytes == 0 {
                if self.main.bytes == 0 {
                    break;
                }
                self.evict_main(ev);
            } else {
                self.evict_small(ev);
            }
        }
        let q = if from_ghost { 1 } else { 0 };
        let i = self.s.alloc(id, size, q);
        if from_ghost {
            self.main.push_head(&mut self.s, i);
        } else {
            self.small.push_head(&mut self.s, i);
        }
        self.m.insert(id, i);
        false
    }
    fn contains(&self, id: u64) -> bool {
        self.m.contains_key(&id)
    }
    fn remove(&mut self, id: u64) {
        if let Some(i) = self.m.remove(&id) {
            if self.s.n[i as usize].q == 0 {
                self.small.unlink(&mut self.s, i);
            } else {
                self.main.unlink(&mut self.s, i);
            }
            self.s.free.push(i);
        }
    }
    fn used(&self) -> u64 {
        self.small.bytes + self.main.bytes
    }
}

pub fn make(policy: &str, cap: u64) -> Box<dyn Policy> {
    match policy {
        "lru" => Box::new(Lru::new(cap)),
        "s3fifo" => Box::new(S3Fifo::new(cap)),
        p => panic!("politica sconosciuta {p}"),
    }
}

#[cfg(test)]
mod t {
    use super::*;
    #[test]
    fn lru_base() {
        let mut c = Lru::new(3);
        let mut ev = vec![];
        assert!(!c.get(1, 1, &mut ev));
        assert!(!c.get(2, 1, &mut ev));
        assert!(!c.get(3, 1, &mut ev));
        assert!(c.get(1, 1, &mut ev));
        assert!(!c.get(4, 1, &mut ev));
        assert_eq!(ev, vec![2]);
        assert!(c.contains(1) && c.contains(3) && c.contains(4));
        assert!(c.used() <= 3);
    }
    #[test]
    fn s3fifo_scan_resistant() {
        // un oggetto caldo resiste a una scansione lunga
        let mut c = S3Fifo::new(100);
        let mut ev = vec![];
        for _ in 0..3 {
            c.get(7, 1, &mut ev);
        }
        for k in 1000..1200 {
            c.get(k, 1, &mut ev);
            c.get(7, 1, &mut ev);
        }
        assert!(c.contains(7));
        assert!(c.used() <= 100);
        let mut l = Lru::new(100);
        for k in 0..10_000u64 {
            l.get(k % 150, 1, &mut ev);
        }
        assert!(l.used() <= 100);
    }
}
