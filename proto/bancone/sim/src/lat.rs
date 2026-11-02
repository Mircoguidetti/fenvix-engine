//! Latenze: scenari dichiarati in SOGLIE.md, campionati dopo la simulazione del bancone.

pub struct Rng(u64);
impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(seed.wrapping_mul(0x9E3779B97F4A7C15) | 1)
    }
    pub fn u(&mut self) -> f64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        ((self.0.wrapping_mul(0x2545F4914F6CDD1D) >> 11) as f64 + 0.5) / (1u64 << 53) as f64
    }
    pub fn normal(&mut self) -> f64 {
        let (a, b) = (self.u(), self.u());
        (-2.0 * a.ln()).sqrt() * (2.0 * std::f64::consts::PI * b).cos()
    }
}

/// Una GET: primo byte lognormale (mediana, p99), più i byte a 85 MB/s, più un evento raro.
#[derive(Clone, Copy)]
pub struct Tier {
    pub med_ms: f64,
    pub p99_ms: f64,
    pub rare_p: f64,
    pub rare_ms: f64,
    pub mbps: f64,
}
impl Tier {
    fn sigma(&self) -> f64 {
        (self.p99_ms / self.med_ms).ln() / 2.3263
    }
    pub fn sample(&self, r: &mut Rng, bytes: u64) -> f64 {
        let mut x = self.med_ms * (self.sigma() * r.normal()).exp() + bytes as f64 / (self.mbps * 1e3);
        if self.rare_p > 0.0 && r.u() < self.rare_p {
            x += self.rare_ms;
        }
        x
    }
    /// p95 dell'attesa del primo byte (la soglia della richiesta doppia), più il trasferimento.
    pub fn p95(&self, bytes: u64) -> f64 {
        self.med_ms * (self.sigma() * 1.6449).exp() + bytes as f64 / (self.mbps * 1e3)
    }
}

pub struct Scenario {
    pub name: &'static str,
    pub s3: Tier,
    pub ex: Tier,
}

pub fn scenarios() -> Vec<Scenario> {
    let t = |m, p, rp, rm| Tier { med_ms: m, p99_ms: p, rare_p: rp, rare_ms: rm, mbps: 85.0 };
    vec![
        Scenario { name: "ottimista", s3: t(20.0, 50.0, 0.0, 0.0), ex: t(2.0, 5.0, 0.0, 0.0) },
        Scenario { name: "medio", s3: t(25.0, 100.0, 0.0, 0.0), ex: t(4.0, 10.0, 0.0, 0.0) },
        Scenario { name: "pessimista", s3: t(30.0, 200.0, 0.001, 1000.0), ex: t(6.0, 25.0, 0.001, 100.0) },
    ]
}

/// Bancone su EBS gp3 (variante): mediana 0,4 ms, p99 1,5 ms (STIMATO).
pub fn gp3() -> Tier {
    Tier { med_ms: 0.4, p99_ms: 1.5, rare_p: 0.0, rare_ms: 0.0, mbps: 1e9 }
}

/// Istogramma: passi da 5 µs fino a 100 ms, poi da 1 ms fino a 10 s.
pub struct Hist {
    b: Vec<u64>,
    pub n: u64,
}
impl Hist {
    pub fn new() -> Self {
        Hist { b: vec![0; 20_000 + 9_900 + 1], n: 0 }
    }
    fn idx(ms: f64) -> usize {
        if ms < 100.0 {
            (ms * 200.0) as usize
        } else {
            (20_000 + (ms - 100.0) as usize).min(29_900)
        }
    }
    fn val(i: usize) -> f64 {
        if i < 20_000 {
            (i as f64 + 0.5) / 200.0
        } else {
            100.0 + (i - 20_000) as f64 + 0.5
        }
    }
    pub fn add(&mut self, ms: f64, k: u64) {
        self.b[Self::idx(ms)] += k;
        self.n += k;
    }
    pub fn q(&self, p: f64) -> f64 {
        if self.n == 0 {
            return f64::NAN;
        }
        let target = (p * self.n as f64).ceil().max(1.0) as u64;
        let mut c = 0;
        for (i, &x) in self.b.iter().enumerate() {
            c += x;
            if c >= target {
                return Self::val(i);
            }
        }
        f64::NAN
    }
}
