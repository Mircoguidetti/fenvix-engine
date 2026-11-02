//! Simulazione delle stesse strategie con lo stesso modello del proxy, senza rete: 1 milione di letture per caso, per
//! avere un p99,9 stabile e per controllare che il banco vero misuri quello che il modello dice.
//! Non conta la CPU né il tempo di rete in locale: solo il tempo delle GET.

use prova_kn::modello::Modello;
use prova_kn::rs;
use rand::rngs::SmallRng;
use rand::SeedableRng;

fn quantili(mut v: Vec<f64>) -> (f64, f64, f64, f64) {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let q = |x: f64| v[((v.len() as f64 * x) as usize).min(v.len() - 1)];
    (q(0.5), q(0.95), q(0.99), q(0.999))
}

fn main() {
    let n: usize = std::env::args().nth(1).map(|s| s.parse().unwrap()).unwrap_or(1_000_000);
    let taglie = [4096usize, 65536, 1 << 20, 16 << 20];
    for m in [Modello::BASE, Modello::LEGGERA, Modello::PESANTE, Modello::UNO_SU_CENTO] {
        for &s in &taglie {
            let mut r = SmallRng::seed_from_u64(42);
            let mut g = |b: usize| m.campiona(b as u64, &mut r);
            // normale
            let norm: Vec<f64> = (0..n).map(|_| g(s)).collect();
            let (p50n, p95n, p99n, p999n) = quantili(norm);
            let riga = |nome: &str, (p50, _p95, p99, p999): (f64, f64, f64, f64), get: f64, byte: f64| {
                println!(
                    "{{\"modello\":\"{}\",\"taglia\":{},\"strategia\":\"{}\",\"p50_ms\":{:.2},\"p99_ms\":{:.2},\"p999_ms\":{:.2},\"get_per_lettura\":{:.3},\"byte_per_lettura\":{:.0},\"p99_vs_normale\":{:.3}}}",
                    m.nome, s, nome, p50, p99, p999, get, byte, p99n / p99
                );
            };
            riga("normale", (p50n, p95n, p99n, p999n), 1.0, s as f64);
            // pezzi4: 4 range in parallelo, serve il più lento
            let f = s.div_ceil(4);
            let v: Vec<f64> = (0..n).map(|_| (0..4).map(|_| g(f)).fold(0.0, f64::max)).collect();
            riga("pezzi4", quantili(v), 4.0, s as f64);
            // doppia a soglia: p50, p90, p95 della normale
            for (nome, t) in [("doppia-p95", p95n), ("doppia-p50", p50n)] {
                let mut get = 0.0;
                let mut byte = 0.0;
                let v: Vec<f64> = (0..n)
                    .map(|_| {
                        let a = g(s);
                        get += 1.0;
                        byte += s as f64;
                        if a <= t {
                            a
                        } else {
                            get += 1.0;
                            byte += s as f64; // al massimo: la seconda può essere annullata prima della fine
                            a.min(t + g(s))
                        }
                    })
                    .collect();
                riga(&format!("{nome}({t:.0}ms)"), quantili(v), get / n as f64, byte / n as f64);
            }
            for c in [2usize, 5, 10] {
                let v: Vec<f64> = (0..n).map(|_| (0..c).map(|_| g(s)).fold(f64::MAX, f64::min)).collect();
                riga(&format!("copie:{c}"), quantili(v), c as f64, (c * s) as f64);
            }
            for (k, mm) in [(4usize, 1usize), (8, 2)] {
                let f = rs::fetta(s, k);
                let v: Vec<f64> = (0..n)
                    .map(|_| {
                        let mut t: Vec<f64> = (0..k + mm).map(|_| g(f)).collect();
                        t.sort_by(|a, b| a.partial_cmp(b).unwrap());
                        t[k - 1]
                    })
                    .collect();
                riga(&format!("kn:{k}+{mm}"), quantili(v), (k + mm) as f64, (f * (k + mm)) as f64);
            }
        }
    }
}
