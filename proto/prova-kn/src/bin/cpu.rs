//! CPU per codificare (al lavaggio) e decodificare (alla lettura, solo quando tra le prime k fette c'è una fetta di
//! parità) con Reed-Solomon 4+1 e 8+2, su un core. Mediana di 7 giri.

use bytes::Bytes;
use prova_kn::rs;
use std::time::Instant;

fn mediana(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn main() {
    for &(k, m) in &[(4usize, 1usize), (8, 2)] {
        for &s in &[4096usize, 65536, 1 << 20, 16 << 20] {
            let dati: Vec<u8> = (0..s).map(|i| (i.wrapping_mul(2654435761) >> 7) as u8).collect();
            let giri_int = ((64usize << 20) / s).clamp(4, 20_000);
            let mut enc = Vec::new();
            let mut dec = Vec::new();
            let mut conc = Vec::new();
            let obj = rs::codifica(&dati, k, m).unwrap();
            let f = rs::fetta(s, k);
            let sl = |i: usize| (i, Bytes::copy_from_slice(&obj[i * f..(i + 1) * f]));
            // decodifica peggiore: manca la fetta di dati 0 (e la 1 per 8+2), al suo posto le parità
            let mancano = m;
            let arr_dec: Vec<(usize, Bytes)> = (mancano..k + m).map(sl).collect();
            let arr_dati: Vec<(usize, Bytes)> = (0..k).map(sl).collect();
            for _ in 0..7 {
                let t = Instant::now();
                for _ in 0..giri_int {
                    std::hint::black_box(rs::codifica(&dati, k, m).unwrap());
                }
                enc.push(t.elapsed().as_secs_f64() * 1e6 / giri_int as f64);
                let t = Instant::now();
                for _ in 0..giri_int {
                    std::hint::black_box(rs::ricostruisci(&arr_dec, k, m, s).unwrap());
                }
                dec.push(t.elapsed().as_secs_f64() * 1e6 / giri_int as f64);
                let t = Instant::now();
                for _ in 0..giri_int {
                    std::hint::black_box(rs::ricostruisci(&arr_dati, k, m, s).unwrap());
                }
                conc.push(t.elapsed().as_secs_f64() * 1e6 / giri_int as f64);
            }
            let (e, d, c) = (mediana(enc), mediana(dec), mediana(conc));
            println!(
                "{{\"codice\":\"{k}+{m}\",\"taglia\":{s},\"codifica_us\":{e:.1},\"codifica_mb_s\":{:.0},\"decodifica_us\":{d:.1},\"decodifica_mb_s\":{:.0},\"solo_dati_us\":{c:.1}}}",
                s as f64 / e, s as f64 / d
            );
        }
    }
}
