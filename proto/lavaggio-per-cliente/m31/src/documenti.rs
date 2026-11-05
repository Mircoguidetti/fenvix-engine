//! Documenti realistici che si comprimono (M29): testo di chat da un vocabolario con frequenze zipf, eventi con campi
//! ripetuti e numeri, ordini con righe e indirizzi, più identificativi casuali (che non si comprimono). Tutto scritto
//! come JSON compatto, come lo vede chi guarda un documento MongoDB. Deterministico dato il seme.

use rand::rngs::SmallRng;
use rand::Rng;
use std::fmt::Write as _;
use std::sync::OnceLock;

struct Vocabolario {
    parole: Vec<String>,
    cdf: Vec<f64>,
    citta: Vec<String>,
}

fn voc() -> &'static Vocabolario {
    static V: OnceLock<Vocabolario> = OnceLock::new();
    V.get_or_init(|| {
        use rand::SeedableRng;
        let mut r = SmallRng::seed_from_u64(2026);
        let sill = ["ka", "lo", "mi", "ne", "ra", "to", "su", "ve", "di", "an", "er", "is", "on", "ul", "pre", "con", "tra", "ste", "gri", "bla", "the", "ing", "ment", "ar", "or", "el", "us", "io", "qua", "fo"];
        let mut parole = Vec::new();
        let mut viste = std::collections::HashSet::new();
        while parole.len() < 4000 {
            let n = r.gen_range(1..=4);
            let mut w = String::new();
            for _ in 0..n {
                w.push_str(sill[r.gen_range(0..sill.len())]);
            }
            if viste.insert(w.clone()) {
                parole.push(w);
            }
        }
        let mut cdf = Vec::new();
        let mut acc = 0.0;
        for i in 0..parole.len() {
            acc += 1.0 / ((i + 1) as f64).powf(1.07);
            cdf.push(acc);
        }
        let citta = (0..60).map(|i| format!("{}{}", parole[i * 7 + 3], ["polis", "burg", "ville", "ano", "ia"][i % 5])).collect();
        Vocabolario { parole, cdf, citta }
    })
}

fn parola(r: &mut SmallRng) -> &'static str {
    let v = voc();
    let x = r.gen::<f64>() * v.cdf.last().unwrap();
    &v.parole[v.cdf.partition_point(|c| *c < x).min(v.parole.len() - 1)]
}

fn frase(s: &mut String, r: &mut SmallRng, n: usize) {
    for i in 0..n {
        if i > 0 {
            s.push(if r.gen_ratio(1, 12) { ',' } else { ' ' });
            if s.ends_with(',') {
                s.push(' ');
            }
        }
        s.push_str(parola(r));
    }
    s.push(if r.gen_ratio(1, 6) { '?' } else { '.' });
}

fn hex(s: &mut String, r: &mut SmallRng, n: usize) {
    for _ in 0..n {
        let _ = write!(s, "{:x}", r.gen_range(0..16u8));
    }
}

fn data(s: &mut String, ts: i64) {
    let secs = ts / 1000;
    let giorno = secs / 86400;
    let _ = write!(s, "\"2026-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z\"", 1 + (giorno / 28) % 12, 1 + giorno % 28, (secs / 3600) % 24, (secs / 60) % 60, secs % 60, ts % 1000);
}

/// Corpo del documento (dopo l'intestazione binaria), lungo circa `len` byte.
pub fn corpo(coll: u8, cust: u32, ts: i64, status: u8, len: usize, r: &mut SmallRng) -> Vec<u8> {
    let mut s = String::with_capacity(len + 64);
    s.push_str("{\"_id\":{\"$oid\":\"");
    hex(&mut s, r, 24);
    let _ = write!(s, "\"}},\"userId\":\"u_{cust:08x}\",");
    match coll {
        0 => {
            let ruoli = ["user", "assistant", "assistant", "tool", "system"];
            let _ = write!(s, "\"conversationId\":\"c_{cust:08x}_{:03}\",\"role\":\"{}\",\"model\":\"{}\",\"createdAt\":", r.gen_range(0..40), ruoli[r.gen_range(0..ruoli.len())], ["gpt-5-mini", "claude-sonnet", "llama-4-70b"][r.gen_range(0..3)]);
            data(&mut s, ts);
            let _ = write!(s, ",\"tokens\":{{\"in\":{},\"out\":{}}},\"content\":\"", r.gen_range(10..4000), r.gen_range(5..900));
            while s.len() + 2 < len {
                let n = r.gen_range(4..18);
                frase(&mut s, r, n);
                s.push(' ');
            }
            s.push_str("\"}");
        }
        1 => {
            let tipi = ["heartbeat", "battery_low", "geofence_enter", "geofence_exit", "button_press", "temp_reading", "door_open", "door_close", "motion", "sync", "error", "boot"];
            let _ = write!(s, "\"deviceId\":\"dev-{cust:08x}-{}\",\"type\":\"{}\",\"ts\":", r.gen_range(0..6), tipi[r.gen_range(0..tipi.len())]);
            data(&mut s, ts);
            let _ = write!(s, ",\"fw\":\"2.{}.{}\",\"battery\":{:.2},\"tempC\":{:.1},\"loc\":{{\"type\":\"Point\",\"coordinates\":[{:.6},{:.6}]}},\"rssi\":-{},\"ok\":{}",
                r.gen_range(0..4), r.gen_range(0..10), r.gen::<f64>(), 15.0 + r.gen::<f64>() * 15.0, 9.0 + r.gen::<f64>(), 45.0 + r.gen::<f64>(), r.gen_range(40..95), r.gen_bool(0.95));
            while s.len() + 16 < len {
                s.push_str(",\"tag\":\"");
                s.push_str(parola(r));
                s.push('"');
            }
            s.push('}');
        }
        _ => {
            let stati = ["created", "paid", "shipped", "delivered", "returned"];
            let _ = write!(s, "\"status\":\"{}\",\"createdAt\":", stati[status as usize % 5]);
            data(&mut s, ts);
            let v = voc();
            let _ = write!(s, ",\"currency\":\"EUR\",\"shipping\":{{\"name\":\"{} {}\",\"street\":\"Via {} {}\",\"city\":\"{}\",\"zip\":\"{:05}\",\"country\":\"IT\"}},\"items\":[",
                parola(r), parola(r), parola(r), r.gen_range(1..200), v.citta[r.gen_range(0..v.citta.len())], r.gen_range(10000..99999));
            let mut tot = 0.0;
            let mut primo = true;
            while s.len() + 80 < len {
                if !primo {
                    s.push(',');
                }
                primo = false;
                let p = (r.gen_range(199..9999) as f64) / 100.0;
                let q = r.gen_range(1..4);
                tot += p * q as f64;
                let _ = write!(s, "{{\"sku\":\"SKU-{:05}\",\"name\":\"{} {}\",\"qty\":{},\"price\":{:.2}}}", r.gen_range(0..20000), parola(r), parola(r), q, p);
            }
            let _ = write!(s, "],\"total\":{tot:.2}}}");
        }
    }
    let mut b = s.into_bytes();
    b.truncate(len);
    b
}

#[cfg(test)]
mod test {
    use super::*;
    use rand::SeedableRng;
    #[test]
    fn deterministico_e_della_taglia_giusta() {
        let mut a = SmallRng::seed_from_u64(1);
        let mut b = SmallRng::seed_from_u64(1);
        for coll in 0..3u8 {
            let x = corpo(coll, 7, 1_767_225_600_000, 1, 300, &mut a);
            let y = corpo(coll, 7, 1_767_225_600_000, 1, 300, &mut b);
            assert_eq!(x, y);
            assert!(x.len() <= 300 && x.len() > 200, "{}", x.len());
        }
    }
}
