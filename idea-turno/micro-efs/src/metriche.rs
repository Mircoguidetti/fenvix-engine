//! Lettura delle metriche CloudWatch di EFS per ogni fase del banco, e conto
//! riportato al mese. Usa la AWS CLI già presente sull'istanza (nessuna dipendenza).
//!
//! Metriche (namespace AWS/EFS, dimensione FileSystemId, periodo 60 s, Sum e SampleCount):
//!   TotalIOBytes        byte con i minimi (32 KiB dati, 4 KiB altro): è la base della
//!                       bolletta Elastic (usage type ETDataAccess-Bytes)
//!   MeteredIOBytes      byte con le letture scontate a 1/3: è quello che consuma il
//!                       throughput (conta per Provisioned)
//!   DataWriteIOBytes, DataReadIOBytes, MetadataIOBytes,
//!   MetadataReadIOBytes, MetadataWriteIOBytes   byte veri e numero di operazioni

use crate::comune::*;
use std::collections::BTreeMap;
use std::fs;
use std::process::Command;

pub const METRICHE: &[&str] = &[
    "TotalIOBytes",
    "MeteredIOBytes",
    "DataWriteIOBytes",
    "DataReadIOBytes",
    "MetadataIOBytes",
    "MetadataReadIOBytes",
    "MetadataWriteIOBytes",
];

const SECONDI_MESE: f64 = 30.0 * 86400.0;
const GIB: f64 = 1024.0 * 1024.0 * 1024.0;
const MIB: f64 = 1024.0 * 1024.0;

/// Data ISO 8601 UTC da secondi Unix (algoritmo "days from civil" al contrario).
pub fn iso(sec: u64) -> String {
    let z = (sec / 86400) as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    let s = sec % 86400;
    format!("{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z", s / 3600, (s / 60) % 60, s % 60)
}

#[derive(Default, Clone, Copy)]
pub struct Somma {
    pub byte: f64,
    pub op: f64,
}

/// Chiede a CloudWatch Sum e SampleCount di una metrica tra due istanti (secondi Unix).
pub fn leggi_metrica(aws: &str, regione: &str, fs_id: &str, nome: &str, da: u64, a: u64) -> Result<Somma, String> {
    let out = Command::new(aws)
        .args(["cloudwatch", "get-metric-statistics", "--region", regione, "--namespace", "AWS/EFS"])
        .args(["--metric-name", nome])
        .args(["--dimensions", &format!("Name=FileSystemId,Value={fs_id}")])
        .args(["--start-time", &iso(da), "--end-time", &iso(a)])
        .args(["--period", "60", "--statistics", "Sum", "SampleCount"])
        .args(["--output", "text", "--query", "Datapoints[].[Sum,SampleCount]"])
        .output()
        .map_err(|e| format!("avvio di {aws}: {e}"))?;
    if !out.status.success() {
        return Err(format!("{nome}: {}", String::from_utf8_lossy(&out.stderr).trim()));
    }
    let mut s = Somma::default();
    for riga in String::from_utf8_lossy(&out.stdout).lines() {
        let v: Vec<f64> = riga.split_whitespace().filter_map(|x| x.parse().ok()).collect();
        if v.len() == 2 {
            s.byte += v[0];
            s.op += v[1];
        }
    }
    Ok(s)
}

struct Fase {
    inizio_ms: u64,
    fine_ms: u64,
    lotti: u64,
    controlli: u64,
    letture: u64,
    byte: u64,
    righe: u64,
}

pub fn metriche(a: &Args) {
    let fs_id = a.req("fs-id");
    let regione = a.s("regione", "eu-west-1");
    let aws = a.s("aws", "aws");
    let fasi_path = a.req("fasi");
    let prezzo_w = a.f("prezzo-scrittura-gb", 0.07); // DICHIARATO eu-west-1, re:Post
    let prezzo_r = a.f("prezzo-lettura-gb", 0.03); // DICHIARATO eu-west-1, re:Post
    let prezzo_prov = a.f("prezzo-prov-mibs-mese", 6.60); // STIMATO: 6,00 in us-east-1 + 10%
    let lotti_s_piccolo = a.f("lotti-s-piccolo", 0.1);
    let lotti_s_cella: Vec<f64> = a.s("lotti-s-cella", "300,730,1000").split(',').filter_map(|x| x.trim().parse().ok()).collect();
    // le fasi con lo stesso nome (una riga per istanza) si sommano
    let mut fasi: BTreeMap<String, Fase> = BTreeMap::new();
    let testo = fs::read_to_string(&fasi_path).unwrap_or_else(|e| fail(&format!("{fasi_path}: {e}")));
    for riga in testo.lines().filter(|l| l.starts_with('{')) {
        let nome = campo(riga, "fase").unwrap_or_default();
        let n = |k: &str| campo(riga, k).and_then(|x| x.parse::<u64>().ok()).unwrap_or(0);
        let f = fasi.entry(nome).or_insert(Fase { inizio_ms: u64::MAX, fine_ms: 0, lotti: 0, controlli: 0, letture: 0, byte: 0, righe: 0 });
        f.inizio_ms = f.inizio_ms.min(n("inizio_ms"));
        f.fine_ms = f.fine_ms.max(n("fine_ms"));
        f.lotti += n("lotti");
        f.controlli += n("controlli");
        f.letture += n("letture");
        f.byte += n("byte");
        f.righe += 1;
    }
    let finestre: Vec<(String, u64, u64)> =
        fasi.iter().map(|(k, f)| (k.clone(), f.inizio_ms / 1000 / 60 * 60, (f.fine_ms / 1000).div_ceil(60) * 60)).collect();
    for (nome, f) in &fasi {
        let (_, da, a_) = finestre.iter().find(|x| &x.0 == nome).unwrap().clone();
        // due fasi nello stesso minuto non si possono separare: lo si dice
        let sovrapposta = finestre.iter().any(|(n2, d2, a2)| n2 != nome && *d2 < a_ && da < *a2);
        let mut m: BTreeMap<&str, Somma> = BTreeMap::new();
        let mut errore = String::new();
        for nome_m in METRICHE {
            match leggi_metrica(&aws, &regione, &fs_id, nome_m, da, a_) {
                Ok(s) => {
                    m.insert(nome_m, s);
                }
                Err(e) => errore = e,
            }
        }
        let g = |k: &str| m.get(k).copied().unwrap_or_default();
        // le conferme si contano a lotti; nelle fasi di sole domande, a domande
        let lotti = if f.lotti > 0 { f.lotti } else { f.controlli.max(1) } as f64;
        let op_app = (f.lotti + f.controlli + f.letture).max(1) as f64;
        let total = g("TotalIOBytes").byte;
        let metered = g("MeteredIOBytes").byte;
        // parte di lettura fatturata, stimata con i minimi (4 KiB metadati, 32 KiB dati)
        let letture_fatt = (g("MetadataReadIOBytes").op * 4096.0).max(g("MetadataReadIOBytes").byte)
            + (g("DataReadIOBytes").op * 32768.0).max(g("DataReadIOBytes").byte);
        let letture_fatt = letture_fatt.min(total);
        let scritture_fatt = total - letture_fatt;
        let costo_lotto_alto = total / lotti / GIB * prezzo_w;
        let costo_lotto = (scritture_fatt * prezzo_w + letture_fatt * prezzo_r) / lotti / GIB;
        let piccolo = costo_lotto * lotti_s_piccolo * SECONDI_MESE;
        let metered_lotto = metered / lotti;
        let mut campi = vec![
            ("fase", js(nome)),
            ("istanze", f.righe.to_string()),
            ("da", js(&iso(da))),
            ("a", js(&iso(a_))),
            ("finestra_sovrapposta", sovrapposta.to_string()),
            ("lotti_app", f.lotti.to_string()),
            ("controlli_app", f.controlli.to_string()),
            ("letture_app", f.letture.to_string()),
            ("byte_app", f.byte.to_string()),
        ];
        for (k, s) in &m {
            campi.push((Box::leak(format!("{k}_byte").into_boxed_str()), format!("{:.0}", s.byte)));
            campi.push((Box::leak(format!("{k}_op").into_boxed_str()), format!("{:.0}", s.op)));
            // KiB per operazione contata da EFS (dice il rotondamento: 4 o 32 KiB)
            let kib_op = if s.op > 0.0 { s.byte / s.op / 1024.0 } else { 0.0 };
            campi.push((Box::leak(format!("{k}_kib_per_op_efs").into_boxed_str()), format!("{kib_op:.2}")));
        }
        // KiB per operazione dell'applicazione (lotto + domanda + lettura)
        campi.push(("total_kib_per_op_app", format!("{:.2}", total / op_app / 1024.0)));
        campi.push(("metered_kib_per_op_app", format!("{:.2}", metered / op_app / 1024.0)));
        campi.push(("op_efs_per_op_app", format!("{:.2}", g("TotalIOBytes").op / op_app)));
        campi.push(("total_byte_per_lotto", format!("{:.0}", total / lotti)));
        campi.push(("metered_byte_per_lotto", format!("{metered_lotto:.0}")));
        campi.push(("elastic_costo_per_lotto_usd", format!("{costo_lotto:.9}")));
        campi.push(("elastic_costo_per_lotto_tetto_usd", format!("{costo_lotto_alto:.9}")));
        campi.push(("elastic_db_piccolo_mese_usd", format!("{piccolo:.4}")));
        for &x in &lotti_s_cella {
            let el = costo_lotto * x * SECONDI_MESE;
            let mibs = metered_lotto * x / MIB;
            campi.push((Box::leak(format!("elastic_cella_{x}_lotti_s_mese_usd").into_boxed_str()), format!("{el:.0}")));
            campi.push((Box::leak(format!("prov_cella_{x}_lotti_s_mibs").into_boxed_str()), format!("{mibs:.2}")));
            campi.push((Box::leak(format!("prov_cella_{x}_lotti_s_mese_usd").into_boxed_str()), format!("{:.0}", mibs * prezzo_prov)));
        }
        if let Some(&x) = lotti_s_cella.iter().find(|&&x| (x - 1000.0).abs() < 1e-9) {
            let el = costo_lotto * x * SECONDI_MESE;
            let pr = metered_lotto * x / MIB * prezzo_prov;
            campi.push(("soglia_cella_1000_elastic", js(if el > 430.0 { "MUORE (> 430 $)" } else { "passa" })));
            campi.push(("soglia_cella_1000_provisioned", js(if pr > 430.0 { "MUORE (> 430 $)" } else { "passa" })));
        }
        campi.push(("soglia_db_piccolo", js(if piccolo > 0.15 { "MUORE (> 0,15 $)" } else { "passa" })));
        if !errore.is_empty() {
            campi.push(("errore", js(&errore)));
        }
        println!("{}", json(&campi));
    }
}

#[cfg(test)]
mod test {
    use super::iso;
    #[test]
    fn date() {
        assert_eq!(iso(0), "1970-01-01T00:00:00Z");
        assert_eq!(iso(1_793_577_600), "2026-11-02T00:00:00Z");
        assert_eq!(iso(951_782_400), "2000-02-29T00:00:00Z");
    }
}
