//! Carichi di prova: scelta delle chiavi (uniforme o zipf), valori, precarico dei database negli SST di base.

use crate::iface::*;
use crate::manifest::{self, gruppo_di, Manifest, Raggruppa, Run};
use crate::registro::RegistroSim;
use crate::sst::{self, Entry};
use crate::store::{Cas, Store};
use bytes::Bytes;
use futures::StreamExt;
use rand::{Rng, RngCore};
use std::collections::BTreeMap;

/// Distribuzione zipf su [0, n) (theta = 0: uniforme). Il rango si rimescola per database, così le chiavi calde
/// cambiano da un database all'altro.
pub struct Zipf {
    n: u64,
    cdf: Vec<f64>,
}

impl Zipf {
    pub fn new(n: u64, theta: f64) -> Zipf {
        if theta <= 0.0 {
            return Zipf { n, cdf: vec![] };
        }
        let mut cdf = Vec::with_capacity(n as usize);
        let mut s = 0.0;
        for i in 1..=n {
            s += 1.0 / (i as f64).powf(theta);
            cdf.push(s);
        }
        for x in cdf.iter_mut() {
            *x /= s;
        }
        Zipf { n, cdf }
    }

    pub fn rango<R: Rng>(&self, r: &mut R) -> u64 {
        if self.cdf.is_empty() {
            return r.gen_range(0..self.n);
        }
        let u: f64 = r.gen();
        (self.cdf.partition_point(|x| *x < u) as u64).min(self.n - 1)
    }

    /// chiave (id del documento) per il database `seme`
    pub fn id<R: Rng>(&self, r: &mut R, seme: u64) -> u64 {
        mescola(self.rango(r), self.n, seme)
    }
}

fn gcd(a: u64, b: u64) -> u64 {
    if b == 0 {
        a
    } else {
        gcd(b, a % b)
    }
}

/// biiezione di [0, n): rango -> id
pub fn mescola(rango: u64, n: u64, seme: u64) -> u64 {
    let mut p = 2_654_435_761u64 % n.max(1);
    while n > 1 && gcd(p, n) != 1 {
        p += 1;
    }
    (rango.wrapping_mul(p) + seme.wrapping_mul(0x9e37_79b9)) % n.max(1)
}

pub fn valore<R: RngCore>(r: &mut R, n: usize) -> Bytes {
    let mut v = vec![0u8; n];
    r.fill_bytes(&mut v);
    Bytes::from(v)
}

pub const COLL: &str = "c";

/// Scrive per ogni gruppo un SST di base con `docs` documenti da `vsize` byte per database, alla posizione `pos0`,
/// e il manifest; registra lo stato di scarico nel registro. Usa uno `Store` a parte: le sue richieste non entrano
/// nei conteggi della misura.
pub async fn precarica(st: &Store, reg: &RegistroSim, r: Raggruppa, dbs: &[Db], docs: u64, vsize: usize, blocco: usize, pos0: Pos) -> anyhow::Result<()> {
    let mut gruppi: BTreeMap<String, Vec<Db>> = BTreeMap::new();
    for d in dbs {
        gruppi.entry(gruppo_di(d, r)).or_default().push(*d);
    }
    let res: Vec<anyhow::Result<()>> = futures::stream::iter(gruppi)
        .map(|(g, mut ds)| async move {
            ds.sort();
            let es = tokio::task::spawn_blocking(move || {
                let mut rng = rand::rngs::SmallRng::from_entropy_compat();
                let mut es = Vec::with_capacity(ds.len() * docs as usize);
                for d in &ds {
                    for i in 0..docs {
                        es.push(Entry { key: sst::full_key(d, &doc_key(COLL, i)), pos: pos0, kind: Kind::Put, val: valore(&mut rng, vsize) });
                    }
                }
                es.sort_by(|a, b| a.key.cmp(&b.key));
                (ds, sst::build(es, blocco))
            })
            .await?;
            let (ds, b) = es;
            let id = format!("{:016x}-base", b.max);
            let data = Bytes::from(b.data);
            st.put(&manifest::path_sst(&g, &id), data.clone()).await?;
            let mut m = Manifest { versione: 1, ..Default::default() };
            m.run.push(Run { id, min: b.min, max: b.max, dim: data.len() as u64, meta_off: b.meta_off, meta_len: b.meta_len, voci: b.voci });
            for d in &ds {
                m.scaricato.insert(db_hex(d), pos0);
            }
            let etag = match manifest::scrivi(st, &g, &m, None).await {
                Cas::Ok(e) => e,
                x => anyhow::bail!("manifest di base {g}: {x:?}"),
            };
            for d in &ds {
                reg.imposta_scaricato(*d, pos0, Some(etag.clone()));
            }
            Ok(())
        })
        .buffer_unordered(16)
        .collect()
        .await;
    for r in res {
        r?;
    }
    reg.avanza(pos0);
    Ok(())
}

trait FromEntropyCompat {
    fn from_entropy_compat() -> Self;
}
impl FromEntropyCompat for rand::rngs::SmallRng {
    fn from_entropy_compat() -> Self {
        use rand::SeedableRng;
        rand::rngs::SmallRng::from_entropy()
    }
}

/// Commit cieco di un put (carico di prova).
pub fn put_req(db: Db, key: Bytes, val: Bytes) -> CommitReq {
    let mut txn = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut txn);
    CommitReq { txn, db, read_pos: 0, mode: Mode::Blind, writes: vec![Write { key, kind: Kind::Put, val }], reads: vec![], session: None }
}
