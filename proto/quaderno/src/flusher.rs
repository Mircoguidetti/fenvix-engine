//! Scaricatore: un worker qualsiasi (senza stato durevole proprio) che porta su S3 i record confermati e lo dice al
//! registro.
//!
//! Ogni `interval` chiede a una replica i database con record non ancora su S3 (`Pending`); quelli con il record più
//! vecchio da più di `age` vengono scaricati, raggruppati: i database piccoli in `groups` gruppi per hash, il database
//! caldo da solo. Per ogni gruppo: `scaricabili` → un oggetto SST (blocco immutabile) → manifest del gruppo con PUT
//! condizionale (If-Match sull'etag precedente) → `conferma_scarico(db, fino_a, etag del manifest)` al leader.
//! Compattazione: ogni `compact_every` scarichi del gruppo l'intero stato del gruppo (tenuto in memoria dal worker)
//! diventa un solo SST e i vecchi oggetti si cancellano.
//! Semplificazioni: un solo scaricatore; lo stato compattato sta nella RAM del worker (un worker vero lo rileggerebbe
//! dagli SST); il formato SST è una sequenza di frame (db, posizione, tipo, chiave, valore) senza indice.

use crate::client::Cluster;
use crate::codec::*;
use crate::window::delta_apply;
use bytes::{BufMut, Bytes, BytesMut};
use object_store::aws::{AmazonS3, AmazonS3Builder, S3ConditionalPut};
use object_store::path::Path;
use object_store::{ClientOptions, ObjectStore, PutMode, PutOptions, PutPayload, RetryConfig, UpdateVersion};
use serde_json::json;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub struct FlushCfg {
    pub endpoint: String,
    pub bucket: String,
    pub groups: u64,
    pub age_ms: u64,
    pub interval_ms: u64,
    pub compact_every: u32,
    pub hot_db: Option<Db>,
}

#[derive(Default)]
struct Group {
    merged: BTreeMap<(Db, Bytes), (Pos, Option<Bytes>)>,
    objects: Vec<String>,
    etag: Option<String>,
    flushes: u32,
}

pub fn s3(endpoint: &str, bucket: &str) -> anyhow::Result<AmazonS3> {
    Ok(AmazonS3Builder::new()
        .with_endpoint(endpoint)
        .with_allow_http(true)
        .with_bucket_name(bucket)
        .with_access_key_id("minioadmin")
        .with_secret_access_key("minioadmin")
        .with_region("us-east-1")
        .with_conditional_put(S3ConditionalPut::ETagMatch)
        .with_client_options(ClientOptions::new().with_allow_http(true).with_timeout(Duration::from_secs(10)).with_connect_timeout(Duration::from_secs(2)))
        .with_retry(RetryConfig { max_retries: 1, retry_timeout: Duration::from_secs(10), ..Default::default() })
        .build()?)
}

fn group_of(db: &Db, c: &FlushCfg) -> u64 {
    if Some(*db) == c.hot_db {
        return c.groups;
    }
    let mut h = 0xcbf29ce484222325u64;
    for b in db {
        h = (h ^ *b as u64).wrapping_mul(0x100000001b3);
    }
    h % c.groups
}

fn sst_put(b: &mut BytesMut, db: &Db, key: &[u8], pos: Pos, val: Option<&Bytes>) {
    let mut x = BytesMut::new();
    x.put_slice(db);
    x.put_u64_le(pos);
    x.put_u8(if val.is_some() { 0 } else { 1 });
    put16(&mut x, key);
    put32(&mut x, val.map(|v| &v[..]).unwrap_or(&[]));
    b.put_slice(&frame(&x));
}

pub async fn run(cl: Arc<Cluster>, c: FlushCfg) -> anyhow::Result<()> {
    let store = Arc::new(s3(&c.endpoint, &c.bucket)?);
    let mut groups: HashMap<u64, Group> = HashMap::new();
    let mut tot_recs = 0u64;
    let mut tot_bytes = 0u64;
    let mut errors = 0u64;
    let t_start = Instant::now();
    let mut last_report = Instant::now();
    let mut rr = 0usize;
    loop {
        let t_cycle = Instant::now();
        rr += 1;
        let r = rr % cl.n();
        let pending = match cl.call(r, &Req::Pending { limit: 2_000_000 }, Duration::from_secs(10)).await {
            Ok(Resp::Pending(v)) => v,
            _ => {
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
        };
        let now = now_ms();
        let mut due: BTreeMap<u64, Vec<Db>> = BTreeMap::new();
        for (db, p) in pending {
            if now.saturating_sub(pos_ms(p)) >= c.age_ms {
                due.entry(group_of(&db, &c)).or_default().push(db);
            }
        }
        for (g, dbs) in due {
          // un gruppo può richiedere più giri (risposte limitate a 4 MB): si ripete finché è in pari
          for _round in 0..64 {
            let gs = groups.entry(g).or_default();
            // scaricabili, a pezzi, da un follower se possibile (il leader ha già il suo lavoro)
            let l = cl.leader.load(std::sync::atomic::Ordering::Relaxed);
            let r = if l >= 0 && r as i32 == l { (r + 1) % cl.n() } else { r };
            let mut recs: Vec<(Db, Vec<FlushRec>)> = vec![];
            let mut resp_bytes = 0usize;
            for chunk in dbs.chunks(4000) {
                match cl.call(r, &Req::Flushable { dbs: chunk.to_vec(), upto: Pos::MAX }, Duration::from_secs(30)).await {
                    Ok(Resp::Flushable(v)) => {
                        for (_, rs) in &v {
                            for x in rs {
                                for w in &x.writes {
                                    resp_bytes += w.key.len() + w.val.len() + 16;
                                }
                            }
                        }
                        recs.extend(v)
                    }
                    other => {
                        eprintln!("scaricabili: {other:?}");
                        errors += 1
                    }
                }
            }
            if recs.is_empty() {
                break;
            }
            let more = resp_bytes > (3 << 20);
            let mut sst = BytesMut::with_capacity(1 << 20);
            let mut items = vec![];
            let mut n = 0u64;
            for (db, rs) in &recs {
                let mut maxp = 0;
                for rcd in rs {
                    for w in &rcd.writes {
                        let k = (*db, w.key.clone());
                        let nv = match w.kind {
                            Kind::Put => Some(w.val.clone()),
                            Kind::Del => None,
                            Kind::Delta => {
                                let base = gs.merged.get(&k).and_then(|x| x.1.clone());
                                Some(delta_apply(base.as_deref(), &w.val))
                            }
                        };
                        sst_put(&mut sst, db, &w.key, rcd.pos, nv.as_ref());
                        gs.merged.insert(k, (rcd.pos, nv));
                    }
                    maxp = maxp.max(rcd.pos);
                    n += 1;
                }
                items.push((*db, maxp));
            }
            let sst = sst.freeze();
            let name = format!("g{g:03}/d-{:016x}-{:08x}.sst", items.iter().map(|x| x.1).max().unwrap(), rand::random::<u32>());
            if let Err(e) = store.put(&Path::from(name.clone()), PutPayload::from(sst.clone())).await {
                eprintln!("put {name}: {e}");
                errors += 1;
                break;
            }
            gs.flushes += 1;
            let mut old = vec![];
            gs.objects.push(name);
            if gs.flushes % c.compact_every == 0 {
                let mut full = BytesMut::with_capacity(1 << 20);
                for ((db, k), (p, v)) in &gs.merged {
                    sst_put(&mut full, db, k, *p, v.as_ref());
                }
                let fname = format!("g{g:03}/f-{:016x}-{:08x}.sst", items.iter().map(|x| x.1).max().unwrap(), rand::random::<u32>());
                if store.put(&Path::from(fname.clone()), PutPayload::from(full.freeze())).await.is_ok() {
                    old = std::mem::replace(&mut gs.objects, vec![fname]);
                }
            }
            // manifest del gruppo: PUT condizionale sull'etag precedente
            let man = serde_json::to_vec(&json!({"group": g, "objects": gs.objects})).unwrap();
            let mode = match &gs.etag {
                None => PutMode::Overwrite,
                Some(e) => PutMode::Update(UpdateVersion { e_tag: Some(e.clone()), version: None }),
            };
            let mp = Path::from(format!("g{g:03}/manifest.json"));
            let etag = match store.put_opts(&mp, PutPayload::from(man), PutOptions { mode, ..Default::default() }).await {
                Ok(r) => r.e_tag.unwrap_or_default(),
                Err(e) => {
                    eprintln!("manifest g{g}: {e}");
                    errors += 1;
                    // ricomincia dall'etag attuale al prossimo giro
                    gs.etag = store.head(&mp).await.ok().and_then(|m| m.e_tag);
                    break;
                }
            };
            gs.etag = Some(etag.clone());
            for o in old {
                let _ = store.delete(&Path::from(o)).await;
            }
            // conferma_scarico al leader (a pezzi)
            let et = Bytes::from(etag);
            for ch in items.chunks(4000) {
                let it = ch.iter().map(|(d, p)| (*d, *p, et.clone())).collect();
                match cl.call_leader(&Req::FlushDone { items: it }, Duration::from_secs(30)).await {
                    Some(Resp::Committed(_)) => {}
                    _ => errors += 1,
                }
            }
            tot_recs += n;
            tot_bytes += sst.len() as u64;
            if !more {
                break;
            }
          }
        }
        if last_report.elapsed() >= Duration::from_secs(10) {
            last_report = Instant::now();
            eprintln!(
                "scaricatore t={:.0}s record {} MB {:.1} errori {} gruppi {}",
                t_start.elapsed().as_secs_f64(),
                tot_recs,
                tot_bytes as f64 / 1e6,
                errors,
                groups.len()
            );
        }
        let el = t_cycle.elapsed();
        if el < Duration::from_millis(c.interval_ms) {
            tokio::time::sleep(Duration::from_millis(c.interval_ms) - el).await;
        }
    }
}
