//! Prova della manopola delle zone (N1, branch prova-zone): copia grezza del quaderno su S3 e ripristino da zero.
//!
//! **Copia.** Il capo (leader), ogni `interval_ms`, passa al thread della copia il tratto di registro confermato dopo
//! l'ultimo tratto copiato. Il thread lo scrive su S3 come UN oggetto, con nome `<prefisso>/quaderno/<inizio, 16 cifre
//! esadecimali>` e PUT condizionale "solo se non esiste" (If-None-Match): per ogni inizio c'è al più un oggetto, e
//! ogni oggetto comincia dove finisce il precedente. Se l'oggetto esiste già (l'ha scritto un capo precedente, o un
//! PUT dato per fallito era passato), lo si rilegge e si continua dalla sua fine: il contenuto confermato del
//! registro è lo stesso su ogni replica, quindi va bene. Dopo il PUT il capo mette nel registro una voce di servizio
//! `Copiato(fino_a)`: tutte le repliche sanno fin dove la copia è su S3 e non cancellano dal disco voci non copiate.
//!
//! Oggetto: `PQCOPIA1 | inizio u64 | fine u64 | mandato_prima u64 | pos_prima u64 | pos_inizio u64 | pos_fine u64 |
//! n u32 | n frame delle voci (come sul disco) | crc32 u32 di tutto quello che precede`.
//!
//! **Ripristino.** `ripristina` legge i fogli indice dello scaricatore (manifest con lo `scaricato` per database) e la
//! catena degli oggetti in ordine di inizio. Si ferma al primo buco (un oggetto che non comincia alla fine del
//! precedente più uno), al primo oggetto illeggibile o con CRC sbagliato, o a una voce che non continua il registro.
//! Gli oggetti dopo il punto di arresto vengono messi da parte (copiati sotto `<prefisso>/scartati/` e cancellati):
//! la cella nuova riuserà quegli indici con voci diverse. Poi scrive tre cartelle di replica nuove con i segmenti, il
//! checkpoint (posizioni su S3 dai manifest, fin dove è copiato) e lo stato di Raft.

use crate::codec::{read_frame, Db, Entry, FrameRead, Pos};
use crate::store::{self, Ckpt, RaftState};
use bytes::{BufMut, Bytes, BytesMut};
use object_store::path::Path;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub const MAGIC: &[u8; 8] = b"PQCOPIA1";
pub const HDR: usize = 8 + 6 * 8 + 4;

#[derive(Clone, Debug)]
pub struct CopyCfg {
    pub endpoint: String,
    pub bucket: String,
    pub prefix: String,
    pub interval_ms: u64,
    pub max_bytes: u64,
    /// allarme se il ritardo della copia supera questi ms
    pub alarm_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Head {
    pub start: u64,
    pub end: u64,
    pub prev_term: u64,
    pub prev_pos: Pos,
    pub start_pos: Pos,
    pub end_pos: Pos,
    pub n: u32,
}

pub fn key(prefix: &str, start: u64) -> String {
    format!("{prefix}/quaderno/{start:016x}")
}

pub fn parse_key(prefix: &str, k: &str) -> Option<u64> {
    let s = k.strip_prefix(&format!("{prefix}/quaderno/"))?;
    if s.len() != 16 {
        return None;
    }
    u64::from_str_radix(s, 16).ok()
}

pub fn encode(h: &Head, frames: &[Bytes]) -> Bytes {
    let tot: usize = frames.iter().map(|f| f.len()).sum();
    let mut b = BytesMut::with_capacity(HDR + tot + 4);
    b.put_slice(MAGIC);
    for x in [h.start, h.end, h.prev_term, h.prev_pos, h.start_pos, h.end_pos] {
        b.put_u64_le(x);
    }
    b.put_u32_le(frames.len() as u32);
    for f in frames {
        b.put_slice(f);
    }
    let crc = crc32fast::hash(&b);
    b.put_u32_le(crc);
    b.freeze()
}

pub fn decode_head(b: &[u8]) -> Option<Head> {
    if b.len() < HDR || &b[..8] != MAGIC {
        return None;
    }
    let u = |i: usize| u64::from_le_bytes(b[8 + 8 * i..16 + 8 * i].try_into().unwrap());
    Some(Head { start: u(0), end: u(1), prev_term: u(2), prev_pos: u(3), start_pos: u(4), end_pos: u(5), n: u32::from_le_bytes(b[56..60].try_into().unwrap()) })
}

/// Decodifica un oggetto intero: intestazione, CRC, voci contigue da `start` a `end`. Err = oggetto da non usare.
pub fn decode(b: &Bytes) -> Result<(Head, Vec<Entry>), String> {
    let h = decode_head(b).ok_or("intestazione non valida")?;
    if b.len() < HDR + 4 {
        return Err("troppo corto".into());
    }
    let body_end = b.len() - 4;
    let crc = u32::from_le_bytes(b[body_end..].try_into().unwrap());
    if crc32fast::hash(&b[..body_end]) != crc {
        return Err("CRC sbagliato".into());
    }
    let mut p = HDR;
    let mut v = Vec::with_capacity(h.n as usize);
    while p < body_end {
        match read_frame(&b[p..body_end]) {
            FrameRead::Ok(_, en) => {
                let e = Entry::decode(b.slice(p..p + en)).map_err(|_| "voce non decodificabile")?;
                p += en;
                v.push(e);
            }
            _ => return Err("frame non valido".into()),
        }
    }
    if v.len() != h.n as usize || h.end < h.start || (h.end - h.start + 1) as usize != v.len() {
        return Err(format!("numero di voci {} diverso dall'intestazione ({}..{})", v.len(), h.start, h.end));
    }
    for (k, e) in v.iter().enumerate() {
        if e.index != h.start + k as u64 {
            return Err(format!("voce {} fuori posto (attesa {})", e.index, h.start + k as u64));
        }
    }
    if v.first().map(|e| e.pos) != Some(h.start_pos) || v.last().map(|e| e.pos) != Some(h.end_pos) {
        return Err("posizioni diverse dall'intestazione".into());
    }
    Ok((h, v))
}

// ================================================================================================ thread della copia

pub enum CopyJob {
    Put { head: Head, frames: Vec<Bytes> },
    /// cancella gli oggetti interamente su S3 (fine < `pos`) e copiati (fine ≤ `idx`), tenendo sempre l'ultimo
    DeleteBelow { pos: Pos, idx: u64 },
}

/// Esito di un `Put`: tratto su S3 fino a `end` (`existed` = c'era già un oggetto con quell'inizio).
pub struct Copied {
    pub start: u64,
    pub end: u64,
    pub end_pos: Pos,
    pub existed: bool,
}

#[derive(Default)]
pub struct CopyStats {
    pub puts: AtomicU64,
    pub put_errs: AtomicU64,
    pub existed: AtomicU64,
    pub bytes: AtomicU64,
    pub deleted: AtomicU64,
    pub cpu_us: AtomicU64,
    /// durata dell'ultimo PUT riuscito e massima (ms)
    pub last_put_ms: AtomicU64,
    pub max_put_ms: AtomicU64,
}

pub fn spawn(cfg: CopyCfg, rx: crossbeam_channel::Receiver<CopyJob>, stats: Arc<CopyStats>, done: impl Fn(Copied) + Send + 'static) {
    std::thread::Builder::new()
        .name("copia".into())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            let store = crate::flusher::s3(&cfg.endpoint, &cfg.bucket).expect("s3 della copia");
            let mut known: Option<BTreeMap<u64, (u64, Pos)>> = None;
            while let Ok(job) = rx.recv() {
                match job {
                    CopyJob::Put { head, frames } => {
                        let data = encode(&head, &frames);
                        drop(frames);
                        let mut wait = Duration::from_millis(100);
                        loop {
                            let c0 = crate::node::thread_cpu_us();
                            let t0 = Instant::now();
                            let r = rt.block_on(put_once(&store, &cfg.prefix, &head, data.clone()));
                            stats.cpu_us.fetch_add(crate::node::thread_cpu_us().saturating_sub(c0), Relaxed);
                            match r {
                                Ok(c) => {
                                    let ms = t0.elapsed().as_millis() as u64;
                                    stats.last_put_ms.store(ms, Relaxed);
                                    stats.max_put_ms.fetch_max(ms, Relaxed);
                                    if c.existed {
                                        stats.existed.fetch_add(1, Relaxed);
                                    } else {
                                        stats.puts.fetch_add(1, Relaxed);
                                        stats.bytes.fetch_add(data.len() as u64, Relaxed);
                                    }
                                    if let Some(k) = known.as_mut() {
                                        k.insert(c.start, (c.end, c.end_pos));
                                    }
                                    done(c);
                                    break;
                                }
                                Err(e) => {
                                    stats.put_errs.fetch_add(1, Relaxed);
                                    eprintln!("copia: PUT {} fallito ({e}), ritento tra {:?}", key(&cfg.prefix, head.start), wait);
                                    std::thread::sleep(wait);
                                    wait = (wait * 2).min(Duration::from_secs(2));
                                }
                            }
                        }
                    }
                    CopyJob::DeleteBelow { pos, idx } => {
                        let c0 = crate::node::thread_cpu_us();
                        if known.is_none() {
                            known = rt.block_on(list_heads(&store, &cfg.prefix)).ok();
                        }
                        if let Some(k) = known.as_mut() {
                            let last = k.keys().next_back().copied();
                            let dead: Vec<u64> = k.iter().filter(|(s, (e, ep))| Some(**s) != last && *ep < pos && *e <= idx).map(|(s, _)| *s).collect();
                            // in ordine crescente: un errore lascia la catena intera (si ferma prima del buco)
                            for s in dead {
                                if rt.block_on(store.delete(&Path::from(key(&cfg.prefix, s)))).is_err() {
                                    break;
                                }
                                k.remove(&s);
                                stats.deleted.fetch_add(1, Relaxed);
                            }
                        }
                        stats.cpu_us.fetch_add(crate::node::thread_cpu_us().saturating_sub(c0), Relaxed);
                    }
                }
            }
        })
        .unwrap();
}

async fn put_once(store: &dyn ObjectStore, prefix: &str, head: &Head, data: Bytes) -> Result<Copied, String> {
    let p = Path::from(key(prefix, head.start));
    match store.put_opts(&p, PutPayload::from(data), PutOptions { mode: PutMode::Create, ..Default::default() }).await {
        Ok(_) => Ok(Copied { start: head.start, end: head.end, end_pos: head.end_pos, existed: false }),
        Err(object_store::Error::AlreadyExists { .. }) | Err(object_store::Error::Precondition { .. }) => {
            // c'è già: lo si rilegge e si continua dalla sua fine
            let b = store.get(&p).await.map_err(|e| e.to_string())?.bytes().await.map_err(|e| e.to_string())?;
            let (h, _) = decode(&b).map_err(|e| format!("oggetto esistente illeggibile: {e}"))?;
            if h.start != head.start {
                return Err("oggetto esistente con inizio diverso".into());
            }
            Ok(Copied { start: h.start, end: h.end, end_pos: h.end_pos, existed: true })
        }
        Err(e) => Err(e.to_string()),
    }
}

/// Elenco degli oggetti della copia con le loro intestazioni (lette con GET a intervallo).
pub async fn list_heads(store: &dyn ObjectStore, prefix: &str) -> anyhow::Result<BTreeMap<u64, (u64, Pos)>> {
    use futures::TryStreamExt;
    let objs: Vec<_> = store.list(Some(&Path::from(format!("{prefix}/quaderno")))).try_collect().await?;
    let mut m = BTreeMap::new();
    for o in objs {
        let Some(s) = parse_key(prefix, o.location.as_ref()) else { continue };
        let b = store.get_range(&o.location, 0..HDR).await?;
        if let Some(h) = decode_head(&b) {
            m.insert(s, (h.end, h.end_pos));
        }
    }
    Ok(m)
}

// ================================================================================================ ripristino

#[derive(Debug, Default, serde::Serialize)]
pub struct Chain {
    /// voci ripristinate, in ordine e contigue
    #[serde(skip)]
    pub entries: Vec<Entry>,
    #[serde(skip)]
    pub first: Option<Head>,
    pub objects_used: usize,
    pub objects_after_stop: Vec<String>,
    /// perché la catena si ferma: "fine" (nessun oggetto dopo), "buco a <indice>", "oggetto illeggibile ..."
    pub stop: String,
    pub first_index: u64,
    pub last_index: u64,
    pub last_pos: Pos,
}

/// Costruisce la catena dagli oggetti (inizio → byte), fermandosi al primo buco o al primo oggetto non valido.
pub fn chain(prefix: &str, objs: &BTreeMap<u64, Bytes>) -> Chain {
    let mut c = Chain { stop: "fine".into(), ..Default::default() };
    let mut expected: Option<u64> = None;
    let mut last_term = 0u64;
    let mut stopped = false;
    for (s, b) in objs {
        if stopped {
            c.objects_after_stop.push(key(prefix, *s));
            continue;
        }
        if let Some(x) = expected {
            if *s != x {
                c.stop = if *s > x { format!("buco a {x} (oggetto successivo comincia a {s})") } else { format!("oggetto sovrapposto a {s} (atteso {x})") };
                stopped = true;
                c.objects_after_stop.push(key(prefix, *s));
                continue;
            }
        }
        match decode(b) {
            Ok((h, v)) if h.start == *s && v.first().map(|e| e.term >= last_term).unwrap_or(false) && v.windows(2).all(|w| w[1].term >= w[0].term) => {
                if c.first.is_none() {
                    c.first = Some(h.clone());
                    c.first_index = h.start;
                }
                last_term = v.last().unwrap().term;
                expected = Some(h.end + 1);
                c.last_index = h.end;
                c.last_pos = h.end_pos;
                c.objects_used += 1;
                c.entries.extend(v);
            }
            Ok(_) => {
                c.stop = format!("oggetto {s} non continua il registro");
                stopped = true;
                c.objects_after_stop.push(key(prefix, *s));
            }
            Err(e) => {
                c.stop = format!("oggetto {s} illeggibile: {e}");
                stopped = true;
                c.objects_after_stop.push(key(prefix, *s));
            }
        }
    }
    c
}

/// Legge i manifest dei gruppi `0..=groups`: posizione su S3 per database (la più alta tra i gruppi).
pub async fn manifests_flushed(store: &dyn ObjectStore, groups: u64) -> anyhow::Result<(HashMap<Db, Pos>, u64)> {
    let mut m: HashMap<Db, Pos> = HashMap::new();
    let mut found = 0;
    for g in 0..=groups {
        let mp = Path::from(format!("g{g:03}/manifest.json"));
        let b = match store.get(&mp).await {
            Ok(r) => r.bytes().await?,
            Err(object_store::Error::NotFound { .. }) => continue,
            Err(e) => return Err(e.into()),
        };
        found += 1;
        let v: serde_json::Value = serde_json::from_slice(&b)?;
        if let Some(f) = v["flushed"].as_object() {
            for (h, p) in f {
                if let (Some(db), Some(p)) = (store::unhex16(h), p.as_u64()) {
                    let e = m.entry(db).or_insert(0);
                    *e = (*e).max(p);
                }
            }
        }
    }
    Ok((m, found))
}

pub struct RestoreCfg {
    pub endpoint: String,
    pub bucket: String,
    pub prefix: String,
    pub groups: u64,
    pub dirs: Vec<std::path::PathBuf>,
    pub seg_bytes: u64,
}

pub async fn ripristina(c: RestoreCfg) -> anyhow::Result<serde_json::Value> {
    use futures::TryStreamExt;
    let t0 = Instant::now();
    let store = crate::flusher::s3(&c.endpoint, &c.bucket)?;
    for d in &c.dirs {
        if d.exists() && std::fs::read_dir(d)?.next().is_some() {
            anyhow::bail!("la cartella {} non è vuota: il ripristino parte da cartelle nuove", d.display());
        }
    }
    let (flushed, n_man) = manifests_flushed(&store, c.groups).await?;
    let t_man = t0.elapsed();
    let list: Vec<_> = store.list(Some(&Path::from(format!("{}/quaderno", c.prefix)))).try_collect().await?;
    let mut objs = BTreeMap::new();
    let mut bytes = 0u64;
    for o in &list {
        let Some(s) = parse_key(&c.prefix, o.location.as_ref()) else { continue };
        let b = store.get(&o.location).await?.bytes().await?;
        bytes += b.len() as u64;
        objs.insert(s, b);
    }
    let t_read = t0.elapsed();
    let ch = chain(&c.prefix, &objs);
    // oggetti dopo il punto di arresto: messi da parte, la cella nuova riusa quegli indici
    let tag = crate::codec::now_ms();
    for k in &ch.objects_after_stop {
        let src = Path::from(k.clone());
        let dst = Path::from(format!("{}/scartati/{tag}/{}", c.prefix, k.rsplit('/').next().unwrap()));
        store.copy(&src, &dst).await?;
        store.delete(&src).await?;
    }
    let max_term = ch.entries.iter().map(|e| e.term).max().unwrap_or(0);
    let (prev_index, prev_term, prev_pos) = match &ch.first {
        Some(h) => (h.start - 1, h.prev_term, h.prev_pos),
        None => (0, 0, 0),
    };
    let ck = Ckpt { prev_index, prev_term, prev_pos, flushed: flushed.iter().map(|(d, p)| (store::hex(d), *p)).collect(), copied: ch.last_index };
    for d in &c.dirs {
        std::fs::create_dir_all(d)?;
        let mut seg = 1u32;
        let mut buf: Vec<u8> = Vec::with_capacity(1 << 20);
        let write_seg = |seg: u32, buf: &mut Vec<u8>| -> std::io::Result<()> {
            let mut f = std::fs::File::create(store::seg_path(d, seg))?;
            std::io::Write::write_all(&mut f, buf)?;
            f.sync_all()?;
            buf.clear();
            Ok(())
        };
        for e in &ch.entries {
            if !buf.is_empty() && (buf.len() + e.raw.len()) as u64 > c.seg_bytes {
                write_seg(seg, &mut buf)?;
                seg += 1;
            }
            buf.extend_from_slice(&e.raw);
        }
        if !buf.is_empty() {
            write_seg(seg, &mut buf)?;
        }
        store::write_atomic(d, "ckpt", &serde_json::to_vec(&ck)?)?;
        store::write_atomic(d, "state", &serde_json::to_vec(&RaftState { term: max_term, voted: -1 })?)?;
    }
    Ok(serde_json::json!({
        "ripristino": true, "manifest_letti": n_man, "database_nei_manifest": flushed.len(), "oggetti_elencati": objs.len(),
        "oggetti_usati": ch.objects_used, "oggetti_messi_da_parte": ch.objects_after_stop.len(), "arresto": ch.stop,
        "primo_indice": ch.first_index, "ultimo_indice": ch.last_index, "ultima_pos": ch.last_pos, "ultima_pos_ms": crate::codec::pos_ms(ch.last_pos),
        "voci": ch.entries.len(), "mb_letti": bytes as f64 / 1e6,
        "ms_manifest": t_man.as_millis() as u64, "ms_lettura": t_read.as_millis() as u64, "ms_totale": t0.elapsed().as_millis() as u64,
    }))
}

/// Controllo dopo il ripristino: ogni testimone confermato (file "n,posizione,ms della conferma") deve stare nella
/// cella ripristinata, nel quaderno (lettura dal capo alla posizione confermata) o negli SST dei manifest. Le
/// mancanti si dividono tra "nella finestra dichiarata" (confermate dopo `kill_ms - window_ms`) e "oltre" (violazione).
/// Con `gap_pos` > 0 (prova del buco) le scritture con posizione ≥ `gap_pos` che mancano, o che si leggono con una
/// versione più vecchia, sono "perse per il buco" (attese: il ripristino si ferma prima); quelle prima del buco devono
/// esserci tutte.
pub async fn check_zona(cl: Arc<crate::client::Cluster>, endpoint: &str, bucket: &str, groups: u64, witness: &str, kill_ms: u64, window_ms: u64, gap_pos: Pos) -> anyhow::Result<serde_json::Value> {
    use crate::codec::{GetRes, Req, Resp};
    use futures::StreamExt;
    let db = crate::bench::db_id(crate::bench::WITNESS_DB);
    let (sst, objects) = crate::flusher::sst_map(endpoint, bucket, groups, db).await?;
    let s = match cl.call_leader(&Req::Confirmed, Duration::from_secs(30)).await {
        Some(Resp::Pos(p)) => p,
        other => anyhow::bail!("posizione confermata: {other:?}"),
    };
    let mut ws = vec![];
    for l in std::fs::read_to_string(witness)?.lines() {
        let f: Vec<&str> = l.split(',').collect();
        if f.len() < 3 {
            continue;
        }
        ws.push((f[0].parse::<u64>()?, f[1].parse::<u64>()?, f[2].parse::<u64>()?));
    }
    let leader = cl.leader.load(Relaxed).max(0) as usize;
    let res: Vec<(u64, Pos, u64, u8)> = futures::stream::iter(ws.clone())
        .map(|(n, pos, t)| {
            let cl = cl.clone();
            let sst = &sst;
            async move {
                let key = crate::bench::doc_key(&format!("w-{n}"));
                let mut st = 9u8;
                for _ in 0..5 {
                    match cl.call(leader, &Req::Get { db, key: key.clone(), at: s, wait_ms: 1000, spec: false, dal: 0 }, Duration::from_secs(5)).await {
                        Ok(Resp::Get(GetRes::Value(p, v))) => {
                            let ok = p == pos && v.get(..8).map(|x| u64::from_le_bytes(x.try_into().unwrap())) == Some(n);
                            st = if ok { 0 } else { 3 };
                        }
                        Ok(Resp::Get(_)) => {
                            st = match sst.get(&key) {
                                Some((p, v)) if *p == pos && v.get(..8).map(|x| u64::from_le_bytes(x.try_into().unwrap())) == Some(n) => 1,
                                Some(_) => 3,
                                None => 2,
                            };
                        }
                        _ => {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            continue;
                        }
                    }
                    break;
                }
                (n, pos, t, st)
            }
        })
        .buffer_unordered(64)
        .collect()
        .await;
    let lim = kill_ms.saturating_sub(window_ms);
    let (mut in_q, mut in_sst, mut wrong, mut err) = (0u64, 0u64, 0u64, 0u64);
    let (mut lost_in, mut lost_beyond, mut lost_gap) = (0u64, 0u64, 0u64);
    let mut oldest_lost_ms_before_kill = 0u64;
    let mut newest_present_ms_before_kill = u64::MAX;
    let mut examples = vec![];
    for (n, pos, t, st) in &res {
        match st {
            0 | 1 => {
                if *st == 0 { in_q += 1 } else { in_sst += 1 }
                newest_present_ms_before_kill = newest_present_ms_before_kill.min(kill_ms.saturating_sub(*t));
            }
            2 | 3 if gap_pos > 0 && *pos >= gap_pos => lost_gap += 1,
            2 => {
                oldest_lost_ms_before_kill = oldest_lost_ms_before_kill.max(kill_ms.saturating_sub(*t));
                if *t <= lim {
                    lost_beyond += 1;
                    if examples.len() < 5 {
                        examples.push(format!("persa oltre la finestra: w-{n} pos {pos} confermata {} ms prima della morte", kill_ms.saturating_sub(*t)));
                    }
                } else {
                    lost_in += 1;
                }
            }
            3 => {
                wrong += 1;
                if examples.len() < 5 {
                    examples.push(format!("w-{n}: posizione o valore diversi (pos confermata {pos})"));
                }
            }
            _ => err += 1,
        }
    }
    let after_kill = ws.iter().filter(|w| w.2 > kill_ms).count();
    Ok(serde_json::json!({
        "check": "zona-ripristinata", "testimoni_confermati": ws.len(), "confermati_dopo_la_morte": after_kill,
        "nel_quaderno": in_q, "negli_sst": in_sst, "persi_nella_finestra": lost_in, "persi_oltre_la_finestra": lost_beyond,
        "doppi_o_spostati": wrong, "persi_per_il_buco": lost_gap, "buco_pos": gap_pos, "errori_di_lettura": err, "finestra_ms": window_ms,
        "piu_vecchio_perso_ms_prima_della_morte": oldest_lost_ms_before_kill,
        "piu_recente_presente_ms_prima_della_morte": if newest_present_ms_before_kill == u64::MAX { serde_json::Value::Null } else { newest_present_ms_before_kill.into() },
        "oggetti_sst_letti": objects, "posizione_letta": s, "esempi": examples,
        "soglia_S1": if lost_beyond == 0 && wrong == 0 && err == 0 { "PASSA" } else { "NON PASSA" },
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{Body, Kind, Write};

    fn ent(term: u64, i: u64) -> Entry {
        Entry::encode(term, i, 1000 + i, Body::Rec { txn: [i as u8; 16], db: [1; 16], session: None, writes: vec![Write { key: Bytes::from(format!("doc/{i}")), kind: Kind::Put, val: Bytes::from_static(b"v") }] })
    }
    fn obj(term: u64, a: u64, b: u64) -> Bytes {
        let v: Vec<Entry> = (a..=b).map(|i| ent(term, i)).collect();
        let h = Head { start: a, end: b, prev_term: term, prev_pos: 1000 + a - 1, start_pos: 1000 + a, end_pos: 1000 + b, n: v.len() as u32 };
        encode(&h, &v.iter().map(|e| e.raw.clone()).collect::<Vec<_>>())
    }

    #[test]
    fn chain_whole() {
        let m: BTreeMap<u64, Bytes> = [(1, obj(1, 1, 10)), (11, obj(1, 11, 20)), (21, obj(2, 21, 25))].into_iter().collect();
        let c = chain("c", &m);
        assert_eq!((c.first_index, c.last_index, c.entries.len(), c.objects_used), (1, 25, 25, 3));
        assert_eq!(c.stop, "fine");
        assert!(c.objects_after_stop.is_empty());
    }

    #[test]
    fn chain_stops_at_gap() {
        // manca 11..20: 21..25 e 26..30 non vanno applicati
        let m: BTreeMap<u64, Bytes> = [(1, obj(1, 1, 10)), (21, obj(1, 21, 25)), (26, obj(1, 26, 30))].into_iter().collect();
        let c = chain("c", &m);
        assert_eq!(c.last_index, 10);
        assert_eq!(c.entries.len(), 10);
        assert!(c.stop.starts_with("buco a 11"), "{}", c.stop);
        assert_eq!(c.objects_after_stop, vec!["c/quaderno/0000000000000015", "c/quaderno/000000000000001a"]);
    }

    #[test]
    fn chain_stops_at_corrupt() {
        let mut bad = obj(1, 11, 20).to_vec();
        let n = bad.len();
        bad[n / 2] ^= 0xff;
        let m: BTreeMap<u64, Bytes> = [(1, obj(1, 1, 10)), (11, Bytes::from(bad)), (21, obj(1, 21, 25))].into_iter().collect();
        let c = chain("c", &m);
        assert_eq!(c.last_index, 10);
        assert!(c.stop.contains("CRC"), "{}", c.stop);
        assert_eq!(c.objects_after_stop.len(), 2);
    }

    #[test]
    fn chain_stops_at_term_going_down() {
        let m: BTreeMap<u64, Bytes> = [(1, obj(3, 1, 10)), (11, obj(2, 11, 20))].into_iter().collect();
        let c = chain("c", &m);
        assert_eq!(c.last_index, 10);
    }

    #[test]
    fn head_roundtrip() {
        let b = obj(4, 7, 9);
        let (h, v) = decode(&b).unwrap();
        assert_eq!((h.start, h.end, h.prev_term, h.n, v.len()), (7, 9, 4, 3, 3));
        assert_eq!(decode_head(&b[..HDR]).unwrap(), h);
        assert_eq!(parse_key("cella", &key("cella", 255)), Some(255));
    }

    #[test]
    fn put_existing_continues_from_its_end() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let st = object_store::memory::InMemory::new();
        rt.block_on(async {
            let b = obj(1, 1, 10);
            let (h, _) = decode(&b).unwrap();
            let c = put_once(&st, "c", &h, b.clone()).await.unwrap();
            assert!(!c.existed);
            // un altro capo prova a scrivere lo stesso inizio con un tratto più corto: trova quello esistente
            let b2 = obj(1, 1, 5);
            let (h2, _) = decode(&b2).unwrap();
            let c2 = put_once(&st, "c", &h2, b2).await.unwrap();
            assert!(c2.existed);
            assert_eq!(c2.end, 10);
            let l = list_heads(&st, "c").await.unwrap();
            assert_eq!(l.get(&1), Some(&(10, 1010)));
        });
    }
}
