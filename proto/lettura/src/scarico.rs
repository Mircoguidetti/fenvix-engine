//! Scarico e compattazione "da chiunque": un worker senza stato durevole proprio. Più worker possono lavorare sugli
//! stessi gruppi: l'unico punto di accordo è il PUT condizionale del manifest.
//!
//! Scarico di un gruppo:
//! 1. GET manifest (etag E). Se il manifest è più avanti del registro (un worker è morto tra manifest e conferma),
//!    si ripara: `conferma_scarico(db, manifest.scaricato, E)`.
//! 2. `scaricabili(dbs)`; si tengono solo i record con posizione > `manifest.scaricato[db]` (niente doppioni).
//! 3. PUT dell'SST con un nome nuovo (mai sovrascritto).
//! 4. PUT condizionale del manifest (If-Match E) con la run nuova in testa e `scaricato` avanzato.
//!    - riuscito (etag E2): `conferma_scarico(db, fino_a, E2)`;
//!    - 412: si rilegge il manifest; se contiene la nostra run (risposta persa e ritentata) è un successo, altrimenti
//!      ha vinto un altro: il nostro SST si cancella subito (nessuno lo può referenziare);
//!    - esito incerto: l'SST resta; se non entra nel manifest è un orfano e lo toglie la pulizia degli orfani, solo
//!      quando è più vecchio di `orfani_eta_ms` (nessun PUT in volo dura tanto).
//!
//! Compattazione: run adiacenti nell'elenco (stessa classe di dimensione, o tutte quando il gruppo è a riposo) si
//! fondono in una; il manifest le sostituisce con un PUT condizionale (se nel frattempo è cambiato si rifà solo la
//! sostituzione, purché le run d'ingresso ci siano ancora). Le run tolte vanno in `da_cancellare` e si cancellano dopo
//! `grazia_ms`.

use crate::iface::*;
use crate::manifest::{self, gruppo_di, DaCancellare, Manifest, Raggruppa, Run};
use crate::sst::{self, Entry};
use crate::store::{Cas, Store};
use bytes::Bytes;
use futures::StreamExt;
use parking_lot::Mutex;
use rand::Rng;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct CfgScarico {
    pub raggruppa: Raggruppa,
    /// si scarica un gruppo quando il suo record più vecchio non su S3 ha questa età...
    pub eta_ms: u64,
    /// ...o quando ha almeno questi byte da scaricare
    pub dim_byte: u64,
    /// byte massimi per SST di scarico
    pub max_batch: u64,
    pub blocco: usize,
    pub intervallo_ms: u64,
    pub paralleli: usize,
    pub compatta: bool,
    /// run adiacenti della stessa classe da fondere
    pub compatta_k: usize,
    /// oltre questo numero di run si fondono le più recenti
    pub max_run: usize,
    /// gruppo senza scarichi da tanto: si fonde tutto in una run (0 = mai)
    pub riposo_ms: u64,
    /// tiene tutte le versioni (verifica del passo 1); altrimenti tiene quelle più nuove dell'orizzonte
    pub tieni_tutte: bool,
    pub orizzonte_ms: u64,
    pub grazia_ms: u64,
    pub orfani_ogni_ms: u64,
    pub orfani_eta_ms: u64,
    /// probabilità di abort() in ogni punto delicato (prova di uccisione)
    pub crash: f64,
    /// cache del worker per gli SST appena scritti o letti (evita GET in compattazione)
    pub cache_byte: u64,
    /// SOLO controllo negativo del banco: manifest scritto senza condizione (deve far fallire la verifica)
    pub senza_cas: bool,
}

impl Default for CfgScarico {
    fn default() -> Self {
        CfgScarico {
            raggruppa: Raggruppa::PerDb,
            eta_ms: 1000,
            dim_byte: 8 << 20,
            max_batch: 16 << 20,
            blocco: 16 << 10,
            intervallo_ms: 100,
            paralleli: 8,
            compatta: true,
            compatta_k: 4,
            max_run: 12,
            riposo_ms: 0,
            tieni_tutte: false,
            orizzonte_ms: 120_000,
            grazia_ms: 60_000,
            orfani_ogni_ms: 600_000,
            orfani_eta_ms: 300_000,
            crash: 0.0,
            cache_byte: 256 << 20,
            senza_cas: false,
        }
    }
}

#[derive(Default, Debug)]
pub struct StatScarico {
    pub scarichi: AtomicU64,
    pub record: AtomicU64,
    pub byte_sst: AtomicU64,
    pub conflitti: AtomicU64,
    pub conflitti_vinti_dopo: AtomicU64,
    pub incerti: AtomicU64,
    pub riparazioni: AtomicU64,
    pub compattazioni: AtomicU64,
    pub compatta_in: AtomicU64,
    pub compatta_out: AtomicU64,
    pub compatta_rifatte: AtomicU64,
    pub cancellati: AtomicU64,
    pub orfani: AtomicU64,
    pub errori: AtomicU64,
    pub ns_scarico: AtomicU64,
    pub ns_compatta: AtomicU64,
}

#[derive(Serialize, Debug, Default, Clone)]
pub struct StatScaricoV {
    pub scarichi: u64,
    pub record: u64,
    pub byte_sst: u64,
    pub conflitti: u64,
    pub conflitti_vinti_dopo: u64,
    pub incerti: u64,
    pub riparazioni: u64,
    pub compattazioni: u64,
    pub compatta_in: u64,
    pub compatta_out: u64,
    pub compatta_rifatte: u64,
    pub cancellati: u64,
    pub orfani: u64,
    pub errori: u64,
}

impl StatScarico {
    pub fn leggi(&self) -> StatScaricoV {
        StatScaricoV {
            scarichi: self.scarichi.load(Relaxed),
            record: self.record.load(Relaxed),
            byte_sst: self.byte_sst.load(Relaxed),
            conflitti: self.conflitti.load(Relaxed),
            conflitti_vinti_dopo: self.conflitti_vinti_dopo.load(Relaxed),
            incerti: self.incerti.load(Relaxed),
            riparazioni: self.riparazioni.load(Relaxed),
            compattazioni: self.compattazioni.load(Relaxed),
            compatta_in: self.compatta_in.load(Relaxed),
            compatta_out: self.compatta_out.load(Relaxed),
            compatta_rifatte: self.compatta_rifatte.load(Relaxed),
            cancellati: self.cancellati.load(Relaxed),
            orfani: self.orfani.load(Relaxed),
            errori: self.errori.load(Relaxed),
        }
    }
}

struct Noto {
    ultimo_scarico_ms: u64,
    compattato_a_riposo: bool,
}

pub struct Worker {
    pub reg: Arc<dyn Registro>,
    pub st: Store,
    pub cfg: CfgScarico,
    pub stat: Arc<StatScarico>,
    cache: Mutex<(lru::LruCache<String, Bytes>, u64)>,
    noti: Mutex<HashMap<String, Noto>>,
    /// ultimo manifest scritto o letto da questo worker, con il suo etag: lo scarico successivo parte da qui senza GET
    /// (se nel frattempo un altro l'ha cambiato, il PUT condizionale fallisce e si rilegge)
    mcache: Mutex<HashMap<String, (Manifest, String)>>,
    occupati: Mutex<std::collections::HashSet<String>>,
}

fn crash(p: f64, _dove: &str) {
    if p > 0.0 && rand::thread_rng().gen::<f64>() < p {
        std::process::abort();
    }
}

fn classe(dim: u64) -> u32 {
    let mut c = 0;
    let mut x = dim.max(1) / (64 << 10);
    while x >= 4 {
        x /= 4;
        c += 1;
    }
    c
}

fn nuovo_id(max: Pos, suffisso: &str) -> String {
    format!("{:016x}-{:08x}{}", max, rand::thread_rng().gen::<u32>(), suffisso)
}

/// Voci di un insieme di record, in ordine SST. Se un record scrive due volte la stessa chiave vale l'ultima.
pub fn voci_da_record(recs: &[(Db, Vec<Record>)]) -> Vec<Entry> {
    let mut es = Vec::new();
    for (db, rs) in recs {
        for r in rs {
            for w in r.writes.iter().rev() {
                es.push(Entry { key: sst::full_key(db, &w.key), pos: r.pos, kind: w.kind, val: w.val.clone() });
            }
        }
    }
    es.sort_by(|a, b| a.key.cmp(&b.key).then(b.pos.cmp(&a.pos)));
    es.dedup_by(|a, b| a.key == b.key && a.pos == b.pos);
    es
}

/// Fonde voci di run con posizioni disgiunte; toglie le versioni più vecchie dell'orizzonte (ne resta una per
/// chiave) e, se la fusione include la run più vecchia, le cancellazioni più vecchie dell'orizzonte.
pub fn fondi(mut es: Vec<Entry>, orizzonte: Option<Pos>, fondo: bool) -> Vec<Entry> {
    es.sort_by(|a, b| a.key.cmp(&b.key).then(b.pos.cmp(&a.pos)));
    let Some(oriz) = orizzonte else { return es };
    let mut out = Vec::with_capacity(es.len());
    let mut i = 0;
    while i < es.len() {
        let mut j = i;
        while j < es.len() && es[j].key == es[i].key {
            j += 1;
        }
        let mut base_vista = false;
        let mut delta_vista = false;
        for e in &es[i..j] {
            if e.pos > oriz {
                out.push(e.clone());
                delta_vista |= e.kind == Kind::Delta;
                continue;
            }
            if base_vista {
                break;
            }
            // prima versione <= orizzonte: è quella che vede chiunque legga dall'orizzonte in poi
            match e.kind {
                Kind::Delta => {
                    out.push(e.clone());
                    delta_vista = true;
                }
                Kind::Del if fondo && !delta_vista => {
                    base_vista = true;
                }
                _ => {
                    out.push(e.clone());
                    base_vista = true;
                }
            }
        }
        i = j;
    }
    out
}

impl Worker {
    pub fn new(reg: Arc<dyn Registro>, st: Store, cfg: CfgScarico) -> Arc<Worker> {
        Arc::new(Worker {
            reg,
            st,
            cfg,
            stat: Arc::new(StatScarico::default()),
            cache: Mutex::new((lru::LruCache::unbounded(), 0)),
            noti: Mutex::new(HashMap::new()),
            mcache: Mutex::new(HashMap::new()),
            occupati: Mutex::new(Default::default()),
        })
    }

    fn cache_put(&self, id: &str, b: Bytes) {
        if self.cfg.cache_byte == 0 {
            return;
        }
        let mut g = self.cache.lock();
        g.1 += b.len() as u64;
        g.0.put(id.to_string(), b);
        while g.1 > self.cfg.cache_byte {
            match g.0.pop_lru() {
                Some((_, v)) => g.1 -= v.len() as u64,
                None => break,
            }
        }
    }

    async fn leggi_run(&self, gruppo: &str, r: &Run) -> anyhow::Result<Bytes> {
        let k = format!("{gruppo}.{}", r.id);
        if let Some(b) = self.cache.lock().0.get(&k).cloned() {
            return Ok(b);
        }
        let (b, _) = self.st.get(&manifest::path_sst(gruppo, &r.id)).await?.ok_or_else(|| anyhow::anyhow!("run {} sparita", r.id))?;
        self.cache_put(&k, b.clone());
        Ok(b)
    }

    /// Un SST scritto da troppo tempo non si mette più nel manifest: la pulizia degli orfani lo potrebbe aver già
    /// cancellato (worker fermo a lungo tra i due PUT). Resta orfano e lo toglie la pulizia.
    fn in_tempo(&self, t_put: std::time::Instant) -> anyhow::Result<()> {
        if self.cfg.orfani_ogni_ms > 0 && t_put.elapsed().as_millis() as u64 > self.cfg.orfani_eta_ms / 2 {
            self.stat.incerti.fetch_add(1, Relaxed);
            anyhow::bail!("SST scritto troppo tempo fa, lasciato alla pulizia degli orfani");
        }
        Ok(())
    }

    /// Un giro: scarica i gruppi pronti, poi compatta e pulisce dove serve.
    pub async fn giro(self: &Arc<Self>) -> anyhow::Result<()> {
        let pend = self.reg.pendenti(1_000_000).await?;
        let now = now_ms();
        let mut gruppi: BTreeMap<String, (Vec<Pendente>, Pos, u64)> = BTreeMap::new();
        for p in &pend {
            let g = gruppi.entry(gruppo_di(&p.db, self.cfg.raggruppa)).or_insert((Vec::new(), u64::MAX, 0));
            g.0.push(p.clone());
            g.1 = g.1.min(p.piu_vecchio);
            g.2 += p.byte;
        }
        let mut pronti: Vec<(String, Vec<Pendente>)> = gruppi
            .into_iter()
            .filter(|(_, (_, vecchio, byte))| now.saturating_sub(pos_ms(*vecchio)) >= self.cfg.eta_ms || *byte >= self.cfg.dim_byte)
            .map(|(g, (d, _, _))| (g, d))
            .collect();
        {
            use rand::seq::SliceRandom;
            pronti.shuffle(&mut rand::thread_rng());
        }
        let me = self.clone();
        futures::stream::iter(pronti)
            .for_each_concurrent(self.cfg.paralleli.max(1), |(g, dbs)| {
                let me = me.clone();
                async move {
                    if !me.occupati.lock().insert(g.clone()) {
                        return;
                    }
                    let t0 = std::time::Instant::now();
                    let r = me.scarica(&g, dbs).await;
                    me.stat.ns_scarico.fetch_add(t0.elapsed().as_nanos() as u64, Relaxed);
                    match r {
                        Ok(Some(m)) => {
                            me.noti.lock().insert(g.clone(), Noto { ultimo_scarico_ms: now_ms(), compattato_a_riposo: false });
                            if me.cfg.compatta {
                                let t0 = std::time::Instant::now();
                                if let Err(e) = me.compatta(&g, Some(m), false).await {
                                    me.stat.errori.fetch_add(1, Relaxed);
                                    eprintln!("compatta {g}: {e:#}");
                                }
                                me.stat.ns_compatta.fetch_add(t0.elapsed().as_nanos() as u64, Relaxed);
                            }
                        }
                        Ok(None) => {}
                        Err(e) => {
                            me.stat.errori.fetch_add(1, Relaxed);
                            eprintln!("scarica {g}: {e:#}");
                        }
                    }
                    me.occupati.lock().remove(&g);
                }
            })
            .await;
        // gruppi a riposo: una compattazione completa; pulizia degli orfani
        if self.cfg.compatta && self.cfg.riposo_ms > 0 {
            let riposo: Vec<String> = self
                .noti
                .lock()
                .iter()
                .filter(|(_, n)| !n.compattato_a_riposo && now.saturating_sub(n.ultimo_scarico_ms) >= self.cfg.riposo_ms)
                .map(|(g, _)| g.clone())
                .collect();
            let me = self.clone();
            futures::stream::iter(riposo)
                .for_each_concurrent(self.cfg.paralleli.max(1), |g| {
                    let me = me.clone();
                    async move {
                        if !me.occupati.lock().insert(g.clone()) {
                            return;
                        }
                        // il riposo si decide dal manifest: lo scarico può averlo fatto un altro worker
                        let m = match manifest::carica(&me.st, &g).await {
                            Ok(Some(x)) => x,
                            _ => {
                                me.occupati.lock().remove(&g);
                                return;
                            }
                        };
                        if now_ms().saturating_sub(m.0.ultimo_scarico_ms) < me.cfg.riposo_ms {
                            if let Some(n) = me.noti.lock().get_mut(&g) {
                                n.ultimo_scarico_ms = m.0.ultimo_scarico_ms;
                            }
                            me.occupati.lock().remove(&g);
                            return;
                        }
                        match me.compatta(&g, Some(m), true).await {
                            Ok(_) => {
                                if let Some(n) = me.noti.lock().get_mut(&g) {
                                    n.compattato_a_riposo = true;
                                }
                            }
                            Err(e) => {
                                me.stat.errori.fetch_add(1, Relaxed);
                                eprintln!("compatta a riposo {g}: {e:#}");
                            }
                        }
                        me.occupati.lock().remove(&g);
                    }
                })
                .await;
        }
        Ok(())
    }

    pub async fn corri(self: Arc<Self>) {
        loop {
            if let Err(e) = self.giro().await {
                self.stat.errori.fetch_add(1, Relaxed);
                eprintln!("giro: {e:#}");
            }
            tokio::time::sleep(std::time::Duration::from_millis(self.cfg.intervallo_ms)).await;
        }
    }

    /// Scarica un gruppo. Some(manifest scritto) se ha scritto una run nuova.
    async fn manifest_corrente(&self, g: &str) -> anyhow::Result<(Manifest, Option<String>)> {
        if let Some((m, e)) = self.mcache.lock().get(g).cloned() {
            return Ok((m, Some(e)));
        }
        Ok(match manifest::carica(&self.st, g).await? {
            Some((m, e)) => (m, Some(e)),
            None => (Manifest::default(), None),
        })
    }

    fn ricorda(&self, g: &str, m: &Manifest, etag: &str) {
        let mut c = self.mcache.lock();
        if c.len() > 100_000 {
            c.clear();
        }
        c.insert(g.to_string(), (m.clone(), etag.to_string()));
    }

    pub async fn scarica(&self, g: &str, pend: Vec<Pendente>) -> anyhow::Result<Option<(Manifest, String)>> {
        let (m, etag) = self.manifest_corrente(g).await?;
        let dbs: Vec<Db> = pend.iter().map(|p| p.db).collect();
        // riparazione: manifest più avanti del registro (worker morto tra manifest e conferma)
        let mut ripara = Vec::new();
        for p in &pend {
            let s = m.scaricato_db(&p.db);
            if s > p.scaricato {
                ripara.push((p.db, s));
            }
        }
        if !ripara.is_empty() {
            self.stat.riparazioni.fetch_add(ripara.len() as u64, Relaxed);
            self.reg.conferma_scarico(ripara, etag.clone().unwrap_or_default()).await?;
        }
        let fino_a = self.reg.posizione_confermata().await?;
        let mut got = self.reg.scaricabili(dbs, fino_a, self.cfg.max_batch).await?;
        for (db, rs) in got.iter_mut() {
            let s = m.scaricato_db(db);
            rs.retain(|r| r.pos > s);
        }
        got.retain(|(_, rs)| !rs.is_empty());
        if got.is_empty() {
            return Ok(None);
        }
        let nrec: u64 = got.iter().map(|(_, r)| r.len() as u64).sum();
        let voci_conf: Vec<(Db, Pos)> = got.iter().map(|(db, rs)| (*db, rs.last().unwrap().pos)).collect();
        let es = voci_da_record(&got);
        drop(got);
        let b = sst::build(es, self.cfg.blocco);
        let id = nuovo_id(b.max, "");
        let data = Bytes::from(b.data);
        let t_put = std::time::Instant::now();
        self.st.put(&manifest::path_sst(g, &id), data.clone()).await?;
        crash(self.cfg.crash, "dopo_sst");
        self.in_tempo(t_put)?;
        let run = Run { id: id.clone(), min: b.min, max: b.max, dim: data.len() as u64, meta_off: b.meta_off, meta_len: b.meta_len, voci: b.voci };
        let now = now_ms();
        let mut m2 = m.clone();
        m2.versione += 1;
        m2.run.insert(0, run);
        for (db, p) in &voci_conf {
            m2.scaricato.insert(db_hex(db), *p);
        }
        m2.ultimo_scarico_ms = now;
        let scaduti: Vec<String> = m2.da_cancellare.iter().filter(|d| d.dal_ms + self.cfg.grazia_ms <= now).map(|d| d.id.clone()).collect();
        m2.da_cancellare.retain(|d| d.dal_ms + self.cfg.grazia_ms > now);
        let esito = if self.cfg.senza_cas {
            match self.st.put_etag(&manifest::path_manifest(g), Bytes::from(serde_json::to_vec(&m2)?)).await {
                Ok(e) => Cas::Ok(e),
                Err(e) => Cas::Incerto(e.to_string()),
            }
        } else {
            manifest::scrivi(&self.st, g, &m2, etag.as_deref()).await
        };
        match esito {
            Cas::Ok(e2) => {
                crash(self.cfg.crash, "dopo_manifest");
                self.ricorda(g, &m2, &e2);
                self.reg.conferma_scarico(voci_conf, e2.clone()).await?;
                crash(self.cfg.crash, "dopo_conferma");
                self.stat.scarichi.fetch_add(1, Relaxed);
                self.stat.record.fetch_add(nrec, Relaxed);
                self.stat.byte_sst.fetch_add(data.len() as u64, Relaxed);
                self.cache_put(&format!("{g}.{id}"), data);
                for s in scaduti {
                    self.st.delete(&manifest::path_sst(g, &s)).await?;
                    self.stat.cancellati.fetch_add(1, Relaxed);
                }
                self.forse_orfani(g, &m2).await;
                Ok(Some((m2, e2)))
            }
            Cas::Conflitto { .. } => {
                self.stat.conflitti.fetch_add(1, Relaxed);
                self.mcache.lock().remove(g);
                // la nostra scrittura può essere riuscita con la risposta persa (ritentativo che vede 412)
                if let Some((m3, e3)) = manifest::carica(&self.st, g).await? {
                    self.ricorda(g, &m3, &e3);
                    if m3.run.iter().any(|r| r.id == id) {
                        self.stat.conflitti_vinti_dopo.fetch_add(1, Relaxed);
                        self.reg.conferma_scarico(voci_conf, e3.clone()).await?;
                        self.stat.scarichi.fetch_add(1, Relaxed);
                        return Ok(Some((m3, e3)));
                    }
                }
                self.st.delete(&manifest::path_sst(g, &id)).await?;
                Ok(None)
            }
            Cas::Incerto(e) => {
                self.stat.incerti.fetch_add(1, Relaxed);
                self.mcache.lock().remove(g);
                anyhow::bail!("manifest incerto: {e}")
            }
        }
    }

    /// Compatta un gruppo se serve (o tutto, `tutto`). Ok(true) se ha sostituito delle run.
    pub async fn compatta(&self, g: &str, m: Option<(Manifest, String)>, tutto: bool) -> anyhow::Result<bool> {
        let (m, etag0) = match m {
            Some(x) => x,
            None => match manifest::carica(&self.st, g).await? {
                Some(x) => x,
                None => return Ok(false),
            },
        };
        let n = m.run.len();
        let (a, z) = if tutto {
            if n <= 1 {
                return Ok(false);
            }
            (0, n)
        } else if n > self.cfg.max_run {
            // troppe run: si fonde la finestra di k run adiacenti meno costosa (la più piccola in byte), così una run
            // grande non si riscrive a ogni giro
            let k = self.cfg.compatta_k.max(2).min(n);
            let mut best = (u64::MAX, 0);
            for i in 0..=n - k {
                let c: u64 = m.run[i..i + k].iter().map(|r| r.dim).sum();
                if c < best.0 {
                    best = (c, i);
                }
            }
            (best.1, best.1 + k)
        } else {
            // gruppo più recente di run adiacenti della stessa classe con almeno k elementi
            let mut i = 0;
            let mut scelta = None;
            while i < n {
                let c = classe(m.run[i].dim);
                let mut j = i;
                while j < n && classe(m.run[j].dim) == c {
                    j += 1;
                }
                if j - i >= self.cfg.compatta_k.max(2) {
                    scelta = Some((i, j));
                    break;
                }
                i = j;
            }
            match scelta {
                Some(x) => x,
                None => return Ok(false),
            }
        };
        let z = z.min(n);
        if z - a < 2 {
            return Ok(false);
        }
        let ingressi: Vec<Run> = m.run[a..z].to_vec();
        let fondo = z == n;
        let mut es = Vec::new();
        let mut byte_in = 0u64;
        for r in &ingressi {
            let b = self.leggi_run(g, r).await?;
            byte_in += b.len() as u64;
            es.extend(sst::all_entries(&b)?);
        }
        let oriz = if self.cfg.tieni_tutte { None } else { Some(pos_make(now_ms().saturating_sub(self.cfg.orizzonte_ms), 0)) };
        let es = fondi(es, oriz, fondo);
        let b = sst::build(es, self.cfg.blocco);
        let id = nuovo_id(b.max, "c");
        let data = Bytes::from(b.data);
        let t_put = std::time::Instant::now();
        self.st.put(&manifest::path_sst(g, &id), data.clone()).await?;
        crash(self.cfg.crash, "compatta_dopo_sst");
        let nuova = Run { id: id.clone(), min: b.min, max: b.max, dim: data.len() as u64, meta_off: b.meta_off, meta_len: b.meta_len, voci: b.voci };
        let ids: Vec<&str> = ingressi.iter().map(|r| r.id.as_str()).collect();
        let mut primo = Some((m.clone(), etag0));
        for tentativo in 0..8 {
            let (mut cur, etag) = match primo.take() {
                Some(x) => x,
                None => match manifest::carica(&self.st, g).await? {
                    Some(x) => x,
                    None => break,
                },
            };
            // le run d'ingresso devono esserci ancora, adiacenti e nello stesso ordine
            let Some(p) = cur.run.iter().position(|r| r.id == ids[0]) else { break };
            if cur.run.len() < p + ids.len() || cur.run[p..p + ids.len()].iter().map(|r| r.id.as_str()).ne(ids.iter().copied()) {
                break;
            }
            let fondo_ora = p + ids.len() == cur.run.len();
            if fondo && !fondo_ora {
                // non succede (le run nuove entrano in testa), ma la fusione delle cancellazioni lo presuppone
                break;
            }
            if tentativo > 0 {
                self.stat.compatta_rifatte.fetch_add(1, Relaxed);
            }
            self.in_tempo(t_put)?;
            let now = now_ms();
            cur.run.splice(p..p + ids.len(), std::iter::once(nuova.clone()));
            cur.versione += 1;
            let scaduti: Vec<String> = cur.da_cancellare.iter().filter(|d| d.dal_ms + self.cfg.grazia_ms <= now).map(|d| d.id.clone()).collect();
            cur.da_cancellare.retain(|d| d.dal_ms + self.cfg.grazia_ms > now);
            for r in &ingressi {
                cur.da_cancellare.push(DaCancellare { id: r.id.clone(), dal_ms: now });
            }
            match manifest::scrivi(&self.st, g, &cur, Some(&etag)).await {
                Cas::Ok(e2) => {
                    crash(self.cfg.crash, "compatta_dopo_manifest");
                    self.ricorda(g, &cur, &e2);
                    for s in scaduti {
                        self.st.delete(&manifest::path_sst(g, &s)).await?;
                        self.stat.cancellati.fetch_add(1, Relaxed);
                    }
                    self.stat.compattazioni.fetch_add(1, Relaxed);
                    self.stat.compatta_in.fetch_add(byte_in, Relaxed);
                    self.stat.compatta_out.fetch_add(data.len() as u64, Relaxed);
                    self.cache_put(&format!("{g}.{id}"), data);
                    return Ok(true);
                }
                Cas::Conflitto { .. } => {
                    self.mcache.lock().remove(g);
                    if let Some((m3, _)) = manifest::carica(&self.st, g).await? {
                        if m3.run.iter().any(|r| r.id == id) {
                            self.stat.compattazioni.fetch_add(1, Relaxed);
                            return Ok(true);
                        }
                    }
                    continue;
                }
                Cas::Incerto(e) => {
                    self.mcache.lock().remove(g);
                    anyhow::bail!("manifest incerto in compattazione: {e}")
                }
            }
        }
        // ha vinto qualcun altro: la nostra run non è referenziata da nessuno
        self.st.delete(&manifest::path_sst(g, &id)).await?;
        Ok(false)
    }

    /// Pulizia degli orfani (SST scritti da worker morti prima del manifest), al massimo ogni `orfani_ogni_ms`.
    async fn forse_orfani(&self, g: &str, m: &Manifest) {
        // il manifest in memoria potrebbe cambiare sotto: la pulizia rilegge da S3
        let now = now_ms();
        if self.cfg.orfani_ogni_ms == 0 || now.saturating_sub(m.orfani_ms) < self.cfg.orfani_ogni_ms {
            return;
        }
        if let Err(e) = self.orfani(g).await {
            self.stat.errori.fetch_add(1, Relaxed);
            eprintln!("orfani {g}: {e:#}");
        }
    }

    pub async fn orfani(&self, g: &str) -> anyhow::Result<u64> {
        let Some((mut m, etag)) = manifest::carica(&self.st, g).await? else { return Ok(0) };
        let now = now_ms();
        let pref = format!("{}sst/", manifest::prefisso(g));
        let lista = self.st.list(&pref).await?;
        let noti: std::collections::HashSet<&str> = m.run.iter().map(|r| r.id.as_str()).chain(m.da_cancellare.iter().map(|d| d.id.as_str())).collect();
        let mut n = 0;
        for o in &lista {
            let id = o.location.filename().unwrap_or("").trim_end_matches(".sst").to_string();
            let eta = now.saturating_sub(o.last_modified.timestamp_millis().max(0) as u64);
            if !noti.contains(id.as_str()) && eta >= self.cfg.orfani_eta_ms {
                self.st.delete(o.location.as_ref()).await?;
                n += 1;
            }
        }
        self.stat.orfani.fetch_add(n, Relaxed);
        m.orfani_ms = now;
        m.versione += 1;
        match manifest::scrivi(&self.st, g, &m, Some(&etag)).await {
            Cas::Ok(e2) => self.ricorda(g, &m, &e2),
            _ => {
                self.mcache.lock().remove(g);
            }
        }
        Ok(n)
    }
}

#[cfg(test)]
mod t {
    use super::*;
    fn e(k: &str, pos: Pos, kind: Kind) -> Entry {
        Entry { key: Bytes::from(k.to_string()), pos, kind, val: Bytes::from(format!("{k}{pos}")) }
    }
    #[test]
    fn taglio_versioni() {
        let es = vec![e("a", 10, Kind::Put), e("a", 20, Kind::Put), e("a", 30, Kind::Put), e("b", 5, Kind::Del), e("c", 8, Kind::Put), e("c", 9, Kind::Del)];
        // orizzonte 25: per "a" restano 30 e 20 (la più nuova sotto l'orizzonte); "b" e "c" cancellati sotto
        // l'orizzonte spariscono solo se la fusione include la run più vecchia
        let r = fondi(es.clone(), Some(25), true);
        let v: Vec<(String, Pos)> = r.iter().map(|x| (String::from_utf8(x.key.to_vec()).unwrap(), x.pos)).collect();
        assert_eq!(v, vec![("a".into(), 30), ("a".into(), 20)]);
        let r = fondi(es.clone(), Some(25), false);
        let v: Vec<(String, Pos)> = r.iter().map(|x| (String::from_utf8(x.key.to_vec()).unwrap(), x.pos)).collect();
        assert_eq!(v, vec![("a".into(), 30), ("a".into(), 20), ("b".into(), 5), ("c".into(), 9)]);
        assert_eq!(fondi(es.clone(), None, true).len(), es.len());
    }
}
