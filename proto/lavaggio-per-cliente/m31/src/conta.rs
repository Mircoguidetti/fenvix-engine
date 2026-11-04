//! Il magazzino della prova M31: avvolge un ObjectStore (disco locale, S3 compatibile in locale, S3 vero) e
//! - conta ogni richiesta (come M29: GET per scatola, byte, PUT);
//! - misura il tempo di ogni GET (dall'invio all'ultimo byte);
//! - può aggiungere un ritardo finto per GET che imita S3 (lognormale per il primo byte + byte / banda);
//! - può servire le scatole delle "pile sul bancone" da una copia vicina (disco locale) senza andare al magazzino;
//! - può mandare una seconda GET uguale se la prima tarda (richiesta doppia, hedged request), e conta le GET in più.

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{BoxStream, StreamExt};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result,
};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------- freno (revisore M31, C7 / R2)
// Contatori di tutto il processo, mai azzerati: se le richieste a S3 passano il tetto (un errore che le fa
// esplodere), il programma si ferma subito con il codice 97 e la riga "FRENO". esegui.sh allora salta alla fine.
// Vale solo quando il magazzino è S3 (`accendi_freno`), non per il disco locale. Tetti da M31_FRENO_GET e
// M31_FRENO_PUT, di serie 500.000 GET (0,20 $) e 100.000 PUT (0,50 $) per processo.
static FRENO_ACCESO: AtomicBool = AtomicBool::new(false);
static FRENO_GET: AtomicU64 = AtomicU64::new(0);
static FRENO_PUT: AtomicU64 = AtomicU64::new(0);
static TETTO_GET: AtomicU64 = AtomicU64::new(500_000);
static TETTO_PUT: AtomicU64 = AtomicU64::new(100_000);
pub const CODICE_FRENO: i32 = 97;

pub fn accendi_freno() {
    let leggi = |k: &str, d: u64| std::env::var(k).ok().and_then(|x| x.parse().ok()).unwrap_or(d);
    TETTO_GET.store(leggi("M31_FRENO_GET", 500_000), Relaxed);
    TETTO_PUT.store(leggi("M31_FRENO_PUT", 100_000), Relaxed);
    FRENO_ACCESO.store(true, Relaxed);
}

/// Conta una richiesta di classe GET (`get`) o PUT/LIST (`!get`) e ferma il processo oltre il tetto.
pub fn freno(get: bool) {
    if !FRENO_ACCESO.load(Relaxed) {
        return;
    }
    let (n, tetto, nome) = if get {
        (FRENO_GET.fetch_add(1, Relaxed) + 1, TETTO_GET.load(Relaxed), "GET")
    } else {
        (FRENO_PUT.fetch_add(1, Relaxed) + 1, TETTO_PUT.load(Relaxed), "PUT")
    };
    if n > tetto {
        eprintln!("FRENO: {n} richieste {nome} a S3 in questo processo, oltre il tetto {tetto}: mi fermo");
        std::process::exit(CODICE_FRENO);
    }
}

#[derive(Default, Debug)]
pub struct Contatori {
    pub get_intera: AtomicU64,
    pub get_range: AtomicU64,
    pub head: AtomicU64,
    pub get_byte: AtomicU64,
    pub put: AtomicU64,
    pub put_byte: AtomicU64,
    /// Solo le scatole (`compacted/`): PUT (parti comprese) e byte.
    pub put_sst: AtomicU64,
    pub put_sst_byte: AtomicU64,
    /// GET sulle scatole e byte (il lavaggio legge da qui).
    pub get_sst: AtomicU64,
    pub get_sst_byte: AtomicU64,
    pub list: AtomicU64,
    pub delete: AtomicU64,
    /// GET in volo adesso e massimo visto (per capire il parallelismo).
    pub in_volo: AtomicU64,
    pub max_in_volo: AtomicU64,
    /// M31: GET servite dal bancone (copia vicina), seconde GET partite (richiesta doppia) e quante hanno vinto.
    pub get_bancone: AtomicU64,
    pub doppie: AtomicU64,
    pub doppie_vinte: AtomicU64,
}

impl Contatori {
    /// Tutte le richieste di classe GET al magazzino (S3 fattura HEAD e GET allo stesso prezzo). Le seconde GET della
    /// richiesta doppia sono comprese; quelle servite dal bancone no.
    pub fn get_totali(&self) -> u64 {
        self.get_intera.load(Relaxed) + self.get_range.load(Relaxed) + self.head.load(Relaxed) + self.doppie.load(Relaxed)
    }
    pub fn azzera(&self) {
        for c in [
            &self.get_intera,
            &self.get_range,
            &self.head,
            &self.get_byte,
            &self.put,
            &self.put_byte,
            &self.put_sst,
            &self.put_sst_byte,
            &self.get_sst,
            &self.get_sst_byte,
            &self.list,
            &self.delete,
            &self.max_in_volo,
            &self.get_bancone,
            &self.doppie,
            &self.doppie_vinte,
        ] {
            c.store(0, Relaxed);
        }
    }
}

/// Ritardo finto per GET: primo byte lognormale (mediana, sigma) più byte / banda.
#[derive(Clone, Copy, Debug)]
pub struct Finto {
    pub mediana_ms: f64,
    pub sigma: f64,
    pub banda_mbs: f64,
}

/// Una GET misurata: millisecondi dall'invio all'ultimo byte, byte, se servita dal bancone.
#[derive(Clone, Copy, Debug, serde::Serialize)]
pub struct GetMisurata {
    pub ms: f64,
    pub byte: u64,
}

pub struct Conta {
    pub inner: Arc<dyn ObjectStore>,
    pub c: Arc<Contatori>,
    /// Ritardo finto (None = nessuno). Si legge a ogni GET.
    pub finto: RwLock<Option<Finto>>,
    rng: Mutex<SmallRng>,
    /// GET per scatola (nome del file senza `.sst`), per sapere su quale pila è andata ogni GET.
    pub per_scatola: Mutex<HashMap<String, u64>>,
    /// Copia vicina (disco locale) e scatole servite da lì: le pile recenti sul bancone.
    pub vicino: Option<Arc<dyn ObjectStore>>,
    pub sul_bancone: RwLock<HashSet<String>>,
    /// Richiesta doppia: 0 = spenta; altrimenti percentile (per esempio 95) delle ultime GET oltre il quale parte la
    /// seconda. Il ritardo minimo e massimo della seconda sono fissi.
    pub doppia_percentile: AtomicU64,
    pub doppia_min_ms: f64,
    finestra: Mutex<VecDeque<f64>>,
    /// GET misurate dall'ultimo azzeramento (solo quelle al magazzino, prima risposta utile).
    pub misurate: Mutex<Vec<GetMisurata>>,
    /// Limite di GET in volo (0 = nessuno): con 1 le GET vanno una dopo l'altra.
    pub semaforo: RwLock<Option<Arc<tokio::sync::Semaphore>>>,
}

impl fmt::Debug for Conta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Conta({})", self.inner)
    }
}

impl Conta {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self::con_vicino(inner, None)
    }
    pub fn con_vicino(inner: Arc<dyn ObjectStore>, vicino: Option<Arc<dyn ObjectStore>>) -> Self {
        Self {
            inner,
            c: Arc::new(Contatori::default()),
            finto: RwLock::new(None),
            rng: Mutex::new(SmallRng::seed_from_u64(31)),
            per_scatola: Default::default(),
            vicino,
            sul_bancone: Default::default(),
            doppia_percentile: AtomicU64::new(0),
            doppia_min_ms: 5.0,
            finestra: Mutex::new(VecDeque::new()),
            misurate: Mutex::new(Vec::new()),
            semaforo: RwLock::new(None),
        }
    }

    pub fn azzera(&self) {
        self.c.azzera();
        self.per_scatola.lock().unwrap().clear();
        self.misurate.lock().unwrap().clear();
    }

    fn ritardo_finto(&self, byte_attesi: u64) -> Option<Duration> {
        let f = (*self.finto.read().unwrap())?;
        let z: f64 = {
            // normale standard con Box-Muller
            let mut r = self.rng.lock().unwrap();
            let (u1, u2): (f64, f64) = (r.gen::<f64>().max(1e-12), r.gen());
            (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos()
        };
        let ms = f.mediana_ms * (f.sigma * z).exp() + byte_attesi as f64 / (f.banda_mbs * 1048.576);
        Some(Duration::from_secs_f64(ms / 1000.0))
    }

    /// Soglia della richiesta doppia: il percentile scelto delle ultime 1.000 GET (almeno `doppia_min_ms`).
    fn soglia_doppia(&self) -> Option<Duration> {
        let p = self.doppia_percentile.load(Relaxed);
        if p == 0 {
            return None;
        }
        let w = self.finestra.lock().unwrap();
        if w.len() < 50 {
            return Some(Duration::from_secs_f64(0.2)); // poche misure: soglia larga
        }
        let mut v: Vec<f64> = w.iter().copied().collect();
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let ms = v[((v.len() - 1) as f64 * p as f64 / 100.0) as usize].max(self.doppia_min_ms);
        Some(Duration::from_secs_f64(ms / 1000.0))
    }

    fn ricorda(&self, ms: f64) {
        let mut w = self.finestra.lock().unwrap();
        w.push_back(ms);
        if w.len() > 1000 {
            w.pop_front();
        }
    }

    /// Una GET al magazzino, con il ritardo finto, letta fino all'ultimo byte.
    async fn una_get(inner: Arc<dyn ObjectStore>, location: Path, options: GetOptions, ritardo: Option<Duration>) -> Result<(GetResult, Bytes)> {
        if let Some(d) = ritardo {
            tokio::time::sleep(d).await;
        }
        let r = inner.get_opts(&location, options).await?;
        let meta = r.meta.clone();
        let range = r.range.clone();
        let attributes = r.attributes.clone();
        let b = r.bytes().await?;
        let vuoto = GetResult { payload: GetResultPayload::Stream(futures::stream::empty().boxed()), meta, range, attributes, extensions: Default::default() };
        Ok((vuoto, b))
    }

    fn con_corpo(mut g: GetResult, b: Bytes) -> GetResult {
        g.payload = GetResultPayload::Stream(futures::stream::once(async move { Ok(b) }).boxed());
        g
    }

    fn byte_attesi(options: &GetOptions) -> u64 {
        match &options.range {
            Some(object_store::GetRange::Bounded(r)) => r.end - r.start,
            _ => 64 << 10,
        }
    }
}

impl fmt::Display for Conta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Conta({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for Conta {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> Result<PutResult> {
        let n = payload.content_length() as u64;
        freno(false);
        self.c.put.fetch_add(1, Relaxed);
        self.c.put_byte.fetch_add(n, Relaxed);
        if location.as_ref().contains("/compacted/") {
            self.c.put_sst.fetch_add(1, Relaxed);
            self.c.put_sst_byte.fetch_add(n, Relaxed);
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> Result<Box<dyn MultipartUpload>> {
        freno(false);
        self.c.put.fetch_add(1, Relaxed);
        let sst = location.as_ref().contains("/compacted/");
        if sst {
            self.c.put_sst.fetch_add(1, Relaxed);
        }
        let inner = self.inner.put_multipart_opts(location, opts).await?;
        Ok(Box::new(Parti { inner, c: Arc::clone(&self.c), sst }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let nome = location.filename().map(|n| n.trim_end_matches(".sst").to_string());
        let sst = location.as_ref().contains("/compacted/");
        // pile sul bancone: la scatola si legge dalla copia vicina, senza ritardo e senza contare una GET al magazzino
        if sst && !options.head {
            if let (Some(v), Some(n)) = (&self.vicino, &nome) {
                if self.sul_bancone.read().unwrap().contains(n) {
                    self.c.get_bancone.fetch_add(1, Relaxed);
                    return v.get_opts(location, options).await;
                }
            }
        }
        freno(true);
        if options.head {
            self.c.head.fetch_add(1, Relaxed);
        } else if options.range.is_some() {
            self.c.get_range.fetch_add(1, Relaxed);
        } else {
            self.c.get_intera.fetch_add(1, Relaxed);
        }
        let sem = self.semaforo.read().unwrap().clone();
        let _permesso = match sem {
            Some(s) => Some(s.acquire_owned().await.expect("semaforo")),
            None => None,
        };
        let v = self.c.in_volo.fetch_add(1, Relaxed) + 1;
        self.c.max_in_volo.fetch_max(v, Relaxed);
        let attesi = Self::byte_attesi(&options);
        let t0 = Instant::now();
        let res = match self.soglia_doppia() {
            Some(soglia) if !options.head => {
                let prima = Self::una_get(self.inner.clone(), location.clone(), options.clone(), self.ritardo_finto(attesi));
                tokio::pin!(prima);
                tokio::select! {
                    r = &mut prima => r,
                    _ = tokio::time::sleep(soglia) => {
                        // la prima tarda: parte la seconda, uguale; vince chi arriva prima
                        self.c.doppie.fetch_add(1, Relaxed);
                        freno(true);
                        let seconda = Self::una_get(self.inner.clone(), location.clone(), options.clone(), self.ritardo_finto(attesi));
                        tokio::pin!(seconda);
                        tokio::select! {
                            r = &mut prima => r,
                            r = &mut seconda => { self.c.doppie_vinte.fetch_add(1, Relaxed); r }
                        }
                    }
                }
            }
            _ => Self::una_get(self.inner.clone(), location.clone(), options.clone(), self.ritardo_finto(attesi)).await,
        };
        let ms = t0.elapsed().as_secs_f64() * 1000.0;
        self.c.in_volo.fetch_sub(1, Relaxed);
        let (g, b) = res?;
        let n = b.len() as u64;
        if !options.head {
            self.ricorda(ms);
            self.misurate.lock().unwrap().push(GetMisurata { ms, byte: n });
        }
        self.c.get_byte.fetch_add(n, Relaxed);
        if sst {
            self.c.get_sst.fetch_add(1, Relaxed);
            self.c.get_sst_byte.fetch_add(n, Relaxed);
            if let Some(nome) = nome {
                *self.per_scatola.lock().unwrap().entry(nome).or_default() += 1;
            }
        }
        Ok(Self::con_corpo(g, b))
    }

    fn delete_stream(&self, locations: BoxStream<'static, Result<Path>>) -> BoxStream<'static, Result<Path>> {
        let c = Arc::clone(&self.c);
        self.inner.delete_stream(
            locations
                .inspect(move |_| {
                    c.delete.fetch_add(1, Relaxed);
                })
                .boxed(),
        )
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        freno(false);
        self.c.list.fetch_add(1, Relaxed);
        self.inner.list(prefix)
    }

    fn list_with_offset(&self, prefix: Option<&Path>, offset: &Path) -> BoxStream<'static, Result<ObjectMeta>> {
        freno(false);
        self.c.list.fetch_add(1, Relaxed);
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        freno(false);
        self.c.list.fetch_add(1, Relaxed);
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

/// Conta ogni parte di un caricamento multiparte come un PUT (S3 fattura UploadPart come PUT).
#[derive(Debug)]
struct Parti {
    inner: Box<dyn MultipartUpload>,
    c: Arc<Contatori>,
    sst: bool,
}

#[async_trait]
impl MultipartUpload for Parti {
    fn put_part(&mut self, data: PutPayload) -> object_store::UploadPart {
        freno(false);
        let n = data.content_length() as u64;
        self.c.put.fetch_add(1, Relaxed);
        self.c.put_byte.fetch_add(n, Relaxed);
        if self.sst {
            self.c.put_sst.fetch_add(1, Relaxed);
            self.c.put_sst_byte.fetch_add(n, Relaxed);
        }
        self.inner.put_part(data)
    }
    async fn complete(&mut self) -> Result<PutResult> {
        self.c.put.fetch_add(1, Relaxed);
        self.inner.complete().await
    }
    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }
}
