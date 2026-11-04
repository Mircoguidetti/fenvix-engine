//! Finto S3 che conta: avvolge un ObjectStore e conta ogni richiesta (come `src/counting.rs` del motore).
//! Può aggiungere un ritardo fisso a ogni GET, per misurare i "giri in fila" di una query.

use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions,
    PutOptions, PutPayload, PutResult, RenameOptions, Result,
};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;

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
}

impl Contatori {
    /// Tutte le richieste di classe GET (S3 fattura HEAD e GET allo stesso prezzo).
    pub fn get_totali(&self) -> u64 {
        self.get_intera.load(Relaxed) + self.get_range.load(Relaxed) + self.head.load(Relaxed)
    }
    pub fn azzera(&self) {
        for c in [&self.get_intera, &self.get_range, &self.head, &self.get_byte, &self.put, &self.put_byte, &self.put_sst, &self.put_sst_byte, &self.get_sst, &self.get_sst_byte, &self.list, &self.delete, &self.max_in_volo] {
            c.store(0, Relaxed);
        }
    }
}

#[derive(Debug)]
pub struct Conta {
    pub inner: Arc<dyn ObjectStore>,
    pub c: Arc<Contatori>,
    /// Ritardo aggiunto a ogni GET, in microsecondi (0 = nessuno).
    pub ritardo_us: AtomicU64,
    /// M29: GET per scatola (nome del file senza `.sst`), per sapere su quale pila è andata ogni GET.
    pub per_scatola: std::sync::Mutex<std::collections::HashMap<String, u64>>,
}

impl Conta {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self { inner, c: Arc::new(Contatori::default()), ritardo_us: AtomicU64::new(0), per_scatola: Default::default() }
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
        self.c.put.fetch_add(1, Relaxed);
        self.c.put_byte.fetch_add(n, Relaxed);
        if location.as_ref().contains("/compacted/") {
            self.c.put_sst.fetch_add(1, Relaxed);
            self.c.put_sst_byte.fetch_add(n, Relaxed);
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> Result<Box<dyn MultipartUpload>> {
        self.c.put.fetch_add(1, Relaxed);
        let sst = location.as_ref().contains("/compacted/");
        if sst {
            self.c.put_sst.fetch_add(1, Relaxed);
        }
        let inner = self.inner.put_multipart_opts(location, opts).await?;
        Ok(Box::new(Parti { inner, c: Arc::clone(&self.c), sst }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        if options.head {
            self.c.head.fetch_add(1, Relaxed);
        } else if options.range.is_some() {
            self.c.get_range.fetch_add(1, Relaxed);
        } else {
            self.c.get_intera.fetch_add(1, Relaxed);
        }
        let v = self.c.in_volo.fetch_add(1, Relaxed) + 1;
        self.c.max_in_volo.fetch_max(v, Relaxed);
        let r = self.ritardo_us.load(Relaxed);
        if r > 0 {
            tokio::time::sleep(std::time::Duration::from_micros(r)).await;
        }
        let res = self.inner.get_opts(location, options).await;
        self.c.in_volo.fetch_sub(1, Relaxed);
        if let Ok(g) = &res {
            self.c.get_byte.fetch_add(g.range.end - g.range.start, Relaxed);
            if location.as_ref().contains("/compacted/") {
                self.c.get_sst.fetch_add(1, Relaxed);
                self.c.get_sst_byte.fetch_add(g.range.end - g.range.start, Relaxed);
                if let Some(nome) = location.filename() {
                    *self.per_scatola.lock().unwrap().entry(nome.trim_end_matches(".sst").to_string()).or_default() += 1;
                }
            }
        }
        res
    }

    fn delete_stream(&self, locations: BoxStream<'static, Result<Path>>) -> BoxStream<'static, Result<Path>> {
        let c = Arc::clone(&self.c);
        self.inner.delete_stream(locations.inspect(move |_| {
            c.delete.fetch_add(1, Relaxed);
        }).boxed())
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.c.list.fetch_add(1, Relaxed);
        self.inner.list(prefix)
    }

    fn list_with_offset(&self, prefix: Option<&Path>, offset: &Path) -> BoxStream<'static, Result<ObjectMeta>> {
        self.c.list.fetch_add(1, Relaxed);
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
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
