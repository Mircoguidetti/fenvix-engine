//! Accesso a S3 (MinIO in locale) con il conteggio di ogni richiesta, per tipo. Le chiamate passano tutte da qui, così
//! il conteggio lato client è completo; il banco lo confronta con i contatori di MinIO per bucket
//! (`/minio/v2/metrics/bucket`, vedi `metriche_minio`).

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::path::Path;
use object_store::{ClientOptions, GetOptions, GetRange, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, RetryConfig, UpdateVersion};
use parking_lot::Mutex;
use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Default, Debug)]
pub struct Contatori {
    pub put: AtomicU64,
    pub put_cond: AtomicU64,
    pub get: AtomicU64,
    pub get_range: AtomicU64,
    pub list: AtomicU64,
    pub delete: AtomicU64,
    pub put_byte: AtomicU64,
    pub get_byte: AtomicU64,
    pub cas_412: AtomicU64,
    pub cas_304: AtomicU64,
    pub errori: AtomicU64,
    pub lat_get: Mutex<Option<hdrhistogram::Histogram<u64>>>,
}

#[derive(Clone, Copy, Debug, Default, Serialize, serde::Deserialize, PartialEq)]
pub struct Conteggi {
    pub put: u64,
    pub put_cond: u64,
    pub get: u64,
    pub get_range: u64,
    pub list: u64,
    pub delete: u64,
    pub put_byte: u64,
    pub get_byte: u64,
    pub cas_412: u64,
    pub cas_304: u64,
    pub errori: u64,
}

impl Conteggi {
    pub fn meno(&self, b: &Conteggi) -> Conteggi {
        Conteggi {
            put: self.put - b.put,
            put_cond: self.put_cond - b.put_cond,
            get: self.get - b.get,
            get_range: self.get_range - b.get_range,
            list: self.list - b.list,
            delete: self.delete - b.delete,
            put_byte: self.put_byte - b.put_byte,
            get_byte: self.get_byte - b.get_byte,
            cas_412: self.cas_412 - b.cas_412,
            cas_304: self.cas_304 - b.cas_304,
            errori: self.errori - b.errori,
        }
    }
    pub fn somma(&self, b: &Conteggi) -> Conteggi {
        Conteggi {
            put: self.put + b.put,
            put_cond: self.put_cond + b.put_cond,
            get: self.get + b.get,
            get_range: self.get_range + b.get_range,
            list: self.list + b.list,
            delete: self.delete + b.delete,
            put_byte: self.put_byte + b.put_byte,
            get_byte: self.get_byte + b.get_byte,
            cas_412: self.cas_412 + b.cas_412,
            cas_304: self.cas_304 + b.cas_304,
            errori: self.errori + b.errori,
        }
    }
    /// richieste di classe PUT (PUT, PUT condizionale, LIST: $0,005 ogni 1.000 su S3 Standard)
    pub fn classe_put(&self) -> u64 {
        self.put + self.put_cond + self.list
    }
    /// richieste di classe GET ($0,0004 ogni 1.000)
    pub fn classe_get(&self) -> u64 {
        self.get + self.get_range
    }
}

impl Contatori {
    pub fn leggi(&self) -> Conteggi {
        Conteggi {
            put: self.put.load(Relaxed),
            put_cond: self.put_cond.load(Relaxed),
            get: self.get.load(Relaxed),
            get_range: self.get_range.load(Relaxed),
            list: self.list.load(Relaxed),
            delete: self.delete.load(Relaxed),
            put_byte: self.put_byte.load(Relaxed),
            get_byte: self.get_byte.load(Relaxed),
            cas_412: self.cas_412.load(Relaxed),
            cas_304: self.cas_304.load(Relaxed),
            errori: self.errori.load(Relaxed),
        }
    }
}

#[derive(Debug)]
pub enum Cas {
    Ok(String),
    /// la condizione non vale: un altro ha scritto prima (412, o oggetto già esistente). `304` se MinIO ha risposto
    /// Not Modified (il bug noto): lo trattiamo come conflitto, e lo contiamo a parte.
    Conflitto { stato_304: bool },
    /// nessuna risposta certa (timeout, 5xx): la scrittura può essere avvenuta o no
    Incerto(String),
}

#[derive(Clone)]
pub struct Store {
    pub inner: Arc<dyn ObjectStore>,
    pub c: Arc<Contatori>,
    pub bucket: String,
}

pub fn endpoint() -> String {
    std::env::var("AWS_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:9000".to_string())
}

impl Store {
    pub fn minio(bucket: &str) -> anyhow::Result<Store> {
        let s3 = AmazonS3Builder::new()
            .with_endpoint(endpoint())
            .with_allow_http(true)
            .with_bucket_name(bucket)
            .with_access_key_id("minioadmin")
            .with_secret_access_key("minioadmin")
            .with_region("us-east-1")
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .with_client_options(ClientOptions::new().with_allow_http(true).with_timeout(Duration::from_secs(30)).with_connect_timeout(Duration::from_secs(3)).with_pool_max_idle_per_host(64))
            .with_retry(RetryConfig { max_retries: 2, retry_timeout: Duration::from_secs(30), ..Default::default() })
            .build()?;
        let c = Contatori { lat_get: Mutex::new(Some(hdrhistogram::Histogram::new_with_bounds(1, 60_000_000, 3).unwrap())), ..Default::default() };
        Ok(Store { inner: Arc::new(s3), c: Arc::new(c), bucket: bucket.to_string() })
    }

    fn lat(&self, t0: Instant) {
        if let Some(h) = self.c.lat_get.lock().as_mut() {
            let _ = h.record(t0.elapsed().as_micros() as u64);
        }
    }

    /// GET intero; None se l'oggetto non c'è.
    pub async fn get(&self, p: &str) -> anyhow::Result<Option<(Bytes, Option<String>)>> {
        self.c.get.fetch_add(1, Relaxed);
        let t0 = Instant::now();
        let r = self.inner.get_opts(&Path::from(p), GetOptions::default()).await;
        match r {
            Ok(r) => {
                let etag = r.meta.e_tag.clone();
                let b = r.bytes().await?;
                self.lat(t0);
                self.c.get_byte.fetch_add(b.len() as u64, Relaxed);
                Ok(Some((b, etag)))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => {
                self.c.errori.fetch_add(1, Relaxed);
                Err(e.into())
            }
        }
    }

    pub async fn get_range(&self, p: &str, a: u64, b: u64) -> anyhow::Result<Option<Bytes>> {
        self.c.get_range.fetch_add(1, Relaxed);
        let t0 = Instant::now();
        let r = self.inner.get_opts(&Path::from(p), GetOptions { range: Some(GetRange::Bounded(a..b)), ..Default::default() }).await;
        match r {
            Ok(r) => {
                let b = r.bytes().await?;
                self.lat(t0);
                self.c.get_byte.fetch_add(b.len() as u64, Relaxed);
                Ok(Some(b))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => {
                self.c.errori.fetch_add(1, Relaxed);
                Err(e.into())
            }
        }
    }

    pub async fn put(&self, p: &str, b: Bytes) -> anyhow::Result<()> {
        self.c.put.fetch_add(1, Relaxed);
        self.c.put_byte.fetch_add(b.len() as u64, Relaxed);
        self.inner.put_opts(&Path::from(p), PutPayload::from(b), PutOptions::default()).await.map_err(|e| {
            self.c.errori.fetch_add(1, Relaxed);
            anyhow::Error::from(e)
        })?;
        Ok(())
    }

    /// PUT condizionale: `prev = None` crea solo se l'oggetto non c'è (If-None-Match: *), altrimenti If-Match.
    pub async fn put_cond(&self, p: &str, b: Bytes, prev: Option<&str>) -> Cas {
        self.c.put_cond.fetch_add(1, Relaxed);
        self.c.put_byte.fetch_add(b.len() as u64, Relaxed);
        let mode = match prev {
            None => PutMode::Create,
            Some(e) => PutMode::Update(UpdateVersion { e_tag: Some(e.to_string()), version: None }),
        };
        match self.inner.put_opts(&Path::from(p), PutPayload::from(b), PutOptions { mode, ..Default::default() }).await {
            Ok(r) => Cas::Ok(r.e_tag.unwrap_or_default()),
            Err(object_store::Error::Precondition { .. }) | Err(object_store::Error::AlreadyExists { .. }) => {
                self.c.cas_412.fetch_add(1, Relaxed);
                Cas::Conflitto { stato_304: false }
            }
            Err(object_store::Error::NotModified { .. }) => {
                self.c.cas_304.fetch_add(1, Relaxed);
                Cas::Conflitto { stato_304: true }
            }
            Err(e) => {
                self.c.errori.fetch_add(1, Relaxed);
                Cas::Incerto(e.to_string())
            }
        }
    }

    pub async fn delete(&self, p: &str) -> anyhow::Result<()> {
        self.c.delete.fetch_add(1, Relaxed);
        match self.inner.delete(&Path::from(p)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => {
                self.c.errori.fetch_add(1, Relaxed);
                Err(e.into())
            }
        }
    }

    /// LIST di un prefisso; conta una richiesta ogni 1.000 chiavi (una pagina S3).
    pub async fn list(&self, prefix: &str) -> anyhow::Result<Vec<object_store::ObjectMeta>> {
        let v: Vec<_> = self.inner.list(Some(&Path::from(prefix))).try_collect().await?;
        self.c.list.fetch_add(1 + (v.len().saturating_sub(1) / 1000) as u64, Relaxed);
        Ok(v)
    }
}

/// Contatori di MinIO per un bucket: richieste per API (`minio_bucket_requests_total`).
pub async fn metriche_minio(bucket: &str) -> anyhow::Result<std::collections::BTreeMap<String, u64>> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let ep = endpoint();
    let host = ep.trim_start_matches("http://").trim_end_matches('/').to_string();
    let mut s = tokio::net::TcpStream::connect(&host).await?;
    s.write_all(format!("GET /minio/v2/metrics/bucket HTTP/1.0\r\nHost: {host}\r\n\r\n").as_bytes()).await?;
    let mut b = String::new();
    s.read_to_string(&mut b).await?;
    let mut out = std::collections::BTreeMap::new();
    let tag = format!("bucket=\"{bucket}\"");
    for l in b.lines() {
        if l.starts_with("minio_bucket_requests_total{") && l.contains(&tag) {
            if let (Some(a), Some(v)) = (l.split("api=\"").nth(1).and_then(|x| x.split('"').next()), l.rsplit(' ').next()) {
                *out.entry(a.to_string()).or_insert(0) += v.parse::<f64>().unwrap_or(0.0) as u64;
            }
        }
    }
    Ok(out)
}
