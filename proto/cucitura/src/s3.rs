//! Client S3 minimo (aws-sdk-s3) con contatori di richieste e byte. object_store 0.14 (quello di proto-lettura) non
//! ha UploadPartCopy, per questo qui si usa l'SDK ufficiale.

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use bytes::Bytes;
use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;

#[derive(Default, Debug)]
pub struct Contatori {
    pub get: AtomicU64,
    pub get_byte: AtomicU64,
    pub put: AtomicU64,
    pub put_byte: AtomicU64,
    pub copia: AtomicU64,
    pub copia_byte: AtomicU64,
    pub altre: AtomicU64,
}

#[derive(Default, Debug, Clone, Copy, Serialize)]
pub struct Conti {
    pub get: u64,
    pub get_byte: u64,
    /// PUT, UploadPart, Create/CompleteMultipartUpload
    pub put: u64,
    pub put_byte: u64,
    /// UploadPartCopy
    pub copia: u64,
    pub copia_byte: u64,
    pub altre: u64,
}

impl Conti {
    pub fn meno(self, o: Conti) -> Conti {
        Conti {
            get: self.get - o.get,
            get_byte: self.get_byte - o.get_byte,
            put: self.put - o.put,
            put_byte: self.put_byte - o.put_byte,
            copia: self.copia - o.copia,
            copia_byte: self.copia_byte - o.copia_byte,
            altre: self.altre - o.altre,
        }
    }
}

#[derive(Clone)]
pub struct S3 {
    pub c: aws_sdk_s3::Client,
    pub bucket: String,
    pub n: Arc<Contatori>,
}

/// Parte di una multipart upload.
pub enum Parte {
    Byte(Bytes),
    /// copia lato server dell'intervallo [da, a) dell'oggetto `chiave`
    Copia { chiave: String, da: u64, a: u64 },
}

impl Parte {
    pub fn len(&self) -> u64 {
        match self {
            Parte::Byte(b) => b.len() as u64,
            Parte::Copia { da, a, .. } => a - da,
        }
    }
}

pub const MIN_PARTE: u64 = 5 << 20;

impl S3 {
    pub async fn new(endpoint: &str, bucket: &str) -> anyhow::Result<S3> {
        let cred = aws_credential_types::Credentials::from_keys("minioadmin", "minioadmin", None);
        let conf = aws_sdk_s3::Config::builder()
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .endpoint_url(endpoint)
            .credentials_provider(cred)
            .force_path_style(true)
            // niente checksum in coda (aws-chunked): il finto S3 su disco vuole corpi semplici con Content-Length
            .request_checksum_calculation(aws_sdk_s3::config::RequestChecksumCalculation::WhenRequired)
            .response_checksum_validation(aws_sdk_s3::config::ResponseChecksumValidation::WhenRequired)
            .build();
        let c = aws_sdk_s3::Client::from_conf(conf);
        let s = S3 { c, bucket: bucket.to_string(), n: Arc::new(Contatori::default()) };
        let _ = s.c.create_bucket().bucket(bucket).send().await;
        Ok(s)
    }

    pub fn conti(&self) -> Conti {
        let n = &self.n;
        Conti {
            get: n.get.load(Relaxed),
            get_byte: n.get_byte.load(Relaxed),
            put: n.put.load(Relaxed),
            put_byte: n.put_byte.load(Relaxed),
            copia: n.copia.load(Relaxed),
            copia_byte: n.copia_byte.load(Relaxed),
            altre: n.altre.load(Relaxed),
        }
    }

    pub async fn get_range(&self, key: &str, da: u64, a: u64) -> anyhow::Result<Bytes> {
        let r = self.c.get_object().bucket(&self.bucket).key(key).range(format!("bytes={}-{}", da, a - 1)).send().await?;
        let b = r.body.collect().await?.into_bytes();
        anyhow::ensure!(b.len() as u64 == a - da, "GET corto: {} invece di {}", b.len(), a - da);
        self.n.get.fetch_add(1, Relaxed);
        self.n.get_byte.fetch_add(b.len() as u64, Relaxed);
        Ok(b)
    }

    pub async fn get_tail(&self, key: &str, n: u64) -> anyhow::Result<(Bytes, u64)> {
        let r = self.c.get_object().bucket(&self.bucket).key(key).range(format!("bytes=-{}", n)).send().await?;
        let tot = r.content_range().and_then(|s| s.rsplit('/').next()).and_then(|s| s.parse::<u64>().ok());
        let b = r.body.collect().await?.into_bytes();
        self.n.get.fetch_add(1, Relaxed);
        self.n.get_byte.fetch_add(b.len() as u64, Relaxed);
        Ok((b, tot.ok_or_else(|| anyhow::anyhow!("Content-Range mancante"))?))
    }

    pub async fn len(&self, key: &str) -> anyhow::Result<u64> {
        let r = self.c.head_object().bucket(&self.bucket).key(key).send().await?;
        self.n.altre.fetch_add(1, Relaxed);
        Ok(r.content_length().unwrap_or(0) as u64)
    }

    /// GET di un oggetto intero a pezzi da 16 MB in parallelo.
    pub async fn get_all(&self, key: &str) -> anyhow::Result<Bytes> {
        let n = self.len(key).await?;
        let pezzo = 16u64 << 20;
        let mut futs = Vec::new();
        let mut da = 0;
        while da < n {
            let a = (da + pezzo).min(n);
            futs.push(self.get_range(key, da, a));
            da = a;
        }
        let parti = futures::future::try_join_all(futs).await?;
        let mut v = Vec::with_capacity(n as usize);
        for p in parti {
            v.extend_from_slice(&p);
        }
        Ok(Bytes::from(v))
    }

    pub async fn delete(&self, key: &str) -> anyhow::Result<()> {
        self.c.delete_object().bucket(&self.bucket).key(key).send().await?;
        self.n.altre.fetch_add(1, Relaxed);
        Ok(())
    }

    /// Multipart upload con parti normali e copie lato server, al più `par` richieste insieme.
    pub async fn multipart(&self, key: &str, parti: Vec<Parte>, par: usize) -> anyhow::Result<()> {
        let n = parti.len();
        for (i, p) in parti.iter().enumerate() {
            anyhow::ensure!(i + 1 == n || p.len() >= MIN_PARTE, "parte {} di {} troppo piccola: {} byte", i + 1, n, p.len());
        }
        let up = self.c.create_multipart_upload().bucket(&self.bucket).key(key).send().await?;
        self.n.put.fetch_add(1, Relaxed);
        let uid = up.upload_id().unwrap().to_string();
        use futures::stream::{self, StreamExt, TryStreamExt};
        let done: Vec<CompletedPart> = stream::iter(parti.into_iter().enumerate())
            .map(|(i, p)| {
                let uid = uid.clone();
                async move {
                    let num = (i + 1) as i32;
                    let etag = match p {
                        Parte::Byte(b) => {
                            let l = b.len() as u64;
                            let r = self.c.upload_part().bucket(&self.bucket).key(key).upload_id(&uid).part_number(num).body(ByteStream::from(b)).send().await?;
                            self.n.put.fetch_add(1, Relaxed);
                            self.n.put_byte.fetch_add(l, Relaxed);
                            r.e_tag().unwrap_or_default().to_string()
                        }
                        Parte::Copia { chiave, da, a } => {
                            let r = self
                                .c
                                .upload_part_copy()
                                .bucket(&self.bucket)
                                .key(key)
                                .upload_id(&uid)
                                .part_number(num)
                                .copy_source(format!("{}/{}", self.bucket, chiave))
                                .copy_source_range(format!("bytes={}-{}", da, a - 1))
                                .send()
                                .await?;
                            self.n.copia.fetch_add(1, Relaxed);
                            self.n.copia_byte.fetch_add(a - da, Relaxed);
                            r.copy_part_result().and_then(|c| c.e_tag()).unwrap_or_default().to_string()
                        }
                    };
                    anyhow::Ok(CompletedPart::builder().part_number(num).e_tag(etag).build())
                }
            })
            .buffered(par)
            .try_collect()
            .await?;
        self.c
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(&uid)
            .multipart_upload(CompletedMultipartUpload::builder().set_parts(Some(done)).build())
            .send()
            .await?;
        self.n.put.fetch_add(1, Relaxed);
        Ok(())
    }

    /// Carica un oggetto costruito in memoria (parti da 16 MB se è grande).
    pub async fn put_grande(&self, key: &str, b: Bytes) -> anyhow::Result<()> {
        let pezzo = 16usize << 20;
        if b.len() <= pezzo {
            self.c.put_object().bucket(&self.bucket).key(key).body(ByteStream::from(b.clone())).send().await?;
            self.n.put.fetch_add(1, Relaxed);
            self.n.put_byte.fetch_add(b.len() as u64, Relaxed);
            return Ok(());
        }
        let mut parti = Vec::new();
        let mut da = 0;
        while da < b.len() {
            let a = (da + pezzo).min(b.len());
            parti.push(Parte::Byte(b.slice(da..a)));
            da = a;
        }
        self.multipart(key, parti, 4).await
    }

    /// Meta di un oggetto: 1 GET della coda (piè di pagina + meta se sta in 64 KB), altrimenti 2.
    pub async fn meta(&self, key: &str) -> anyhow::Result<crate::formato::Meta> {
        use crate::formato::*;
        let (coda, tot) = self.get_tail(key, 64 << 10).await?;
        let p = leggi_piede(&coda[coda.len() - FOOTER..])?;
        let need = p.meta_len as u64 + FOOTER as u64;
        let meta = if need <= coda.len() as u64 {
            let s = coda.len() - need as usize;
            coda.slice(s..s + p.meta_len as usize)
        } else {
            self.get_range(key, p.meta_off, p.meta_off + p.meta_len as u64).await?
        };
        Meta::parse(meta, p, tot)
    }
}
