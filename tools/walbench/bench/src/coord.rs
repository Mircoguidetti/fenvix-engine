//! Small S3 helpers for coordination and result upload (the instances have no AWS CLI). Requests are counted in the
//! same counters as the S3 standard backend (same bucket, same prices).

use crate::counters::{add, C};
use anyhow::{anyhow, Result};
use bytes::Bytes;

pub struct Coord {
    c: aws_sdk_s3::Client,
    bucket: String,
}

impl Coord {
    pub async fn new(bucket: &str) -> Coord {
        Coord { c: crate::store::s3_client().await, bucket: bucket.to_string() }
    }

    pub async fn put(&self, key: &str, body: Bytes) -> Result<()> {
        for attempt in 0..5u32 {
            add(&C.s3_put, 1);
            add(&C.s3_put_bytes, body.len() as u64);
            if self.c.put_object().bucket(&self.bucket).key(key).body(body.clone().into()).send().await.is_ok() {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(200 << attempt)).await;
        }
        Err(anyhow!("put {key} failed"))
    }

    pub async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        add(&C.s3_get, 1);
        match self.c.get_object().bucket(&self.bucket).key(key).send().await {
            Ok(r) => Ok(Some(r.body.collect().await?.into_bytes())),
            Err(e) => {
                if e.as_service_error().map(|s| s.is_no_such_key()).unwrap_or(false) {
                    Ok(None)
                } else {
                    Err(anyhow!("get {key}: {}", aws_sdk_s3::error::DisplayErrorContext(&e)))
                }
            }
        }
    }
}

pub async fn cli_put(bucket: &str, key: &str, file: &str) -> Result<()> {
    let c = Coord::new(bucket).await;
    c.put(key, Bytes::from(std::fs::read(file)?)).await
}

pub async fn cli_get(bucket: &str, key: &str, file: &str) -> Result<()> {
    let c = Coord::new(bucket).await;
    match c.get(key).await? {
        Some(b) => Ok(std::fs::write(file, b)?),
        None => Err(anyhow!("{key} not found")),
    }
}
