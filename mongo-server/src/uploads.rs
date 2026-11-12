//! Multipart uploads left unfinished (c2-fix-clone-uploads).
//!
//! The server starts a multipart upload for every run of sealed runs (`runfmt::RunWriter`). S3 keeps the parts of an
//! upload that is neither completed nor aborted, and bills them, until someone aborts it (integration-3: 9.4 GB of them
//! filled MinIO). A writer that fails or is given up aborts its own upload (`RunWriter`'s drop); what a crash leaves
//! (kill -9, a machine lost, a process ending with a job running) nobody else would abort. At startup this sweep lists
//! the bucket's unfinished uploads under `tenants/` and aborts those started more than `--abandoned-upload-secs` ago.
//!
//! The bound: an upload of a live process on the same bucket (the old owner during a takeover, a deploy overlap) must
//! not be aborted while it runs. The longest upload is one run: up to `split_bytes` (2 GiB) for a sealed run, a few GiB
//! for the largest delta tier, written at tens of MB/s, so minutes (ESTIMATED). The default, one hour, is about ten times
//! that and keeps the bill of a crash's leftovers to at most one hour of their storage. Aborting a live upload anyway
//! loses no data: a run is listed by a version only after its upload completed, so the job fails, and its run is
//! written again by a later job.

use object_store::aws::AmazonS3;
use object_store::multipart::MultipartStore;
use object_store::path::Path;
use object_store::signer::{SignedUrlOptions, Signer};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::Duration;

/// Uploads listed and aborted by the startup sweeps of this process.
pub static LISTED: AtomicU64 = AtomicU64::new(0);
pub static ABORTED: AtomicU64 = AtomicU64::new(0);

/// The prefix of every object the server writes with a multipart upload (the runs of `fx`).
const PREFIX: &str = "tenants/";

struct Upload {
    key: String,
    id: String,
    initiated_ms: i64,
}

/// Abort the unfinished uploads under `tenants/` started more than `older_than` ago. Returns (listed, aborted).
pub async fn abort_abandoned(s3: &Arc<AmazonS3>, older_than: Duration) -> anyhow::Result<(u64, u64)> {
    let now_ms = chrono::Utc::now().timestamp_millis();
    let cut = now_ms - older_than.as_millis() as i64;
    let http = reqwest::Client::new();
    let (mut listed, mut aborted) = (0u64, 0u64);
    let (mut key_marker, mut id_marker) = (String::new(), String::new());
    loop {
        // ListMultipartUploads through a URL signed by the store's own credentials (object_store has no list call). No
        // `prefix`: MinIO lists only the uploads of the exact key given as a prefix; the keys are filtered here.
        let mut q = vec![("uploads".to_string(), String::new())];
        if !key_marker.is_empty() {
            q.push(("key-marker".into(), key_marker.clone()));
            q.push(("upload-id-marker".into(), id_marker.clone()));
        }
        let url = s3.signed_url_opts(reqwest::Method::GET, &Path::from(""), Duration::from_secs(300), &SignedUrlOptions::default().with_query(q)).await?;
        let resp = http.get(url).send().await?;
        let status = resp.status();
        let body = resp.text().await?;
        anyhow::ensure!(status.is_success(), "ListMultipartUploads answered {status}: {}", body.chars().take(300).collect::<String>());
        let (page, next) = parse(&body)?;
        for u in page.into_iter().filter(|u| u.key.starts_with(PREFIX)) {
            listed += 1;
            if u.initiated_ms <= cut {
                match s3.abort_multipart(&Path::from(u.key.as_str()), &u.id).await {
                    Ok(()) => aborted += 1,
                    Err(e) => eprintln!("uploads: abort of {} ({}) failed: {e}", u.key, u.id),
                }
            }
        }
        match next {
            Some((k, i)) => {
                key_marker = k;
                id_marker = i;
            }
            None => break,
        }
    }
    LISTED.fetch_add(listed, Relaxed);
    ABORTED.fetch_add(aborted, Relaxed);
    Ok((listed, aborted))
}

/// The uploads of one ListMultipartUploads page, and the markers of the next page when it is truncated.
fn parse(body: &str) -> anyhow::Result<(Vec<Upload>, Option<(String, String)>)> {
    let mut out = Vec::new();
    for part in body.split("<Upload>").skip(1) {
        let u = part.split("</Upload>").next().unwrap_or("");
        let key = tag(u, "Key").ok_or_else(|| anyhow::anyhow!("an upload without a key"))?;
        let id = tag(u, "UploadId").ok_or_else(|| anyhow::anyhow!("an upload without an id"))?;
        let at = tag(u, "Initiated").ok_or_else(|| anyhow::anyhow!("an upload without a start time"))?;
        let initiated_ms = chrono::DateTime::parse_from_rfc3339(&at).map_err(|e| anyhow::anyhow!("start time {at}: {e}"))?.timestamp_millis();
        out.push(Upload { key, id, initiated_ms });
    }
    let truncated = tag(body, "IsTruncated").is_some_and(|t| t == "true");
    let next = if truncated {
        let k = tag(body, "NextKeyMarker").unwrap_or_default();
        let i = tag(body, "NextUploadIdMarker").unwrap_or_default();
        anyhow::ensure!(!k.is_empty(), "a truncated ListMultipartUploads page without a next key marker");
        Some((k, i))
    } else {
        None
    };
    Ok((out, next))
}

/// The text of the first `<name>...</name>` in `s`, XML entities decoded.
fn tag(s: &str, name: &str) -> Option<String> {
    let open = format!("<{name}>");
    let close = format!("</{name}>");
    let a = s.find(&open)? + open.len();
    let b = a + s[a..].find(&close)?;
    Some(s[a..b].replace("&lt;", "<").replace("&gt;", ">").replace("&quot;", "\"").replace("&apos;", "'").replace("&amp;", "&"))
}

/// The sweep in the background (startup is not delayed); a failure is logged, the next start tries again.
pub fn spawn_sweep(s3: Arc<AmazonS3>, older_than: Duration) {
    tokio::spawn(async move {
        match abort_abandoned(&s3, older_than).await {
            Ok((l, a)) => eprintln!("uploads: {l} unfinished multipart uploads under {PREFIX}, {a} older than {older_than:?} aborted"),
            Err(e) => eprintln!("uploads: the sweep of unfinished multipart uploads failed: {e:#}"),
        }
    });
}

#[cfg(test)]
mod tests {
    #[test]
    fn parses_a_page() {
        let body = r#"<?xml version="1.0" encoding="UTF-8"?><ListMultipartUploadsResult><Bucket>b</Bucket><IsTruncated>true</IsTruncated><NextKeyMarker>tenants/a/x</NextKeyMarker><NextUploadIdMarker>id2</NextUploadIdMarker><Upload><Key>tenants/a/fx/runs/1&amp;2</Key><UploadId>id1</UploadId><Initiated>2026-11-10T21:35:02.000Z</Initiated></Upload><Upload><Key>tenants/a/x</Key><UploadId>id2</UploadId><Initiated>2026-11-10T21:36:02Z</Initiated></Upload></ListMultipartUploadsResult>"#;
        let (u, next) = super::parse(body).unwrap();
        assert_eq!(u.len(), 2);
        assert_eq!(u[0].key, "tenants/a/fx/runs/1&2");
        assert_eq!(u[1].initiated_ms - u[0].initiated_ms, 60_000);
        assert_eq!(next, Some(("tenants/a/x".to_string(), "id2".to_string())));
        let (u, next) = super::parse("<ListMultipartUploadsResult><IsTruncated>false</IsTruncated></ListMultipartUploadsResult>").unwrap();
        assert!(u.is_empty() && next.is_none());
    }
}
