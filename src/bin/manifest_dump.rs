//! Print SlateDB manifests of a database as JSON (fix-store-errors diagnostics).
//! Usage: manifest_dump <endpoint> <bucket> <db> [first_id [last_id]]  (MinIO credentials minioadmin)
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let a: Vec<String> = std::env::args().collect();
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(
        object_store::aws::AmazonS3Builder::new()
            .with_endpoint(&a[1])
            .with_bucket_name(&a[2])
            .with_access_key_id("minioadmin")
            .with_secret_access_key("minioadmin")
            .with_region("us-east-1")
            .with_allow_http(true)
            .build()?,
    );
    let admin = slatedb::admin::Admin::builder(format!("tenants/{}", a[3]), store).build();
    let first: u64 = a.get(4).map(|s| s.parse().unwrap()).unwrap_or(1);
    let last: u64 = a.get(5).map(|s| s.parse().unwrap()).unwrap_or(first);
    for id in first..=last {
        match admin.read_manifest(Some(id)).await? {
            Some(m) => println!("{}", serde_json::to_string(&m)?),
            None => println!("{{\"id\": {id}, \"missing\": true}}"),
        }
    }
    Ok(())
}
