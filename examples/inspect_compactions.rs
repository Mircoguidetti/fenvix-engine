//! Print a tenant's LSM shape and its compactions (local store): `inspect_compactions LOCAL_DIR TENANT`.
use object_store::local::LocalFileSystem;
use slatedb::admin::Admin;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let a: Vec<String> = std::env::args().collect();
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(&a[1])?);
    let admin = Admin::builder(format!("tenants/{}", a[2]), store).build();
    if let Some(m) = admin.read_manifest(None).await? {
        let srs: Vec<String> = m.compacted().iter().map(|r| format!("SR{}:{}MB/{}", r.id, r.estimate_size() >> 20, r.sst_views().len())).collect();
        println!("manifest {} l0={} srs={:?} checkpoints={}", m.id(), m.l0().len(), srs, m.checkpoints().len());
    }
    if let Some(c) = admin.read_compactions(None).await? {
        println!("compactions file {} epoch {}", c.id(), c.compactor_epoch());
        for x in c.recent_compactions() {
            let v = serde_json::to_value(x)?;
            println!(
                "  {} status={:?} active={} sources={} bytes_processed={}MB outputs={} ctx_outputs={}",
                x.id(),
                x.status(),
                x.active(),
                v["spec"].to_string().chars().take(160).collect::<String>(),
                x.bytes_processed() >> 20,
                x.output_ssts().len(),
                v["ctx"].to_string().matches("\"id\"").count()
            );
        }
    }
    Ok(())
}
