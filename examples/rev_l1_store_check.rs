//! Reviewer of L1: live SST ids and bytes of a tenant on a local store, and whether each SST file on disk is live.
//! `rev_l1_store_check LOCAL_DIR TENANT [SST_ID...]`
use object_store::local::LocalFileSystem;
use slatedb::admin::Admin;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let a: Vec<String> = std::env::args().collect();
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(&a[1])?);
    let admin = Admin::builder(format!("tenants/{}", a[2]), store).build();
    let m = admin.read_manifest(None).await?.expect("no manifest");
    let mut live = std::collections::HashSet::new();
    let mut bytes = 0u64;
    for v in m.l0() {
        if live.insert(v.sst.id.value().to_string()) { bytes += v.sst.info.index_offset + 0; }
    }
    for r in m.compacted() {
        for v in r.sst_views() {
            live.insert(v.sst.id.value().to_string());
        }
    }
    let est: u64 = m.compacted().iter().map(|r| r.estimate_size()).sum();
    println!("manifest {} l0={} srs={} live_ssts={} sr_estimate_mb={} checkpoints={}", m.id(), m.l0().len(), m.compacted().len(), live.len(), est >> 20, m.checkpoints().len());
    let dir = format!("{}/tenants/{}/compacted", a[1], a[2]);
    let (mut on_disk, mut dead, mut dead_mb, mut live_mb) = (0, 0, 0u64, 0u64);
    for e in std::fs::read_dir(&dir)? {
        let e = e?;
        let n = e.file_name().to_string_lossy().trim_end_matches(".sst").to_string();
        let sz = e.metadata()?.len();
        on_disk += 1;
        if live.contains(&n) { live_mb += sz; } else { dead += 1; dead_mb += sz; }
    }
    println!("on disk {on_disk} SSTs: live {} MB, not in the manifest {dead} SSTs {} MB", live_mb >> 20, dead_mb >> 20);
    for id in &a[3..] {
        println!("{id}: in manifest {}", live.contains(id));
    }
    Ok(())
}
