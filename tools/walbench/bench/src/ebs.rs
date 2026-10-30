//! EBS reattach test, instance side. Both instances run `ebs-holder` for the same volume. Whenever the volume shows
//! up (attached by the orchestrator), the holder: mounts it (ext4 journal replay), recovers the log file, resumes
//! writing (8 writers, 1 KB, group commit w0, fdatasync) and uploads the session's timestamps and the recovered ids.
//! When the volume disappears (the orchestrator force-detaches it mid-write), it uploads the ids that were
//! acknowledged in the session, lazily unmounts and waits for the volume again.
//! The orchestrator measures the AWS side (detach, attach) and checks acked(previous sessions) ⊆ recovered(next).

use crate::bench::NEXT_ID;
use crate::codec;
use crate::committer::{Committer, Mode};
use crate::coord::Coord;
use crate::counters::{self, now_ms};
use crate::crash::ranges;
use crate::store::{FileStore, Store};
use anyhow::Result;
use bytes::Bytes;
use serde_json::json;
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(clap::Args, Debug, Clone)]
pub struct HolderArgs {
    #[arg(long)]
    pub vol: String,
    #[arg(long, default_value = "/mnt/wal")]
    pub mount: String,
    #[arg(long)]
    pub me: String,
    #[arg(long)]
    pub bucket: String,
    /// coordination prefix, e.g. runs/<run>/ebs/
    #[arg(long)]
    pub coord: String,
    #[arg(long, default_value_t = 8)]
    pub conc: usize,
    #[arg(long, default_value_t = 1024)]
    pub size: usize,
    #[arg(long, default_value_t = 256)]
    pub prealloc_mb: u64,
    #[arg(long, default_value_t = 100)]
    pub max_sessions: u32,
}

pub async fn holder(a: HolderArgs) -> Result<()> {
    counters::start_live(format!("ebs-holder-{}", a.me));
    let coord = Arc::new(Coord::new(&a.bucket).await);
    let dev = format!("/dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_{}", a.vol.replace('-', ""));
    std::fs::create_dir_all(&a.mount)?;
    for _ in 0..a.max_sessions {
        while !Path::new(&dev).exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let t_dev = now_ms();
        let mut mount_tries = 0;
        loop {
            mount_tries += 1;
            let st = tokio::process::Command::new("mount").args(["-o", "noatime", &dev, &a.mount]).status().await?;
            if st.success() {
                break;
            }
            if mount_tries > 200 {
                anyhow::bail!("mount failed 200 times");
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let t_mount = now_ms();
        let path = format!("{}/reattach.wal", a.mount);
        let existed = Path::new(&path).exists();
        let (p2, mb) = (path.clone(), a.prealloc_mb);
        let store = Arc::new(tokio::task::spawn_blocking(move || FileStore::open(&p2, mb)).await??);
        let rec = store.read_all().await?;
        let t_recovered = now_ms();
        let ids: BTreeSet<u64> = rec.batches.iter().flat_map(|b| b.entries.iter().map(|e| e.id)).collect();
        let bad_payload = rec.batches.iter().flat_map(|b| b.entries.iter()).filter(|e| !codec::check_payload(e)).count();
        let sid: u32 = rand::random::<u32>() >> 1;
        NEXT_ID.store((sid as u64) << 32, Relaxed);
        let com = Committer::new(store.clone() as Arc<dyn Store>, Mode::parse("w0", 4_000_000, true)?);
        let acked = Arc::new(Mutex::new(Vec::<u64>::new()));
        let first_ack = Arc::new(AtomicU64::new(0));
        let failed = Arc::new(AtomicBool::new(false));
        let mut hs = Vec::new();
        for w in 0..a.conc {
            let (com, acked, first_ack, failed, size) = (com.clone(), acked.clone(), first_ack.clone(), failed.clone(), a.size);
            hs.push(tokio::spawn(async move {
                loop {
                    let id = NEXT_ID.fetch_add(1, Relaxed);
                    match com.write(w as u32, id, codec::payload(id, size)).await {
                        Ok(()) => {
                            let _ = first_ack.compare_exchange(0, now_ms(), Relaxed, Relaxed);
                            acked.lock().unwrap().push(id);
                        }
                        Err(_) => {
                            failed.store(true, Relaxed);
                            return;
                        }
                    }
                }
            }));
        }
        while first_ack.load(Relaxed) == 0 && !failed.load(Relaxed) && Path::new(&dev).exists() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let sess = json!({"me": a.me, "sid": sid, "existed": existed, "t_dev_ms": t_dev, "t_mount_ms": t_mount, "mount_tries": mount_tries,
                          "t_recovered_ms": t_recovered, "t_first_ack_ms": first_ack.load(Relaxed), "recovered_n": ids.len(),
                          "recovered": ranges(&ids), "bad": rec.bad, "bad_payload": bad_payload});
        coord.put(&format!("{}sess-{sid:010}.json", a.coord), Bytes::from(sess.to_string())).await?;
        eprintln!("session {sid}: device {t_dev}, mount +{} ms, recovered {} ids +{} ms, first ack +{} ms",
                  t_mount - t_dev, ids.len(), t_recovered - t_mount, first_ack.load(Relaxed).saturating_sub(t_recovered));
        // write until the volume goes away (force-detach) or the writes fail
        while Path::new(&dev).exists() && !failed.load(Relaxed) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let t_gone = now_ms();
        // a write acknowledged after this snapshot is not counted (it may still be in flight on a dead device)
        let mine: BTreeSet<u64> = acked.lock().unwrap().iter().copied().collect();
        let done = json!({"me": a.me, "sid": sid, "t_gone_ms": t_gone, "acked_n": mine.len(), "acked": ranges(&mine),
                          "writes_failed": failed.load(Relaxed), "device_present": Path::new(&dev).exists()});
        coord.put(&format!("{}sess-{sid:010}-acked.json", a.coord), Bytes::from(done.to_string())).await?;
        eprintln!("session {sid} ended: {} acked", mine.len());
        for h in hs {
            h.abort();
        }
        // lazy unmount of the dead filesystem (in the background: it must not block waiting for the next attach)
        let m = a.mount.clone();
        tokio::spawn(async move {
            let _ = tokio::time::timeout(Duration::from_secs(30), tokio::process::Command::new("umount").args(["-l", &m]).status()).await;
        });
        tokio::time::sleep(Duration::from_millis(500)).await;
        while Path::new(&dev).exists() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        counters::flush_live(&format!("ebs-holder-{}", a.me));
    }
    Ok(())
}
