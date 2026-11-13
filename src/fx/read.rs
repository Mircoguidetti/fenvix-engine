//! The merged read: SlateDB above X_d, our runs at or below it (plan section 2; RULES-FOR-THE-CODE rules 1-3).
//!
//! SlateDB's own merged reads return the newest row of a key with its seq. A row with seq > X_d is the answer (its
//! envelope decides live or deleted). A row with seq <= X_d is ignored: our runs hold the newest row <= X_d of every
//! key, so the runs answer. The memtable and every L0 SST not yet absorbed are inside SlateDB's read, so they need no
//! code of ours. Every SlateDB row above X_d is an envelope, because a version is only published with E_env <= X_d
//! (`Version::check`): SlateDB's own tombstones are invisible to its merged reads (probe 1 of the plan), and a plain
//! delete above X_d would bring the run's older row back.
//!
//! The functions are generic over SlateDB's read trait, so the writer (`Db`), a reader (`DbReader`) and a transaction
//! snapshot (`DbSnapshot`) take the same path.

use super::hold::Hold;
use super::view::View;
use anyhow::Result;
use bytes::Bytes;
use slatedb::config::{ReadOptions, ScanOptions};
use slatedb::DbReadOps;
use std::sync::Arc;

/// Negative control of the step 7b fix round, tests only: a bounded read walks SlateDB's rows at or below X_d to the end
/// of its range, as step 3 did.
pub static CONTROL_FULL_WALK: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

fn control_full_walk() -> bool {
    CONTROL_FULL_WALK.load(std::sync::atomic::Ordering::Relaxed)
}

/// A point read. `view` None: the plain SlateDB read of the releases before.
pub async fn get<R: DbReadOps + Sync>(src: &R, opts: &ReadOptions, view: Option<&View>, env: Option<u64>, key: &[u8]) -> Result<Option<Bytes>> {
    // step 5b: SlateDB skips the SSTs whose rows are all at or below the held version's X_d
    let skip_opts;
    let opts = match view.and_then(|v| super::seqfilter::context(v.x_d)) {
        Some(c) => {
            skip_opts = ReadOptions { filter_context: Some(c), ..opts.clone() };
            &skip_opts
        }
        None => opts,
    };
    if let Some(v) = view.filter(|_| super::view::WAVE.load(std::sync::atomic::Ordering::Relaxed)) {
        // one wave: SlateDB's read and the runs' read at once
        let (kv, r) = futures::future::try_join(async { Ok::<_, anyhow::Error>(src.get_key_value_with_options(key, opts).await?) }, v.get(key)).await?;
        if let Some(kv) = kv {
            if kv.seq > v.x_d {
                return crate::envelope::decode(env, &kv.key, kv.seq, kv.value);
            }
        }
        return Ok(match r {
            Some(r) if !r.is_dead() => Some(r.into_doc()),
            _ => None,
        });
    }
    let kv = src.get_key_value_with_options(key, opts).await?;
    let Some(v) = view else {
        return match kv {
            Some(kv) => crate::envelope::decode(env, &kv.key, kv.seq, kv.value),
            None => Ok(None),
        };
    };
    if let Some(kv) = kv {
        if kv.seq > v.x_d {
            return crate::envelope::decode(env, &kv.key, kv.seq, kv.value);
        }
    }
    Ok(match v.get(key).await? {
        Some(r) if !r.is_dead() => Some(r.into_doc()),
        _ => None,
    })
}

/// A range read of [lo, hi), at most `limit` live rows, ascending or descending.
#[allow(clippy::too_many_arguments)]
pub async fn scan<R: DbReadOps + Sync>(src: &R, opts: &ScanOptions, view: Option<&Arc<View>>, env: Option<u64>, lo: &[u8], hi: Option<&[u8]>, reverse: bool, limit: Option<usize>) -> Result<Vec<(Bytes, Bytes)>> {
    if let Some(h) = hi {
        if lo >= h {
            return Ok(Vec::new()); // an empty range would panic the store
        }
    }
    let mut out = Vec::new();
    if limit == Some(0) {
        return Ok(out);
    }
    let range = match hi {
        Some(e) => (std::ops::Bound::Included(lo.to_vec()), std::ops::Bound::Excluded(e.to_vec())),
        None => (std::ops::Bound::Included(lo.to_vec()), std::ops::Bound::Unbounded),
    };
    // step 5b: SlateDB skips the SSTs whose rows are all at or below the held version's X_d
    let skip_opts;
    let opts = match view.and_then(|v| super::seqfilter::context(v.x_d)) {
        Some(c) => {
            skip_opts = ScanOptions { filter_context: Some(c), ..opts.clone() };
            &skip_opts
        }
        None => opts,
    };
    let mut it = src.scan_with_options(range, opts).await?;
    let full = |out: &Vec<(Bytes, Bytes)>| limit.is_some_and(|l| out.len() >= l);
    let Some(v) = view else {
        while let Some(kv) = it.next().await? {
            let Some(val) = crate::envelope::decode(env, &kv.key, kv.seq, kv.value)? else { continue };
            out.push((kv.key, val));
            if full(&out) {
                break;
            }
        }
        return Ok(out);
    };
    let x_d = v.x_d;
    let mut runs = v.cursor(lo, hi, reverse, opts.read_ahead_bytes);
    // SlateDB's next row above X_d (rows at or below X_d are answered by the runs)
    let mut s_head: Option<slatedb::KeyValue> = None;
    let mut s_done = false;
    let mut r_head: Option<super::view::RRow> = None;
    let mut r_done = false;
    if super::view::WAVE.load(std::sync::atomic::Ordering::Relaxed) {
        // one wave: SlateDB's first row and the runs' first rows at once
        let (s1, r1) = futures::future::try_join(async { Ok::<_, anyhow::Error>(it.next().await?) }, runs.next()).await?;
        match s1 {
            Some(kv) if kv.seq > x_d => s_head = Some(kv),
            Some(_) => {}
            None => s_done = true,
        }
        r_done = r1.is_none();
        r_head = r1;
    }
    loop {
        if r_head.is_none() && !r_done {
            r_head = runs.next().await?;
            r_done = r_head.is_none();
        }
        if s_head.is_none() && !s_done {
            loop {
                match it.next().await? {
                    Some(kv) if kv.seq > x_d => {
                        s_head = Some(kv);
                        break;
                    }
                    Some(kv) => {
                        // a row at or below X_d is ignored; once one lies past the runs' next row (SlateDB yields one row
                        // per key, in order), no SlateDB row above X_d comes before it: the runs' row goes first and the
                        // search goes on later. Without this a bounded read walked every such row to the end of its range
                        // (step 7b fix round: 4.7 s for a read of one row over 100,000 rows not yet dropped).
                        if let Some(r) = &r_head {
                            if control_full_walk() {
                                continue;
                            }
                            let past = if reverse { kv.key[..] < r.key[..] } else { kv.key[..] > r.key[..] };
                            if past {
                                break;
                            }
                        }
                        continue;
                    }
                    None => {
                        s_done = true;
                        break;
                    }
                }
            }
        }
        let take_s = match (&s_head, &r_head) {
            (None, None) => break,
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (Some(s), Some(r)) => {
                let c = s.key[..].cmp(&r.key[..]);
                if c == std::cmp::Ordering::Equal {
                    r_head = None; // SlateDB's row above X_d wins over every run row
                    true
                } else {
                    (c == std::cmp::Ordering::Less) != reverse
                }
            }
        };
        if take_s {
            let kv = s_head.take().unwrap();
            if let Some(val) = crate::envelope::decode(env, &kv.key, kv.seq, kv.value)? {
                out.push((kv.key, val));
            }
        } else {
            let mut r = r_head.take().unwrap();
            if !r.is_dead() {
                let k = Bytes::from(std::mem::take(&mut r.key));
                out.push((k, r.into_doc()));
            }
        }
        if full(&out) {
            break;
        }
    }
    Ok(out)
}

/// A transaction's snapshot: SlateDB's snapshot at S and the version held at begin, as one pair (rule 1). The version
/// was taken first, under the database's lock, so its X_d <= S; the transaction never re-reads the manifest
/// [TxnRereads], and checks its hold after every step (rule 5).
pub struct Snap {
    pub slate: Arc<slatedb::DbSnapshot>,
    pub hold: Option<Hold>,
    pub env: Option<u64>,
}

impl Snap {
    pub fn seq(&self) -> u64 {
        self.slate.seq()
    }

    /// P-fix-read-amplification: [`View::prefetch_points`] with the snapshot's version (a cache warm-up only).
    pub async fn prefetch_points(&self, keys: &[Vec<u8>]) {
        if let Some(v) = self.view() {
            let _ = v.prefetch_points(keys).await;
        }
    }

    fn view(&self) -> Option<&Arc<View>> {
        self.hold.as_ref().map(|h| &h.view)
    }

    fn check(&self) -> Result<()> {
        match &self.hold {
            Some(h) => h.check(),
            None => Ok(()),
        }
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let r = get(&*self.slate, &ReadOptions::default(), self.view().map(|v| &**v), self.env, key).await?;
        self.check()?;
        Ok(r)
    }

    pub async fn scan(&self, lo: &[u8], hi: Option<&[u8]>, reverse: bool, limit: Option<usize>, read_ahead: usize) -> Result<Vec<(Bytes, Bytes)>> {
        let opts = scan_options(read_ahead, reverse, false);
        let r = scan(&*self.slate, &opts, self.view(), self.env, lo, hi, reverse, limit).await?;
        self.check()?;
        Ok(r)
    }
}

/// The SlateDB readers the control layer opens on a checkpoint or a clone (export, PITR). They are only used through
/// `pair::PairedReader`, which reads them with the version paired with the checkpoint (rules 18-20, step 7a). They are
/// opened here so that the judge's raw-reader check (`judge/raw_readers.sh`) keeps every SlateDB reader in this module or
/// in `lifecycle.rs`.
pub async fn open_checkpoint_reader(path: String, store: Arc<dyn object_store::ObjectStore>, checkpoint: uuid::Uuid, opts: slatedb::config::DbReaderOptions) -> Result<slatedb::DbReader> {
    // A small cache of its own (perf-bytes fix round): an export reads each block once, and SlateDB's default reader
    // cache keeps up to 512 MiB of blocks, so the export's memory grew with its size (MEASURED with lz4 SSTs, where every
    // cached block is a decompressed copy: +160 MB for a 163 MB export; uncompressed, +70 MB). The metadata cache keeps
    // the SST indexes and filters between the export's pages.
    use slatedb::db_cache::foyer::{FoyerCache, FoyerCacheOptions};
    use slatedb::db_cache::DbCache;
    let meta: Arc<dyn DbCache> = Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions { max_capacity: EXPORT_META_CACHE_BYTES, ..Default::default() }));
    let cache: Arc<dyn DbCache> = Arc::new(slatedb::db_cache::SplitCache::new().with_block_cache(None).with_meta_cache(Some(meta)).build());
    let b = slatedb::DbReader::builder(path, store).with_reader_mode(slatedb::DbReaderMode::Checkpoint(checkpoint)).with_options(opts).with_db_cache(cache, 0);
    let b = match crate::lifecycle::filter_policies() {
        Some(p) => b.with_filter_policies(p),
        None => b,
    };
    Ok(b.build().await?)
}

/// The metadata cache of a checkpoint reader (SST indexes and filters between an export's pages; no block cache: an
/// export reads each block once).
pub const EXPORT_META_CACHE_BYTES: u64 = 32 << 20;

/// A reader that follows the latest state of a database (PITR's diff: the staging clone and the live database).
pub async fn open_follow_reader(path: String, store: Arc<dyn object_store::ObjectStore>, opts: slatedb::config::DbReaderOptions) -> Result<slatedb::DbReader> {
    let b = slatedb::DbReader::builder(path, store).with_reader_mode(slatedb::DbReaderMode::FollowLatest).with_options(opts);
    let b = match crate::lifecycle::filter_policies() {
        Some(p) => b.with_filter_policies(p),
        None => b,
    };
    Ok(b.build().await?)
}

/// The scan options of the server's range reads.
pub fn scan_options(read_ahead: usize, reverse: bool, durable: bool) -> ScanOptions {
    ScanOptions {
        read_ahead_bytes: read_ahead,
        max_fetch_tasks: crate::tiered::scan_fetch_tasks(read_ahead),
        cache_blocks: true,
        order: if reverse { slatedb::IterationOrder::Descending } else { slatedb::IterationOrder::Ascending },
        durability_filter: if durable { slatedb::config::DurabilityLevel::Remote } else { slatedb::config::DurabilityLevel::Memory },
        ..Default::default()
    }
}

