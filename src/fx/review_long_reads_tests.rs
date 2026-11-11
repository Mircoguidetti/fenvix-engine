//! Review of c2-fix-long-reads: attacks on the reader's per-step renewal (`Tenant::step_state`).
//!
//! - `review_lr_one_renewal_for_many_steps`: 32 steps at once on a reader due for renewal make one renewal; a long scan
//!   renews about twice per T_pin; an idle reader never renews. Store requests per renewal are counted.
//! - `review_lr_failed_renewal_keeps_the_guard`: a renewal that fails half way (the marker read, then the announcement
//!   HEAD or the version LIST fails) leaves the state as it was; steps are served until the state is T_pin old, then
//!   refused; when the store answers again the next step renews.
//! - `review_lr_shortest_legal_grace`: GC with the shortest grace `check_config` accepts (hold life + PUT timeout +
//!   1 ms), product ratios of T_pin, hold life and PUT timeout; a step that starts with a state younger than T_pin and
//!   lasts less than the hold life must never read with a state that lists a deleted run.
//! - `review_lr_writer_returns_with_envelopes`: the takeovers' case (E1 announced, no marker), and then a writer comes
//!   back, waits E1, writes the marker and enveloped rows and publishes while the reader pages through.
//! - `review_lr_upgrade_mid_scan_and_old_writer_returns`: the reader renews, becomes the writer between two steps of
//!   its scan, and the old writer comes back and tries to write and publish.
//! - `review_lr_pages_across_v2_to_v2k_reseals`: a reader pages through while every run is resealed from layout V2 to
//!   V2K (the server's customer framing).
//!
//! Every id read is compared with the writer's.

use super::compactor::{Compactor, Knobs};
use super::tests::*;
use super::*;
use crate::counting::CountingStore;
use crate::lifecycle::{Lifecycle, Tenant};
use crate::runfmt::{CustomerKind, RunOptions};
use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

// ---- a store that fails or delays chosen requests, and counts what a reader asks

#[derive(Debug, Default)]
struct Faults {
    /// LIST of the manifest directory fails.
    fail_list: AtomicBool,
    /// HEAD (or GET) of E1's announcement fails.
    fail_announce: AtomicBool,
    /// The answer to a HEAD of E1's announcement arrives this many ms after the store served it (0: at once).
    late_announce_ms: AtomicU64,
    announce_served: Mutex<Option<Instant>>,
    /// GETs of sealed or delta runs wait until this instant before they reach the store.
    gate_runs_until: Mutex<Option<Instant>>,
    gated: AtomicU64,
    run_not_found: AtomicU64,
    lists: AtomicU64,
    manifest_gets: AtomicU64,
    announce_heads: AtomicU64,
    run_gets: AtomicU64,
    other_gets: AtomicU64,
}

#[derive(Debug)]
struct FaultStore {
    inner: Arc<dyn ObjectStore>,
    f: Arc<Faults>,
}

impl std::fmt::Display for FaultStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FaultStore({})", self.inner)
    }
}

fn injected(p: &Path) -> object_store::Error {
    object_store::Error::Generic { store: "review", source: format!("injected failure on {p}").into() }
}

#[async_trait]
impl ObjectStore for FaultStore {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        let s = location.as_ref();
        if s.ends_with("/e1-announced") {
            self.f.announce_heads.fetch_add(1, Relaxed);
            if self.f.fail_announce.load(Relaxed) {
                return Err(injected(location));
            }
            let late = self.f.late_announce_ms.load(Relaxed);
            if late > 0 {
                let r = self.inner.get_opts(location, options).await;
                *self.f.announce_served.lock().unwrap() = Some(Instant::now());
                tokio::time::sleep(Duration::from_millis(late)).await;
                return r;
            }
        } else if s.contains("/fx/manifest/") {
            self.f.manifest_gets.fetch_add(1, Relaxed);
        } else if s.contains("/fx/run/") {
            self.f.run_gets.fetch_add(1, Relaxed);
            let gate = *self.f.gate_runs_until.lock().unwrap();
            if let Some(t) = gate {
                self.f.gated.fetch_add(1, Relaxed);
                tokio::time::sleep_until(tokio::time::Instant::from_std(t)).await;
            }
            let r = self.inner.get_opts(location, options).await;
            if matches!(r, Err(object_store::Error::NotFound { .. })) {
                self.f.run_not_found.fetch_add(1, Relaxed);
            }
            return r;
        } else {
            self.f.other_gets.fetch_add(1, Relaxed);
        }
        self.inner.get_opts(location, options).await
    }
    fn delete_stream(&self, locations: BoxStream<'static, object_store::Result<Path>>) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        if prefix.is_some_and(|p| p.as_ref().ends_with("/fx/manifest")) {
            self.f.lists.fetch_add(1, Relaxed);
            if self.f.fail_list.load(Relaxed) {
                let e = injected(prefix.unwrap());
                return Box::pin(futures::stream::once(async move { Err(e) }));
            }
        }
        self.inner.list(prefix)
    }
    fn list_with_offset(&self, prefix: Option<&Path>, offset: &Path) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> object_store::Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

/// A lifecycle (another process) whose requests go through a [`FaultStore`].
fn faulty_lc(store: &Arc<dyn ObjectStore>) -> (Arc<Lifecycle>, Arc<Faults>) {
    let f = Arc::new(Faults::default());
    let fs: Arc<dyn ObjectStore> = Arc::new(FaultStore { inner: Arc::clone(store), f: Arc::clone(&f) });
    (Lifecycle::new(Arc::new(CountingStore::new(fs)), None), f)
}

// ---- common setup (the owner's test knobs, `long_reads_tests.rs`)

const T_PIN: u64 = 300;
const HOLD_MS: u64 = 1500;
const CUSTOMERS: usize = 8;
const STABLE: usize = 200;

fn knobs(kind: CustomerKind) -> Knobs {
    Knobs {
        theta: 0.25,
        r3: true,
        r3_gets_per_gib: 470.0,
        r2_age: None,
        tier_t: 4,
        tier_unit: 2 << 10,
        absorb_min_l0: 1,
        absorb_max_l0: 8,
        local_max_bytes: 16 << 10,
        split_bytes: 40 << 10,
        merge_bytes: 18 << 10,
        min_seal_bytes: 8 << 10,
        sealed: RunOptions::unchecked(2048, 1 << 20, kind),
        delta: RunOptions::unchecked(1024, 1 << 20, kind),
        drop: true,
        fold_one_run: false,
        fold_max_runs: 6,
        fold_at_l0: 6,
        lease: Duration::from_millis(T_PIN + 100),
        remote_lease: Duration::from_millis(HOLD_MS + 600),
        serving_lease: Duration::from_millis(300),
        put_timeout: Duration::from_millis(250),
        gc_grace: Duration::from_millis(HOLD_MS + 800),
        version_retention: Duration::from_millis(0),
        gc_every: Duration::from_millis(300),
        tick: Duration::from_millis(10),
        reseal_parallel: 1,
        read_ahead: 64 << 10,
        derive: None,
    }
}

fn setup() {
    enable();
    T_PIN_MS.store(T_PIN, Relaxed);
    hold::HOLD_LIFE_MS.store(HOLD_MS, Relaxed);
    crate::lifecycle::L0_SST_BYTES_OVERRIDE.store(3 << 10, Relaxed);
    crate::lifecycle::L0_MAX_SSTS_OVERRIDE.store(32, Relaxed);
    crate::lifecycle::WRITER_POLL_SECS.store(1, Relaxed);
    local::set_store(Some(Arc::new(InMemory::new())));
    view::clear_cache();
}

fn teardown() {
    crate::lifecycle::L0_SST_BYTES_OVERRIDE.store(0, Relaxed);
    crate::lifecycle::L0_MAX_SSTS_OVERRIDE.store(0, Relaxed);
    crate::lifecycle::WRITER_POLL_SECS.store(30, Relaxed);
    hold::HOLD_LIFE_MS.store(120_000, Relaxed);
    T_PIN_MS.store(TEST_T_PIN_MS, Relaxed);
    E1_WAIT_MS.store(u64::MAX, Relaxed);
    E1_WAIT_TEST.store(false, Relaxed);
    CONTROL_NO_RENEW.store(false, Relaxed);
    PUT_TIMEOUT_MS.store(20_000, Relaxed);
    LEASE_MS.store(60_000, Relaxed);
    LEASE_MARGIN_MS.store(30_000, Relaxed);
    REMOTE_LEASE_MS.store(180_000, Relaxed);
    GC_GRACE_MS.store(600_000, Relaxed);
    local::set_store(None);
}

fn runs_of(v: &manifest::Version) -> Vec<String> {
    v.groups.iter().filter_map(|g| g.sealed.as_ref().map(|s| s.path.clone())).chain(v.delta.iter().map(|d| d.path.clone())).collect()
}

/// Key layout v2 as the server writes it (V2K): `u/` + keyenc string of the customer + collection path.
fn kkey(c: usize, i: usize) -> Vec<u8> {
    let mut k = b"u/".to_vec();
    k.push(4);
    k.extend(format!("c{c:02}").as_bytes());
    k.extend([0, 1]);
    k.extend(format!("msgs/d/{i:05}").as_bytes());
    k
}

fn range_lo_hi(key: fn(usize, usize) -> Vec<u8>) -> (Vec<u8>, Vec<u8>) {
    let lo = key(0, 0);
    let hi = key(CUSTOMERS, 0);
    let cut = "msgs/d/00000".len();
    (lo[..lo.len() - cut].to_vec(), hi[..hi.len() - cut].to_vec())
}

#[derive(Default, Debug)]
struct Pages {
    steps: u64,
    rows: u64,
    stable_seen: u64,
    wrong: u64,
    refused: u64,
    other_errors: u64,
    first_error: String,
    passes: u64,
    extra_seen: u64,
}

/// One pass of a paged scan (`page` rows per step) over customers 0..CUSTOMERS; stable rows must equal `want`, rows
/// whose key is in `extra` must equal it byte for byte, any other row is wrong. Every stable id must be seen.
async fn one_pass(r: &Tenant, key: fn(usize, usize) -> Vec<u8>, want: &BTreeMap<Vec<u8>, Vec<u8>>, extra: &Mutex<BTreeMap<Vec<u8>, Vec<u8>>>, page: usize, pause: Duration, p: &mut Pages) {
    let (lo, hi) = range_lo_hi(key);
    let mut start = lo;
    let mut seen = std::collections::HashSet::new();
    let mut tries = 0;
    loop {
        p.steps += 1;
        match r.scan_range(&start, Some(&hi), false, Some(page), 4096).await {
            Ok(rows) => {
                tries = 0;
                let done = rows.len() < page;
                for (k, v) in &rows {
                    p.rows += 1;
                    if let Some(w) = want.get(&k[..]) {
                        p.stable_seen += 1;
                        seen.insert(k.to_vec());
                        p.wrong += (&v[..] != &w[..]) as u64;
                    } else if let Some(w) = extra.lock().unwrap().get(&k[..]) {
                        p.extra_seen += 1;
                        p.wrong += (&v[..] != &w[..]) as u64;
                    } else {
                        p.wrong += 1;
                        if p.first_error.is_empty() {
                            p.first_error = format!("a row nobody wrote: {:?} = {:?}", String::from_utf8_lossy(k), String::from_utf8_lossy(&v[..v.len().min(40)]));
                        }
                    }
                }
                if let Some((k, _)) = rows.last() {
                    start = k.to_vec();
                    start.push(0);
                }
                if done {
                    p.wrong += want.keys().filter(|k| !seen.contains(*k)).count() as u64;
                    p.passes += 1;
                    return;
                }
            }
            Err(e) => {
                let msg = format!("{e:#}");
                if e.downcast_ref::<Retry>().is_some() {
                    p.refused += 1;
                } else {
                    p.other_errors += 1;
                }
                if p.first_error.is_empty() {
                    p.first_error = msg.chars().take(300).collect();
                }
                tries += 1;
                if tries > 200 {
                    return;
                }
            }
        }
        tokio::time::sleep(pause).await;
    }
}

async fn stable_db(store: &Arc<dyn ObjectStore>, id: &str, key: fn(usize, usize) -> Vec<u8>, opts: &build::BuildOpts) -> (Tenant, BTreeMap<Vec<u8>, Vec<u8>>) {
    let mut w = lc(store).create(id).await.unwrap();
    let mut want = BTreeMap::new();
    for c in 0..CUSTOMERS {
        for i in 0..STABLE {
            apply(&mut w, &mut want, vec![(key(c, i), Some(format!("stable-{c}-{i}-{}", "x".repeat(i % 40)).into_bytes()))]).await;
        }
    }
    publish(&mut w, opts).await;
    (w, want)
}

// ---- 1. one renewal for many steps; renewals per T_pin; requests per renewal

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn review_lr_one_renewal_for_many_steps() {
    let _s = SERIAL.lock().await;
    setup();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "fx-rlr-storm";
    let (mut w, want) = stable_db(&store, id, ckey, &small_opts(build::Reseal::All)).await;
    // more versions in the store: a renewal LISTs them all (`load_newest`)
    let mut churned = BTreeMap::new();
    for i in 0..30 {
        apply(&mut w, &mut churned, vec![(ckey(0, 9000 + i), Some(b"churn".to_vec()))]).await;
        publish(&mut w, &small_opts(build::Reseal::None)).await;
    }
    let versions = manifest::list_versions(&store, id).await.unwrap().len();
    let (rlc, f) = faulty_lc(&store);
    let mut r = rlc.wake(id).await.unwrap();
    r.fx_own_holds();
    r.fx_refresh().await.unwrap();
    let r = Arc::new(r);
    // (a) 32 steps at once, the state past half T_pin
    tokio::time::sleep(Duration::from_millis(T_PIN / 2 + 20)).await;
    let (l0, g0, h0, o0, n0) = (f.lists.load(Relaxed), f.manifest_gets.load(Relaxed), f.announce_heads.load(Relaxed), f.other_gets.load(Relaxed), RENEWALS.load(Relaxed));
    let mut js = Vec::new();
    for c in 0..32 {
        let r = Arc::clone(&r);
        js.push(tokio::spawn(async move { r.get(ckey(c % CUSTOMERS, c)).await }));
    }
    let mut errs = 0;
    for j in js {
        errs += j.await.unwrap().is_err() as u64;
    }
    let storm = RENEWALS.load(Relaxed) - n0;
    let (lists, mgets, heads, others) = (f.lists.load(Relaxed) - l0, f.manifest_gets.load(Relaxed) - g0, f.announce_heads.load(Relaxed) - h0, f.other_gets.load(Relaxed) - o0);
    eprintln!("review storm: 32 concurrent steps -> {storm} renewal(s), {errs} errors; requests: {lists} LIST of the manifest dir ({versions} versions), {mgets} manifest GET, {heads} announcement HEAD, {others} other GETs (SlateDB, includes the 32 reads)");
    // (b) idle: no step, no renewal
    let n1 = RENEWALS.load(Relaxed);
    tokio::time::sleep(Duration::from_millis(5 * T_PIN)).await;
    let idle = RENEWALS.load(Relaxed) - n1;
    // (c) a long scan of 10 T_pin with 4 concurrent pagers
    let n2 = RENEWALS.load(Relaxed);
    let l2 = f.lists.load(Relaxed);
    let t0 = Instant::now();
    let extra = Arc::new(Mutex::new(churned));
    let want = Arc::new(want);
    let mut js = Vec::new();
    for _ in 0..4 {
        let (r, want, extra) = (Arc::clone(&r), Arc::clone(&want), Arc::clone(&extra));
        js.push(tokio::spawn(async move {
            let mut p = Pages::default();
            while t0.elapsed() < Duration::from_millis(10 * T_PIN) {
                one_pass(&r, ckey, &want, &extra, 16, Duration::from_millis(5), &mut p).await;
            }
            p
        }));
    }
    let mut wrong = 0;
    let mut refused = 0;
    let mut steps = 0;
    for j in js {
        let p = j.await.unwrap();
        wrong += p.wrong;
        refused += p.refused + p.other_errors;
        steps += p.steps;
    }
    let el = t0.elapsed().as_millis() as f64;
    let scan = RENEWALS.load(Relaxed) - n2;
    eprintln!(
        "review storm: idle 5 T_pin -> {idle} renewals; long scan {:.1} T_pin, 4 pagers, {steps} steps -> {scan} renewals ({:.2} per T_pin), {} LISTs, wrong {wrong}, refused/errors {refused}",
        el / T_PIN as f64,
        scan as f64 / (el / T_PIN as f64),
        f.lists.load(Relaxed) - l2
    );
    assert_eq!(errs, 0);
    assert_eq!(storm, 1, "32 steps at once must make one renewal");
    assert_eq!(idle, 0, "an idle reader must not renew");
    assert_eq!(wrong, 0);
    assert_eq!(refused, 0);
    assert!(scan as f64 <= 2.0 * (el / T_PIN as f64) + 2.0, "renewals must stay about two per T_pin");
    drop(r);
    w.abandon().await;
    teardown();
}

// ---- 2. a renewal that fails half way

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn review_lr_failed_renewal_keeps_the_guard() {
    let _s = SERIAL.lock().await;
    for case in ["version-list-fails", "e1-announcement-fails"] {
        setup();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let id = format!("fx-rlr-fail-{case}");
        let e1 = case.starts_with("e1");
        let mut w = lc(&store).create(&id).await.unwrap();
        let mut want = BTreeMap::new();
        for c in 0..CUSTOMERS {
            for i in 0..STABLE {
                apply(&mut w, &mut want, vec![(ckey(c, i), Some(format!("stable-{c}-{i}").into_bytes()))]).await;
            }
        }
        if e1 {
            E1_WAIT_MS.store(3_600_000, Relaxed);
            E1_WAIT_TEST.store(true, Relaxed);
            assert!(w.fx_publish(&small_opts(build::Reseal::All), None).await.unwrap().is_none());
        } else {
            publish(&mut w, &small_opts(build::Reseal::All)).await;
        }
        w.abandon().await;
        let (rlc, f) = faulty_lc(&store);
        let mut r = rlc.wake(&id).await.unwrap();
        r.fx_own_holds();
        r.fx_refresh().await.unwrap();
        assert_eq!(r.fx_view().is_none(), e1);
        let r0 = RENEWALS.load(Relaxed);
        if e1 {
            f.fail_announce.store(true, Relaxed);
        } else {
            f.fail_list.store(true, Relaxed);
        }
        let t0 = Instant::now();
        let extra = Mutex::new(BTreeMap::new());
        let (mut served_after_tpin, mut refused, mut served, mut wrong, mut other) = (0u64, 0u64, 0u64, 0u64, 0u64);
        let mut first_refusal_ms = None;
        while t0.elapsed() < Duration::from_millis(2 * T_PIN) {
            let k = ckey(1, 7);
            match r.get(&k).await {
                Ok(v) => {
                    served += 1;
                    wrong += (v.as_deref() != want.get(&k).map(|v| &v[..])) as u64;
                    if t0.elapsed() > Duration::from_millis(T_PIN + 20) {
                        served_after_tpin += 1;
                    }
                }
                Err(e) if e.downcast_ref::<Retry>().is_some() => {
                    refused += 1;
                    first_refusal_ms.get_or_insert(t0.elapsed().as_millis());
                }
                Err(_) => other += 1,
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let renewals_failing = RENEWALS.load(Relaxed) - r0;
        f.fail_list.store(false, Relaxed);
        f.fail_announce.store(false, Relaxed);
        let mut p = Pages::default();
        one_pass(&r, ckey, &want, &extra, 64, Duration::ZERO, &mut p).await;
        let renewals_after = RENEWALS.load(Relaxed) - r0 - renewals_failing;
        eprintln!("review failed renewal ({case}): served {served} (after T_pin {served_after_tpin}), refused {refused} (first at {first_refusal_ms:?} ms, T_pin {T_PIN}), other errors {other}, wrong {wrong}, renewals while failing {renewals_failing}; store back: pass wrong {} refused {} errors {} ({}), renewals {renewals_after}", p.wrong, p.refused, p.other_errors, p.first_error);
        assert_eq!(wrong + p.wrong, 0);
        assert_eq!(other, 0);
        assert_eq!(renewals_failing, 0, "a failed renewal must not store a state");
        assert_eq!(served_after_tpin, 0, "a state older than T_pin served a step");
        assert!(refused > 0);
        assert_eq!(p.passes, 1);
        assert_eq!(p.refused + p.other_errors, 0, "the reader must renew once the store answers: {}", p.first_error);
        assert!(renewals_after >= 1);
        drop(r);
        teardown();
    }
}

// ---- 3. GC with the shortest legal grace

/// The product ratios (T_pin 30 s, hold life 120 s, PUT timeout 20 s) scaled down by 100, and the GC grace set to the
/// shortest value `check_config` accepts: hold life + PUT timeout + 1 ms. The reader learns version 1 at L; at once the
/// writer reseals every group and merges (version 2 retires every run of version 1) and GC runs every 20 ms. The
/// reader's renewals then fail (a LIST error), so it keeps serving with version 1 while that state is younger than
/// T_pin, as the fix allows. One step starts at L + T_pin - 25 ms (state younger than T_pin, `fx_fresh` passes) and
/// its run GETs answer late, 40 ms before the step's hold life ends, so the step lasts less than the hold life
/// (`Hold::check` passes). With `case = "renewing"` the store answers and the step starts with a renewed state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn review_lr_shortest_legal_grace() {
    let _s = SERIAL.lock().await;
    let mut results = Vec::new();
    for case in ["renewing", "renewal-fails"] {
        setup();
        local::set_store(None); // every run block comes from the store
        let (t_pin, hold, put) = (300u64, 1200u64, 200u64);
        T_PIN_MS.store(t_pin, Relaxed);
        hold::HOLD_LIFE_MS.store(hold, Relaxed);
        PUT_TIMEOUT_MS.store(put, Relaxed);
        LEASE_MS.store(t_pin + 100, Relaxed);
        LEASE_MARGIN_MS.store(50, Relaxed);
        REMOTE_LEASE_MS.store(hold + put + 1, Relaxed);
        GC_GRACE_MS.store(hold + put + 1, Relaxed);
        check_config().expect("a legal configuration");
        let mut k = knobs(CustomerKind::V2);
        k.lease = Duration::from_millis(t_pin + 100);
        k.put_timeout = Duration::from_millis(put);
        k.remote_lease = Duration::from_millis(hold + put + 1);
        k.gc_grace = Duration::from_millis(hold + put + 1);
        k.gc_every = Duration::from_millis(20);
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let id = format!("fx-rlr-grace-{case}");
        let (mut w, want) = stable_db(&store, &id, ckey, &small_opts(build::Reseal::All)).await;
        let comp = Compactor::start(&mut w, Arc::clone(&store), k.clone(), pool::Pool::new(2), tokio::runtime::Handle::current()).await.unwrap();
        comp.run();
        let (rlc, f) = faulty_lc(&store);
        let mut r = rlc.wake(&id).await.unwrap();
        r.fx_own_holds();
        let l = Instant::now();
        r.fx_refresh().await.unwrap();
        let first = runs_of(&r.fx_view().unwrap().meta);
        // version 2 at once: every group resealed, delta merged, so no run of version 1 is listed any more
        let mut m = BTreeMap::new();
        apply(&mut w, &mut m, vec![(ckey(0, 60_000), Some(b"churn".to_vec()))]).await;
        w.fx_sync();
        let v = comp.current().await;
        for gi in 0..v.groups.len() {
            let _ = comp.reseal_once(Some(gi)).await;
        }
        while comp.merge_once().await.unwrap_or(false) {}
        w.fx_sync();
        let p_ms = l.elapsed().as_millis();
        let now_listed = runs_of(&comp.current().await);
        let retired = first.iter().filter(|p| !now_listed.contains(p)).count();
        if case == "renewal-fails" {
            f.fail_list.store(true, Relaxed);
        }
        // GC keeps running in the writer (its own loop every 20 ms, plus this one)
        let stop = Arc::new(AtomicBool::new(false));
        let gc_task = {
            let (stop, comp) = (Arc::clone(&stop), Arc::clone(&comp));
            tokio::spawn(async move {
                while !stop.load(Relaxed) {
                    let _ = gc::gc_once(&comp).await;
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
        };
        // a step at L + T_pin/2 + 10 ms: due for renewal (it fails in the second case)
        tokio::time::sleep_until(tokio::time::Instant::from_std(l + Duration::from_millis(t_pin / 2 + 10))).await;
        let _ = r.get(ckey(2, 3)).await;
        // the long step: it starts just before its state is T_pin old; its run GETs answer at L + T_pin + hold - 150 ms
        tokio::time::sleep_until(tokio::time::Instant::from_std(l + Duration::from_millis(t_pin - 25))).await;
        view::clear_cache();
        let s_at = Instant::now();
        *f.gate_runs_until.lock().unwrap() = Some(s_at + Duration::from_millis(hold - 40));
        let s_ms = l.elapsed().as_millis();
        let (lo, hi) = range_lo_hi(ckey);
        let res = r.scan_range(&lo, Some(&hi), false, None, 4096).await;
        let e_ms = l.elapsed().as_millis();
        *f.gate_runs_until.lock().unwrap() = None;
        stop.store(true, Relaxed);
        gc_task.await.unwrap();
        let mut gone = 0;
        for p in &first {
            gone += matches!(store.head(&Path::from(p.as_str())).await, Err(object_store::Error::NotFound { .. })) as u64;
        }
        let (wrong, outcome) = match &res {
            Ok(rows) => {
                let mut wr = 0u64;
                let got: BTreeMap<Vec<u8>, Vec<u8>> = rows.iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect();
                for (k, v) in &want {
                    wr += (got.get(k) != Some(v)) as u64;
                }
                (wr, format!("answered {} rows", rows.len()))
            }
            Err(e) => (0, format!("error ({}): {}", if e.downcast_ref::<Retry>().is_some() { "retryable" } else { "NOT retryable" }, format!("{e:#}").chars().take(240).collect::<String>())),
        };
        let line = format!(
            "review shortest grace ({case}): T_pin {t_pin} ms, hold {hold} ms, PUT timeout {put} ms, grace {} ms; version 2 at L+{p_ms} ms retires {retired} of {} runs of version 1; step from L+{s_ms} to L+{e_ms} ms ({} ms, hold life {hold}), {} run GETs held, {} run GETs NotFound; GC deleted {gone} of version 1's runs; the step: {outcome}; wrong {wrong}",
            hold + put + 1,
            first.len(),
            e_ms - s_ms,
            f.gated.load(Relaxed),
            f.run_not_found.load(Relaxed),
        );
        eprintln!("{line}");
        results.push((case, res.is_ok(), wrong, f.run_not_found.load(Relaxed), (e_ms - s_ms) as u64, line));
        comp.stop().await;
        drop(r);
        w.abandon().await;
        teardown();
    }
    for (case, ok, wrong, nf, len, line) in &results {
        assert_eq!(*wrong, 0, "{line}");
        assert!(*len < 1200, "the step must stay under the hold life: {line}");
        assert!(*ok && *nf == 0, "{case}: a step younger than the hold life, with a state younger than T_pin, read with a state that lists a deleted run: {line}");
    }
}

// ---- 4. E1 announced with no marker, then a writer comes back

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn review_lr_writer_returns_with_envelopes() {
    let _s = SERIAL.lock().await;
    setup();
    E1_WAIT_MS.store(u64::MAX, Relaxed); // the product rule: E1's wait is T_pin
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "fx-rlr-e1-back";
    let mut w = lc(&store).create(id).await.unwrap();
    let mut want = BTreeMap::new();
    for c in 0..CUSTOMERS {
        for i in 0..STABLE {
            apply(&mut w, &mut want, vec![(ckey(c, i), Some(format!("stable-{c}-{i}").into_bytes()))]).await;
        }
    }
    // the old writer announces E1 and dies before its wait ends: announced, no marker
    assert!(w.fx_publish(&small_opts(build::Reseal::All), None).await.unwrap().is_none());
    w.abandon().await;
    let mut r = lc(&store).wake(id).await.unwrap();
    r.fx_own_holds();
    r.fx_refresh().await.unwrap();
    assert!(r.fx_view().is_none() && r.envelope_seq().is_none());
    let r = Arc::new(r);
    let extra = Arc::new(Mutex::new(BTreeMap::<Vec<u8>, Vec<u8>>::new()));
    let want = Arc::new(want);
    let done = Arc::new(AtomicBool::new(false));
    let pager = {
        let (r, want, extra, done) = (Arc::clone(&r), Arc::clone(&want), Arc::clone(&extra), Arc::clone(&done));
        tokio::spawn(async move {
            let mut p = Pages::default();
            while !done.load(Relaxed) {
                one_pass(&r, ckey, &want, &extra, 16, Duration::from_millis(10), &mut p).await;
            }
            one_pass(&r, ckey, &want, &extra, 16, Duration::from_millis(10), &mut p).await;
            p
        })
    };
    // a writer comes back (another process): it waits E1 (T_pin), writes the marker, writes enveloped rows, publishes
    tokio::time::sleep(Duration::from_millis(2 * T_PIN)).await;
    let mut w2 = lc(&store).wake(id).await.unwrap();
    w2.upgrade().await.unwrap();
    let t0 = Instant::now();
    let mut published = 0;
    let mut n = 0usize;
    while t0.elapsed() < Duration::from_millis(12 * T_PIN) {
        let mut m = BTreeMap::new();
        let k = ckey(n % CUSTOMERS, 70_000 + n);
        let v = format!("after-e1-{n}").into_bytes();
        // the row is announced to the checker before it is written: a read may see it as soon as it lands
        extra.lock().unwrap().insert(k.clone(), v.clone());
        apply(&mut w2, &mut m, vec![(k, Some(v))]).await;
        n += 1;
        if n % 10 == 0 {
            if w2.fx_publish(&small_opts(if published % 3 == 0 { build::Reseal::All } else { build::Reseal::None }), None).await.unwrap().is_some() {
                published += 1;
            }
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    done.store(true, Relaxed);
    let p = pager.await.unwrap();
    eprintln!(
        "review E1 writer returns: {} steps, {} passes, {} stable rows, {} rows written after E1 seen, wrong {}, refused {}, other errors {} ({}); writer: {n} rows, {published} versions, E_env {:?}; renewals {}",
        p.steps, p.passes, p.stable_seen, p.extra_seen, p.wrong, p.refused, p.other_errors, p.first_error, w2.envelope_seq(), RENEWALS.load(Relaxed)
    );
    assert!(w2.envelope_seq().is_some() && published > 0, "the writer must have started envelopes and published");
    assert_eq!(p.wrong, 0, "{}", p.first_error);
    assert_eq!(p.other_errors, 0, "{}", p.first_error);
    drop(r);
    w2.abandon().await;
    teardown();
}

// ---- 5. the reader becomes the writer mid-scan; the old writer comes back

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn review_lr_upgrade_mid_scan_and_old_writer_returns() {
    let _s = SERIAL.lock().await;
    setup();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "fx-rlr-upgrade";
    let (mut old, want) = stable_db(&store, id, ckey, &small_opts(build::Reseal::All)).await;
    let k = knobs(CustomerKind::V2);
    let comp = Compactor::start(&mut old, Arc::clone(&store), k.clone(), pool::Pool::new(2), tokio::runtime::Handle::current()).await.unwrap();
    comp.run();
    let mut r = lc(&store).wake(id).await.unwrap();
    r.fx_own_holds();
    r.fx_refresh().await.unwrap();
    let v0 = r.fx_view().unwrap().version;
    let n0 = RENEWALS.load(Relaxed);
    // the old writer publishes more versions while the reader pages (the reader renews to them)
    let extra = Mutex::new(BTreeMap::new());
    let mut p = Pages::default();
    let (lo, hi) = range_lo_hi(ckey);
    let mut start = lo.clone();
    let mut seen = std::collections::HashSet::new();
    let mut upgraded_at = None;
    let mut old_write_after = None;
    let mut old_publish_after = None;
    let t0 = Instant::now();
    let mut i = 0usize;
    loop {
        if i % 3 == 0 && upgraded_at.is_none() {
            let mut m = BTreeMap::new();
            apply(&mut old, &mut m, vec![(ckey(i % CUSTOMERS, 80_000 + i), Some(b"churn-old".to_vec()))]).await;
            old.fx_sync();
            let v = comp.current().await;
            let _ = comp.reseal_once(Some(i % v.groups.len().max(1))).await;
            old.fx_sync();
            extra.lock().unwrap().insert(ckey(i % CUSTOMERS, 80_000 + i), b"churn-old".to_vec());
        }
        i += 1;
        // the takeover: after 4 T_pin, between two steps of the scan, the reader becomes the writer
        if upgraded_at.is_none() && t0.elapsed() > Duration::from_millis(4 * T_PIN) {
            // `fx_view` shows the state of the last exclusive step; the renewed one is the tenant's private cell, so the
            // count of renewals stands for it here
            let renewed_v = Some(RENEWALS.load(Relaxed) - n0);
            let newest_before = manifest::load_newest(&store, id).await.unwrap().map(|v| v.version);
            r.upgrade().await.unwrap();
            assert!(r.fx_view().map(|v| v.version) > newest_before, "the new writer must fence above the newest version");
            upgraded_at = Some((p.steps, renewed_v, r.fx_view().map(|v| v.version)));
            // the old writer comes back: its write and its publish must not land in the new writer's database
            let wr = old.apply(vec![crate::lifecycle::Op::Put(ckey(0, 99_999), b"zombie".to_vec())]).await;
            old_write_after = Some(wr.is_ok());
            let pb = old.fx_publish(&small_opts(build::Reseal::All), None).await;
            old_publish_after = Some(format!("{:?}", pb.as_ref().map(|o| o.is_some()).map_err(|e| format!("{e:#}").chars().take(120).collect::<String>())));
        }
        p.steps += 1;
        match r.scan_range(&start, Some(&hi), false, Some(16), 4096).await {
            Ok(rows) => {
                let done = rows.len() < 16;
                for (k, v) in &rows {
                    if let Some(w) = want.get(&k[..]) {
                        p.stable_seen += 1;
                        seen.insert(k.to_vec());
                        p.wrong += (&v[..] != &w[..]) as u64;
                    } else if let Some(w) = extra.lock().unwrap().get(&k[..]) {
                        p.wrong += (&v[..] != &w[..]) as u64;
                    } else if &v[..] == b"zombie" {
                        p.wrong += 1;
                        p.first_error = "the old writer's write after the takeover was read".into();
                    } else {
                        p.wrong += 1;
                    }
                }
                if let Some((k, _)) = rows.last() {
                    start = k.to_vec();
                    start.push(0);
                }
                if done {
                    p.wrong += want.keys().filter(|k| !seen.contains(*k)).count() as u64;
                    p.passes += 1;
                    if upgraded_at.is_some() && p.passes >= 2 {
                        break;
                    }
                    start = lo.clone();
                    seen.clear();
                }
            }
            Err(e) => {
                p.other_errors += 1;
                if p.first_error.is_empty() {
                    p.first_error = format!("{e:#}").chars().take(300).collect();
                }
                if p.other_errors > 50 {
                    break;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    // the new writer's own state after the takeover is the newest in the store
    let newest = manifest::load_newest(&store, id).await.unwrap().map(|v| v.version);
    eprintln!(
        "review upgrade mid-scan: first version {v0}, at the upgrade (step, renewals so far, writer's version) {upgraded_at:?}; old writer after the takeover: write ok {old_write_after:?}, publish {old_publish_after:?}; newest in the store {newest:?}, new writer reads with {:?}; {} steps, {} passes, wrong {}, errors {} ({})",
        r.fx_view().map(|v| v.version),
        p.steps,
        p.passes,
        p.wrong,
        p.other_errors,
        p.first_error
    );
    assert_eq!(p.wrong, 0, "{}", p.first_error);
    assert_eq!(p.other_errors, 0, "{}", p.first_error);
    let (_, renewals_before_upgrade, wv) = upgraded_at.unwrap();
    assert!(renewals_before_upgrade.unwrap() > 0, "the reader must have renewed before the upgrade");
    assert!(wv.unwrap() > v0);
    comp.stop().await;
    old.abandon().await;
    r.abandon().await;
    teardown();
}

// ---- 6. pages across reseals from V2 to V2K

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn review_lr_pages_across_v2_to_v2k_reseals() {
    let _s = SERIAL.lock().await;
    setup();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "fx-rlr-v2k";
    // version 1: runs in layout V2 (the step 4 compactor's format before integration-3), keys as the server writes them
    let (mut w, want) = stable_db(&store, id, kkey, &small_opts(build::Reseal::All)).await;
    let comp = Compactor::start(&mut w, Arc::clone(&store), knobs(CustomerKind::V2K), pool::Pool::new(2), tokio::runtime::Handle::current()).await.unwrap();
    comp.run();
    let mut r = lc(&store).wake(id).await.unwrap();
    r.fx_own_holds();
    r.fx_refresh().await.unwrap();
    let first = runs_of(&r.fx_view().unwrap().meta);
    let r = Arc::new(r);
    let extra = Arc::new(Mutex::new(BTreeMap::new()));
    let want = Arc::new(want);
    let done = Arc::new(AtomicBool::new(false));
    let pager = {
        let (r, want, extra, done) = (Arc::clone(&r), Arc::clone(&want), Arc::clone(&extra), Arc::clone(&done));
        tokio::spawn(async move {
            let mut p = Pages::default();
            while !done.load(Relaxed) {
                one_pass(&r, kkey, &want, &extra, 16, Duration::from_millis(20), &mut p).await;
            }
            p
        })
    };
    let t0 = Instant::now();
    let mut i = 0usize;
    let mut reseals = 0;
    while t0.elapsed() < Duration::from_millis(15 * T_PIN) {
        let mut m = BTreeMap::new();
        // a new key each time (never rewritten), so a row read is either absent or exactly this value
        let k = kkey(i % CUSTOMERS, 50_000 + i);
        let v = format!("churn{i}").into_bytes();
        extra.lock().unwrap().insert(k.clone(), v.clone());
        apply(&mut w, &mut m, vec![(k, Some(v))]).await;
        i += 1;
        if i % 20 == 0 {
            w.fx_sync();
            let v = comp.current().await;
            for gi in 0..v.groups.len() {
                reseals += comp.reseal_once(Some(gi)).await.unwrap_or(false) as u64;
            }
            while comp.merge_once().await.unwrap_or(false) {}
            w.fx_sync();
            let _ = gc::gc_once(&comp).await;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    done.store(true, Relaxed);
    let p = pager.await.unwrap();
    let newest = comp.current().await;
    let mut gone = 0;
    for path in &first {
        gone += matches!(store.head(&Path::from(path.as_str())).await, Err(object_store::Error::NotFound { .. })) as u64;
    }
    eprintln!(
        "review V2 -> V2K: {} steps, {} passes, {} stable rows, wrong {}, refused {}, errors {} ({}); {reseals} reseals; GC deleted {gone} of {} runs of version 1; newest version {}",
        p.steps, p.passes, p.stable_seen, p.wrong, p.refused, p.other_errors, p.first_error, first.len(), newest.version
    );
    assert!(p.passes >= 2);
    assert!(gone > 0, "every V2 run must be retired and deleted under the scan");
    assert_eq!(p.refused + p.other_errors, 0, "{}", p.first_error);
    assert_eq!(p.wrong, 0, "{}", p.first_error);
    comp.stop().await;
    drop(r);
    w.abandon().await;
    teardown();
}

// ---- 7. a single step longer than the hold life still fails (DECISIONS 2026-11-09, unchanged by the fix)

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn review_lr_single_step_past_hold_life_still_fails() {
    let _s = SERIAL.lock().await;
    setup();
    local::set_store(None);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let id = "fx-rlr-hold";
    let (mut w, want) = stable_db(&store, id, ckey, &small_opts(build::Reseal::All)).await;
    let (rlc, f) = faulty_lc(&store);
    let mut r = rlc.wake(id).await.unwrap();
    r.fx_own_holds();
    r.fx_refresh().await.unwrap();
    let extra = Mutex::new(BTreeMap::new());
    // a long command of short steps first: 4 T_pin, renewing, never refused
    let mut p = Pages::default();
    let t0 = Instant::now();
    while t0.elapsed() < Duration::from_millis(4 * T_PIN) {
        one_pass(&r, ckey, &want, &extra, 16, Duration::from_millis(10), &mut p).await;
    }
    // then one step whose reads take longer than the hold life
    view::clear_cache();
    let s_at = Instant::now();
    *f.gate_runs_until.lock().unwrap() = Some(s_at + Duration::from_millis(HOLD_MS + 200));
    let (lo, hi) = range_lo_hi(ckey);
    let long = r.scan_range(&lo, Some(&hi), false, None, 4096).await;
    let long_ms = s_at.elapsed().as_millis();
    *f.gate_runs_until.lock().unwrap() = None;
    // and the command's retry of that step, short again
    let retry = r.scan_range(&lo, Some(&hi), false, None, 4096).await;
    let msg = match &long {
        Ok(rows) => format!("answered {} rows", rows.len()),
        Err(e) => format!("{} error: {}", if e.downcast_ref::<Retry>().is_some() { "retryable" } else { "NOT retryable" }, format!("{e:#}").chars().take(200).collect::<String>()),
    };
    eprintln!(
        "review single step past the hold life: short steps {} over {:.1} T_pin, wrong {}, refused {}; the long step ({long_ms} ms, hold life {HOLD_MS} ms): {msg}; its retry: {}",
        p.steps,
        t0.elapsed().as_millis() as f64 / T_PIN as f64,
        p.wrong,
        p.refused + p.other_errors,
        retry.as_ref().map(|r| format!("{} rows", r.len())).unwrap_or_else(|e| format!("{e:#}"))
    );
    assert_eq!(p.wrong + p.refused + p.other_errors, 0);
    let e = long.expect_err("a step longer than the hold life must fail");
    assert!(e.downcast_ref::<Retry>().is_some() && format!("{e:#}").contains("for more than"), "{e:#}");
    assert_eq!(retry.unwrap().len(), want.len());
    drop(r);
    w.abandon().await;
    teardown();
}

// ---- 8. note 5 on the renewal path: the window of a renewal starts before its first request

/// The 7a review's note 5 for a renewal (the owner's test of note 5 now runs with `CONTROL_NO_RENEW`, so it covers the
/// exclusive refresh only). Release E1 without sealed runs (`--envelopes write`): a reader follows E1's start. A step
/// renews; the store serves the renewal's HEAD before E1's announcement lands, and the answer arrives 200 ms later; the
/// writer announces meanwhile. Then the store fails the HEAD (renewals fail), so a step may only use the renewed state
/// while it is younger than T_pin counted from the renewal's request. At the request plus T_pin + 30 ms (before the
/// answer plus T_pin) the step must be refused. Control: `CONTROL_LOADED_AFTER_ANSWER` (the window counted from the
/// answer) serves it plain.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn review_lr_renewal_window_starts_before_its_request() {
    let _s = SERIAL.lock().await;
    let mut out = Vec::new();
    for control in [false, true] {
        setup();
        ENABLED.store(false, Relaxed);
        crate::envelope::START.store(true, Relaxed);
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let id = format!("fx-rlr-note5-{control}");
        let mut w = lc(&store).create(&id).await.unwrap();
        w.apply(vec![crate::lifecycle::Op::Put(b"doc/c/00001".to_vec(), b"a".to_vec())]).await.unwrap();
        // the writer's first write announced E1 (release E1 announces with its first write): start from a fresh store
        // state where the announcement is absent, as the follower first sees it
        let _ = store.delete(&manifest::announce_path(&id)).await;
        let (flc, f) = faulty_lc(&store);
        let mut r = flc.wake(&id).await.unwrap();
        r.fx_refresh().await.unwrap();
        tokio::time::sleep(Duration::from_millis(T_PIN / 2 + 20)).await;
        f.late_announce_ms.store(200, Relaxed);
        CONTROL_LOADED_AFTER_ANSWER.store(control, Relaxed);
        let t_req = Instant::now();
        let r = Arc::new(r);
        let step = {
            let r = Arc::clone(&r);
            tokio::spawn(async move { r.get(b"doc/c/00001".to_vec()).await })
        };
        while f.announce_served.lock().unwrap().is_none() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        // the store answered "not announced"; E1 is announced now
        manifest::announce(&store, &id).await.unwrap();
        let first = step.await.unwrap();
        let answered = t_req.elapsed();
        CONTROL_LOADED_AFTER_ANSWER.store(false, Relaxed);
        f.late_announce_ms.store(0, Relaxed);
        f.fail_announce.store(true, Relaxed);
        tokio::time::sleep(Duration::from_millis(T_PIN + 30).saturating_sub(t_req.elapsed())).await;
        let late = r.get(b"doc/c/00001".to_vec()).await;
        crate::envelope::START.store(false, Relaxed);
        ENABLED.store(true, Relaxed);
        let line = format!(
            "review note 5 on the renewal path (control {control}): first step {:?}, answer after {answered:?}; at request + T_pin + 30 ms: {}",
            first.as_ref().map(|v| v.is_some()).map_err(|e| format!("{e:#}")),
            match &late {
                Ok(_) => "served (plain)".to_string(),
                Err(e) => format!("refused: {}", format!("{e:#}").chars().take(160).collect::<String>()),
            }
        );
        eprintln!("{line}");
        out.push((control, answered, late.is_err(), late.err().map(|e| e.chain().any(|c| c.downcast_ref::<Retry>().is_some())), line));
        drop(r);
        w.abandon().await;
        teardown();
    }
    for (control, answered, refused, retry, line) in out {
        assert!(answered >= Duration::from_millis(200), "the answer was not late: {line}");
        if control {
            assert!(!refused, "the control must serve the step (the test must be able to see the bug): {line}");
        } else {
            assert!(refused && retry == Some(true), "past T_pin from its renewal's request the reader must refuse: {line}");
        }
    }
}
