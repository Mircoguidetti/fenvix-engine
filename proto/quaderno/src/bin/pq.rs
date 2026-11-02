//! pq: replica, proxy, scaricatore, generatori di carico e controlli del prototipo "quaderno".
use quaderno::{bench, client::Cluster, flusher, node, proxy};

// allocatore che restituisce al sistema la memoria liberata con un thread di sfondo (con glibc l'RSS restava alto dopo
// un arretrato; mimalloc lo restituiva ma costava il 27% del throughput dei commit e p99 di 16 ms nel banco 1b)
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[allow(non_upper_case_globals)]
#[export_name = "_rjem_malloc_conf"]
pub static malloc_conf: &[u8] = b"background_thread:true,dirty_decay_ms:2000,muzzy_decay_ms:2000\0";
use std::collections::HashMap;
use std::sync::Arc;

struct Args(HashMap<String, String>);
impl Args {
    fn parse(v: &[String]) -> Args {
        let mut m = HashMap::new();
        let mut i = 0;
        while i < v.len() {
            if let Some(k) = v[i].strip_prefix("--") {
                let val = v.get(i + 1).filter(|x| !x.starts_with("--")).cloned();
                if val.is_some() {
                    i += 1;
                }
                m.insert(k.to_string(), val.unwrap_or_else(|| "1".into()));
            }
            i += 1;
        }
        Args(m)
    }
    fn s(&self, k: &str, d: &str) -> String {
        self.0.get(k).cloned().unwrap_or_else(|| d.to_string())
    }
    fn u(&self, k: &str, d: u64) -> u64 {
        self.0.get(k).map(|x| x.parse().unwrap_or_else(|_| panic!("--{k}"))).unwrap_or(d)
    }
    fn f(&self, k: &str, d: f64) -> f64 {
        self.0.get(k).map(|x| x.parse().unwrap_or_else(|_| panic!("--{k}"))).unwrap_or(d)
    }
}

fn mem_total_mb() -> u64 {
    std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("MemTotal:")).and_then(|l| l.split_whitespace().nth(1)?.parse::<u64>().ok()))
        .map(|kb| kb / 1024)
        .unwrap_or(4096)
}

fn cluster(a: &Args) -> Arc<Cluster> {
    let addrs: Vec<String> = a.s("replicas", "127.0.0.1:7200,127.0.0.1:7201,127.0.0.1:7202").split(',').map(String::from).collect();
    Cluster::new(addrs, a.u("conns", 16) as usize)
}

fn rt(threads: usize) -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread().worker_threads(threads).enable_all().build().unwrap()
}

fn main() -> anyhow::Result<()> {
    let argv: Vec<String> = std::env::args().collect();
    let cmd = argv.get(1).cloned().unwrap_or_default();
    let a = Args::parse(&argv[2.min(argv.len())..]);
    match cmd.as_str() {
        "replica" => {
            let peers = a
                .s("peers", "")
                .split(',')
                .filter(|x| !x.is_empty())
                .map(|p| {
                    let (id, addr) = p.split_once('=').expect("--peers id=addr,...");
                    (id.parse().unwrap(), addr.to_string())
                })
                .collect();
            node::start(node::Config {
                id: a.u("id", 0) as u8,
                dir: a.s("dir", "/tmp/pq").into(),
                listen_peer: a.s("peer-listen", "127.0.0.1:7100"),
                listen_client: a.s("client-listen", "127.0.0.1:7200"),
                peers,
                window_ms: a.u("window-ms", 60_000),
                // tetto del quaderno: predefinito il 60% della RAM della macchina
                mem_cap: a.u("mem-cap-mb", mem_total_mb() * 6 / 10) << 20,
                elect_min_ms: a.u("elect-min-ms", 1000),
                elect_max_ms: a.u("elect-max-ms", 2000),
                hb_ms: a.u("hb-ms", 50),
                seg_bytes: a.u("seg-mb", 64) << 20,
                max_inflight: a.u("max-inflight", 200_000),
                zstd: std::env::var("PQ_ZSTD").ok().and_then(|x| x.parse().ok()).unwrap_or(0),
                zstd_main: std::env::var("PQ_ZSTD_MAIN").map(|x| x == "1").unwrap_or(false),
                disk_soft: a.u("disk-soft-mb", 0) << 20,
                disk_hard: a.u("disk-hard-mb", 0) << 20,
                disk_ro: a.u("disk-ro-mb", 0) << 20,
                alarm_s: a.u("alarm-s", 120),
            })?;
        }
        "proxy" => {
            let routes: Vec<proxy::Route> = serde_json::from_str(&std::fs::read_to_string(a.s("routes", "routes.json"))?)?;
            rt(a.u("threads", 2) as usize).block_on(proxy::run(routes, a.s("ctl", "ctl.json")));
        }
        "valbench" => bench::valbench(a.u("records", 2_000_000), a.u("keys", 5) as usize, a.u("dbs", 10_000), a.u("keyspace", 1_000_000), a.u("val", 200) as usize, a.u("window", 1_200_000)),
        "commit" => {
            let r = rt(a.u("threads", 2) as usize);
            r.block_on(async {
                let cl = cluster(&a);
                for c in a.s("conc", "32").split(',') {
                    bench::commit_bench(cl.clone(), c.parse().unwrap(), a.u("secs", 20), a.u("warm", 3), a.u("keys", 1) as usize, a.u("val", 200) as usize, a.u("dbs", 100)).await;
                }
            });
        }
        "mixed" => {
            let r = rt(a.u("threads", 3) as usize);
            r.block_on(bench::mixed(
                cluster(&a),
                bench::MixedCfg {
                    secs: a.u("secs", 120),
                    small_dbs: a.u("small-dbs", 10_000),
                    small_rate: a.f("small-rate", 1.0),
                    hot_rate: a.f("hot-rate", 20_000.0),
                    reads: a.f("reads", 100_000.0),
                    ryw: a.f("ryw", 1000.0),
                    val: a.u("val", 200) as usize,
                    hot_keys: a.u("hot-keys", 100_000),
                    small_keys: a.u("small-keys", 20),
                    report_every: a.u("report-every", 10),
                    read_lag_ms: a.u("read-lag-ms", 5),
                    witness_rate: a.f("witness-rate", 0.0),
                    witness_out: a.s("witness-out", ""),
                },
            ));
        }
        "lotti" => {
            let r = rt(a.u("threads", 2) as usize);
            r.block_on(bench::lotti(cluster(&a), a.f("rate", 4000.0), a.u("batch", 1) as usize, a.u("clients", 64) as usize, a.u("secs", 20), a.u("warm", 5),
                a.u("keys", 1) as usize, a.u("val", 200) as usize, a.u("dbs", 100), a.u("real", 0), a.f("reads", 0.0)));
        }
        "hot" => {
            let r = rt(a.u("threads", 2) as usize);
            r.block_on(bench::hot(cluster(&a), &a.s("mode", "blind"), a.u("clients", 64) as usize, a.u("secs", 30), a.u("max-attempts", 50) as u32));
        }
        "lin" => {
            let r = rt(a.u("threads", 2) as usize);
            r.block_on(bench::lin(cluster(&a), a.u("secs", 60), a.u("keys", 200), a.u("writers", 8) as usize, a.u("readers", 16) as usize, a.s("out", "lin.csv"), a.u("pause-us", 0)));
        }
        "esiste-race" => {
            let r = rt(a.u("threads", 2) as usize);
            r.block_on(bench::esiste_race(cluster(&a), a.u("secs", 60), a.u("keys", 4), a.u("incs", 16) as usize, a.u("readers", 4) as usize, a.s("out", "esiste.csv"), a.0.contains_key("senza-esiste")));
        }
        "check-lin" => {
            let iso = a.0.get("iso").map(|s| {
                let v: Vec<u64> = s.split(',').map(|x| x.parse().unwrap()).collect();
                (v[0] as usize, v[1], v[2])
            });
            bench::check_lin(&a.s("file", "lin.csv"), iso);
        }
        "failover" => {
            let r = rt(a.u("threads", 2) as usize);
            r.block_on(bench::failover(cluster(&a), a.u("secs", 90), a.u("dbs", 10_000), a.u("period-ms", 1000), a.s("out", "failover.csv")));
        }
        "flusher" => {
            let r = rt(a.u("threads", 2) as usize);
            let hot = if a.u("hot-db", 1) == 1 { Some(bench::db_id(999_999_999)) } else { None };
            r.block_on(flusher::run(
                cluster(&a),
                flusher::FlushCfg {
                    endpoint: a.s("s3", "http://127.0.0.1:9000"),
                    bucket: a.s("bucket", "pq-sst"),
                    groups: a.u("groups", 16),
                    age_ms: a.u("age-ms", 10_000),
                    interval_ms: a.u("interval-ms", 1000),
                    compact_every: a.u("compact-every", 10) as u32,
                    hot_db: hot,
                    size_bytes: a.u("size-kb", 1024) << 10,
                    pressure: a.f("pressure", 0.7),
                    pressure_rate: a.u("pressure-mbs", 64) << 20,
                },
            ))?;
        }
        "check-sst" => {
            let r = rt(2);
            let v = r.block_on(flusher::check_sst(&a.s("s3", "http://127.0.0.1:9000"), &a.s("bucket", "pq-sst"), a.u("groups", 16), bench::db_id(a.u("db", bench::WITNESS_DB)), &a.s("witness", "witness.csv")))?;
            println!("{v}");
        }
        "status" => {
            let r = rt(1);
            r.block_on(async {
                let cl = cluster(&a);
                for i in 0..cl.n() {
                    match cl.status(i).await {
                        Some(s) => println!("{}", serde_json::to_string(&s).unwrap()),
                        None => println!("{{\"replica\": {i}, \"down\": true}}"),
                    }
                }
            });
        }
        _ => {
            eprintln!("uso: pq replica|proxy|valbench|commit|mixed|hot|lin|check-lin|failover|flusher|status [--opzioni]");
            std::process::exit(2);
        }
    }
    Ok(())
}
