//! Network limits of the data port: per-database lists of allowed client networks, limits on failed authentications
//! (temporary blocks per address and per address and database), connections at once (all of them in the process, not yet
//! authenticated per address, authenticated per database, and optionally all of them per address), the handshake timeout,
//! the silence allowed to a connection that has not authenticated and the size of messages before authentication.
//!
//! Why the connection limits are split: the functions of a serverless platform (Vercel, Lambda) leave through a few shared
//! NAT addresses, so one address can carry thousands of legitimate, authenticated connections to many databases. What one
//! address must not do is hold many connections without ever authenticating (they cost memory and handshakes and prove
//! nothing): that is capped per address. Authenticated connections are capped per database, where they belong.
//!
//! The client address is always the TCP peer of the connection: no header, PROXY protocol line or client field is read
//! (nothing stands in front of the engine). IPv4-mapped IPv6 peers (`::ffff:a.b.c.d`) are treated as the IPv4 address.
//! Counters and blocks live in memory, per process.

use std::collections::{HashMap, VecDeque};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

/// Most entries a database's list may hold.
pub const MAX_ENTRIES: usize = 100;

#[derive(clap::Args, Debug, Clone)]
pub struct NetArgs {
    /// Networks that skip the per-database allow lists, the authentication blocks and the per-address connection limit
    /// (comma-separated CIDRs; an empty value trusts nothing). Meant for the processes on the same host (control plane,
    /// Data Explorer): loopback, and the host's container network if they run in containers, e.g.
    /// `127.0.0.0/8,::1/128,172.17.0.0/16`. Never put a network through which outside clients arrive here.
    #[arg(long, default_value = "127.0.0.0/8,::1/128")]
    pub trusted_networks: String,
    /// Failed authentications from one address within --auth-fail-window-secs that block the address on every database
    /// (the failure that reaches the number starts the block; 0: never). High on purpose: a platform with one database per
    /// customer connects from few addresses, and one wrong password rotation must not block all its databases.
    #[arg(long, default_value_t = 100)]
    pub auth_fail_max: u32,
    /// Failed authentications from one address on one database within --auth-fail-window-secs that block the address on
    /// that database only (0: never).
    #[arg(long, default_value_t = 5)]
    pub auth_fail_max_per_db: u32,
    /// Length of the sliding window, in seconds, in which failed authentications are counted for both thresholds.
    #[arg(long, default_value_t = 60)]
    pub auth_fail_window_secs: u64,
    /// Length of a first block, in seconds; every new block of the same address (or address and database) doubles it.
    #[arg(long, default_value_t = 300)]
    pub auth_block_secs: u64,
    /// Longest block, in seconds (the doubling stops here).
    #[arg(long, default_value_t = 3600)]
    pub auth_block_max_secs: u64,
    /// Without a new block for this many seconds, the doubling starts again from --auth-block-secs.
    #[arg(long, default_value_t = 86400)]
    pub auth_block_forget_secs: u64,
    /// Connections at once from one address, authenticated or not (0: no limit, the default). Further connections are closed
    /// at once. Off by default: serverless platforms reach the engine from a few shared addresses (see the module doc).
    #[arg(long, default_value_t = 0)]
    pub max_conns_per_ip: u32,
    /// Connections at once in this process, from every address (like mongod's net.maxIncomingConnections). Further
    /// connections are closed at once, before TLS; trusted addresses are counted but never refused. 0: no limit. Default:
    /// below the open-files limit read at start (RLIMIT_NOFILE), with room for the process's own files and sockets (S3,
    /// databases, admin port): the limit minus 1000, or half of it when it is under 4000. Without it, a client that holds
    /// enough connections (5 addresses at the per-address limit, or one with a password and a few databases) runs the
    /// process out of open files.
    #[arg(long)]
    pub max_conns: Option<u32>,
    /// Connections at once from one address that have not authenticated (0: no limit). Further connections are closed at
    /// once. A connection stops counting when it authenticates, and counts again if it logs out of every database. A
    /// driver keeps two such connections per client (monitoring and round-trip time), which never authenticate, and every
    /// new connection counts here until its authentication: the default leaves room for about 1,900 clients (for example
    /// warm serverless functions) behind one address. Measured: with 1000, 1000 clients behind one address do not start.
    #[arg(long, default_value_t = 4000)]
    pub max_unauth_conns_per_ip: u32,
    /// Authenticated connections at once on one database, from every address (0: no limit). An authentication that would go
    /// beyond it is refused (the connection stays open, not authenticated). A connection authenticated on a database
    /// counts once, whatever the number of its users there.
    #[arg(long, default_value_t = 5000)]
    pub max_conns_per_db: u32,
    /// The TLS handshake and the first message must arrive within this many seconds of the connection, and before
    /// authentication (with --auth) every message must arrive whole within it once its first byte has (0: off).
    #[arg(long, default_value_t = 10)]
    pub handshake_timeout_secs: u64,
    /// With --auth, a connection that has not authenticated (or has logged out of every database) and sends nothing for
    /// this many seconds is closed (0: off). A driver's monitoring connections, which never authenticate, send a hello at
    /// least every ~10 s (heartbeatFrequencyMS) and are not touched.
    #[arg(long, default_value_t = 120)]
    pub unauth_idle_timeout_secs: u64,
    /// Largest message (bytes, also after decompression) accepted before a connection has authenticated (with --auth).
    #[arg(long, default_value_t = 65536)]
    pub pre_auth_max_message_bytes: usize,
    /// A database's allow list is re-read from its metadata at most this often (changes made by this process apply at once).
    #[arg(long, default_value_t = 5)]
    pub ip_allowlist_refresh_secs: u64,
    /// IPv6 addresses are counted and limited by this prefix (one /64 is usually one client).
    #[arg(long, default_value_t = 64)]
    pub ipv6_group_prefix: u8,
    /// Most client addresses whose failures are tracked at once (memory bound).
    #[arg(long, default_value_t = 100_000)]
    pub netguard_max_tracked: usize,
    /// Most databases whose failures are tracked at once for one address (memory bound); beyond it, failures on further
    /// databases count only toward the per-address threshold.
    #[arg(long, default_value_t = 1000)]
    pub netguard_max_dbs_per_ip: usize,
}

impl Default for NetArgs {
    fn default() -> Self {
        NetArgs {
            trusted_networks: "127.0.0.0/8,::1/128".into(),
            auth_fail_max: 100,
            auth_fail_max_per_db: 5,
            auth_fail_window_secs: 60,
            auth_block_secs: 300,
            auth_block_max_secs: 3600,
            auth_block_forget_secs: 86400,
            max_conns_per_ip: 0,
            max_conns: None,
            max_unauth_conns_per_ip: 4000,
            max_conns_per_db: 5000,
            handshake_timeout_secs: 10,
            unauth_idle_timeout_secs: 120,
            pre_auth_max_message_bytes: 65536,
            ip_allowlist_refresh_secs: 5,
            ipv6_group_prefix: 64,
            netguard_max_tracked: 100_000,
            netguard_max_dbs_per_ip: 1000,
        }
    }
}

// ---------------------------------------------------------------------------------------------------- networks

/// An IPv4-mapped IPv6 address is the IPv4 address. (Not `to_ipv4`, which would also turn `::1` into `0.0.0.1`.)
pub fn canonical_ip(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map(IpAddr::V4).unwrap_or(ip),
        v4 => v4,
    }
}

/// A network in canonical form: IPv4-mapped networks are IPv4 ones, no bit is set after the prefix.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Net {
    addr: IpAddr,
    bits: u8,
}

fn mask_v4(a: Ipv4Addr, bits: u8) -> Ipv4Addr {
    if bits == 0 {
        return Ipv4Addr::UNSPECIFIED;
    }
    Ipv4Addr::from(u32::from(a) & (u32::MAX << (32 - bits as u32)))
}

fn mask_v6(a: Ipv6Addr, bits: u8) -> Ipv6Addr {
    if bits == 0 {
        return Ipv6Addr::UNSPECIFIED;
    }
    Ipv6Addr::from(u128::from(a) & (u128::MAX << (128 - bits as u32)))
}

impl Net {
    /// Strict parsing: `a.b.c.d`, `a.b.c.d/n`, an IPv6 address with or without `/n`. No spaces, zone ids, leading zeros in
    /// the prefix, signs, host bits after the prefix, or characters other than hex digits, `.`, `:` and `/`.
    pub fn parse(s: &str) -> Result<Net, String> {
        if s.is_empty() {
            return Err("is empty".into());
        }
        if s.len() > 49 {
            return Err("is too long to be a network".into());
        }
        if let Some(c) = s.chars().find(|c| !(c.is_ascii_hexdigit() || matches!(c, '.' | ':' | '/'))) {
            return Err(format!("contains a character that is not allowed ({:?})", c));
        }
        let (a, p) = match s.split_once('/') {
            Some((a, p)) => (a, Some(p)),
            None => (s, None),
        };
        let ip: IpAddr = a.parse().map_err(|_| format!("{a:?} is not an IPv4 or IPv6 address"))?;
        let max: u8 = if ip.is_ipv4() { 32 } else { 128 };
        let bits = match p {
            None => max,
            Some(p) => {
                if p.is_empty() || !p.bytes().all(|b| b.is_ascii_digit()) || (p.len() > 1 && p.starts_with('0')) || p.len() > 3 {
                    return Err(format!("has an invalid prefix length {p:?}"));
                }
                let n: u32 = p.parse().map_err(|_| format!("has an invalid prefix length {p:?}"))?;
                if n > max as u32 {
                    return Err(format!("has a prefix length /{n} larger than /{max}"));
                }
                n as u8
            }
        };
        let net = match ip {
            IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
                Some(v4) => {
                    if bits < 96 {
                        return Err(format!("is an IPv4-mapped network with a prefix /{bits}, it needs at least /96"));
                    }
                    Net { addr: IpAddr::V4(v4), bits: bits - 96 }
                }
                None => Net { addr: ip, bits },
            },
            _ => Net { addr: ip, bits },
        };
        let masked = match net.addr {
            IpAddr::V4(a) => IpAddr::V4(mask_v4(a, net.bits)),
            IpAddr::V6(a) => IpAddr::V6(mask_v6(a, net.bits)),
        };
        if masked != net.addr {
            return Err(format!("has address bits set after the prefix (the network would be {})", Net { addr: masked, bits: net.bits }));
        }
        Ok(net)
    }

    /// Does the network contain the (canonical) address? IPv4 networks hold IPv4 peers, IPv6 networks IPv6 peers.
    pub fn contains(&self, ip: IpAddr) -> bool {
        match (self.addr, canonical_ip(ip)) {
            (IpAddr::V4(n), IpAddr::V4(a)) => mask_v4(a, self.bits) == n,
            (IpAddr::V6(n), IpAddr::V6(a)) => mask_v6(a, self.bits) == n,
            _ => false,
        }
    }
}

impl std::fmt::Display for Net {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.addr, self.bits)
    }
}

/// A comma-separated list of networks (the trusted networks flag). Empty: none.
pub fn parse_list(s: &str) -> Result<Vec<Net>, String> {
    if s.is_empty() {
        return Ok(Vec::new());
    }
    s.split(',').enumerate().map(|(i, e)| Net::parse(e).map_err(|m| format!("entry {i} ({:?}) {m}", e))).collect()
}

/// Validation of an allow list given on the admin port: an array of at most 100 strings, each a valid network, no two
/// the same once in canonical form. Returns the canonical list or a message naming the faulty entry.
pub fn validate_entries(v: &serde_json::Value) -> Result<Vec<Net>, String> {
    let Some(arr) = v.as_array() else { return Err("`entries` must be an array of strings (CIDR networks or addresses)".into()) };
    if arr.len() > MAX_ENTRIES {
        return Err(format!("the list has {} entries; at most {MAX_ENTRIES} are allowed", arr.len()));
    }
    let mut out: Vec<Net> = Vec::with_capacity(arr.len());
    for (i, e) in arr.iter().enumerate() {
        let Some(s) = e.as_str() else { return Err(format!("entry {i} is not a string")) };
        let shown: String = s.chars().take(60).collect();
        let n = Net::parse(s).map_err(|m| format!("entry {i} ({shown:?}) {m}"))?;
        if let Some(j) = out.iter().position(|x| *x == n) {
            return Err(format!("entry {i} ({shown:?}) is the same network as entry {j} ({})", out[j]));
        }
        out.push(n);
    }
    Ok(out)
}

// ---------------------------------------------------------------------------------------------------- limits

/// Failures and blocks of one key (an address, or an address on one database).
#[derive(Default)]
struct Track {
    fails: VecDeque<Instant>,
    blocked_until: Option<Instant>,
    strike: u32,
    last_block: Option<Instant>,
}

impl Track {
    fn blocked(&self, now: Instant) -> bool {
        self.blocked_until.map(|u| u > now).unwrap_or(false)
    }

    /// Count one failure; returns the block length if this one starts a block.
    fn fail(&mut self, now: Instant, max: u32, cfg: &Cfg) -> Option<(Duration, u32)> {
        if max == 0 {
            return None;
        }
        while self.fails.front().map(|t| now.duration_since(*t) >= cfg.window).unwrap_or(false) {
            self.fails.pop_front();
        }
        self.fails.push_back(now);
        if (self.fails.len() as u32) < max {
            return None;
        }
        self.fails.clear();
        if self.last_block.map(|t| now.duration_since(t) >= cfg.forget).unwrap_or(true) {
            self.strike = 0;
        }
        self.strike += 1;
        let d = cfg.block.saturating_mul(1u32 << (self.strike - 1).min(20)).min(cfg.block_max);
        self.blocked_until = Some(now + d);
        self.last_block = Some(now);
        Some((d, self.strike))
    }

    fn idle(&self, now: Instant, cfg: &Cfg) -> bool {
        !self.blocked(now)
            && self.fails.back().map(|t| now.duration_since(*t) >= cfg.window).unwrap_or(true)
            && self.last_block.map(|t| now.duration_since(t) >= cfg.forget).unwrap_or(true)
    }
}

#[derive(Default)]
struct Client {
    all: Track,
    dbs: HashMap<String, Track>,
    conns: u32,
    /// Of `conns`, those not authenticated (trusted addresses are not counted).
    unauth: u32,
    last_reject_log: Option<Instant>,
}

struct Cfg {
    trusted: Vec<Net>,
    fail_max: u32,
    fail_max_db: u32,
    window: Duration,
    block: Duration,
    block_max: Duration,
    forget: Duration,
    max_conns: u32,
    max_unauth: u32,
    max_db: u32,
    /// All connections of the process (0: no limit).
    max_total: u32,
    handshake: Option<Duration>,
    unauth_idle: Option<Duration>,
    pre_auth_max: usize,
    refresh: Duration,
    v6_prefix: u8,
    max_tracked: usize,
    max_dbs: usize,
}

#[derive(Default)]
pub struct Totals {
    pub auth_failures: AtomicU64,
    pub refused_while_blocked: AtomicU64,
    pub refused_not_in_list: AtomicU64,
    pub blocks: AtomicU64,
    pub rejected_connections: AtomicU64,
    pub rejected_unauthenticated: AtomicU64,
    pub refused_db_full: AtomicU64,
    pub handshake_timeouts: AtomicU64,
    pub oversized_before_auth: AtomicU64,
    pub rejected_total_limit: AtomicU64,
    pub accept_errors: AtomicU64,
    pub idle_unauthenticated_closed: AtomicU64,
}

type ListCache = HashMap<String, (Instant, Option<Arc<Vec<Net>>>)>;

pub struct NetGuard {
    cfg: Cfg,
    clients: Mutex<HashMap<IpAddr, Client>>,
    /// Authenticated connections per database (entries at 0 are removed).
    per_db: Mutex<HashMap<String, u32>>,
    db_reject_log: Mutex<HashMap<String, Instant>>,
    lists: Mutex<ListCache>,
    /// Connections open in the process (every one holding a `ConnSlot`).
    open_total: AtomicU64,
    total_reject_log: Mutex<Option<Instant>>,
    idle_log: Mutex<Option<Instant>>,
    pub totals: Totals,
}

/// The open-files limit of the process (soft), if it can be read and is finite.
fn open_files_limit() -> Option<u64> {
    let mut r = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut r) } != 0 || r.rlim_cur == libc::RLIM_INFINITY {
        return None;
    }
    Some(r.rlim_cur as u64)
}

/// The default total of connections for an open-files limit: the limit minus 1000, or half of it under 4000 (the process
/// itself keeps tens to hundreds of files and sockets open). Without a readable limit: 1,000,000.
pub fn default_max_conns(nofile: Option<u64>) -> u32 {
    match nofile {
        None => 1_000_000,
        Some(n) if n >= 4000 => (n - 1000).min(1_000_000) as u32,
        Some(n) => (n / 2).max(1) as u32,
    }
}

/// Why an authentication is refused before its password is looked at.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    Allowed,
    Blocked,
    NotInList,
}

static GUARD: OnceLock<NetGuard> = OnceLock::new();

/// The process's guard (the defaults if `install` was not called).
pub fn get() -> &'static NetGuard {
    GUARD.get_or_init(|| {
        let mut a = NetArgs::default();
        if cfg!(test) {
            // the unit tests share this guard and run at once (thousands of slots each): no total limit for them
            a.max_conns = Some(0);
        }
        NetGuard::new(&a).expect("default network settings")
    })
}

pub fn install(args: &NetArgs) -> anyhow::Result<()> {
    let g = NetGuard::new(args).map_err(|e| anyhow::anyhow!("{e}"))?;
    GUARD.set(g).map_err(|_| anyhow::anyhow!("network limits installed twice"))?;
    tokio::spawn(async {
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
            get().prune(Instant::now());
        }
    });
    Ok(())
}

impl NetGuard {
    pub fn new(a: &NetArgs) -> Result<NetGuard, String> {
        let trusted = parse_list(&a.trusted_networks).map_err(|e| format!("--trusted-networks: {e}"))?;
        if a.ipv6_group_prefix == 0 || a.ipv6_group_prefix > 128 {
            return Err("--ipv6-group-prefix must be between 1 and 128".into());
        }
        Ok(NetGuard {
            cfg: Cfg {
                trusted,
                fail_max: a.auth_fail_max,
                fail_max_db: a.auth_fail_max_per_db,
                window: Duration::from_secs(a.auth_fail_window_secs.max(1)),
                block: Duration::from_secs(a.auth_block_secs),
                block_max: Duration::from_secs(a.auth_block_max_secs.max(a.auth_block_secs)),
                forget: Duration::from_secs(a.auth_block_forget_secs),
                max_conns: a.max_conns_per_ip,
                max_unauth: a.max_unauth_conns_per_ip,
                max_db: a.max_conns_per_db,
                max_total: a.max_conns.unwrap_or_else(|| default_max_conns(open_files_limit())),
                handshake: (a.handshake_timeout_secs > 0).then(|| Duration::from_secs(a.handshake_timeout_secs)),
                unauth_idle: (a.unauth_idle_timeout_secs > 0).then(|| Duration::from_secs(a.unauth_idle_timeout_secs)),
                pre_auth_max: a.pre_auth_max_message_bytes.max(1024),
                refresh: Duration::from_secs(a.ip_allowlist_refresh_secs),
                v6_prefix: a.ipv6_group_prefix,
                max_tracked: a.netguard_max_tracked.max(1),
                max_dbs: a.netguard_max_dbs_per_ip.max(1),
            },
            clients: Mutex::new(HashMap::new()),
            per_db: Mutex::new(HashMap::new()),
            db_reject_log: Mutex::new(HashMap::new()),
            lists: Mutex::new(HashMap::new()),
            open_total: AtomicU64::new(0),
            total_reject_log: Mutex::new(None),
            idle_log: Mutex::new(None),
            totals: Totals::default(),
        })
    }

    pub fn handshake_timeout(&self) -> Option<Duration> {
        self.cfg.handshake
    }

    pub fn pre_auth_max(&self) -> usize {
        self.cfg.pre_auth_max
    }

    pub fn unauth_idle_timeout(&self) -> Option<Duration> {
        self.cfg.unauth_idle
    }

    /// Connections open in the process now.
    pub fn open_connections(&self) -> u64 {
        self.open_total.load(Relaxed)
    }

    /// A connection that had not authenticated was closed for its silence (one log line a minute at most: an attacker
    /// can make thousands of them).
    pub fn note_idle_close(&self, conn_id: i64, peer: &str) {
        let now = Instant::now();
        let mut l = self.idle_log.lock().unwrap();
        if l.map(|t| now.duration_since(t) >= Duration::from_secs(60)).unwrap_or(true) {
            *l = Some(now);
            eprintln!("conn {conn_id}: closed: not authenticated and silent for {} s (peer {peer}; {} such closes so far, next message at most in 60 s)",
                self.cfg.unauth_idle.map(|d| d.as_secs()).unwrap_or(0), self.totals.idle_unauthenticated_closed.load(Relaxed));
        }
    }

    pub fn is_trusted(&self, ip: IpAddr) -> bool {
        self.cfg.trusted.iter().any(|n| n.contains(ip))
    }

    /// The key an address is counted under: IPv4 as it is, IPv6 by its group prefix.
    fn key(&self, ip: IpAddr) -> IpAddr {
        match canonical_ip(ip) {
            IpAddr::V6(a) => IpAddr::V6(mask_v6(a, self.cfg.v6_prefix)),
            v4 => v4,
        }
    }

    fn show_key(&self, k: IpAddr) -> String {
        match k {
            IpAddr::V6(_) if self.cfg.v6_prefix < 128 => format!("{k}/{}", self.cfg.v6_prefix),
            _ => k.to_string(),
        }
    }

    // ---- connections

    /// A connection from `ip` is opening (not authenticated yet). `None`: over a per-address limit, close it.
    pub fn open_connection(&'static self, ip: Option<IpAddr>) -> Option<Arc<ConnSlot>> {
        // the total of the process first: it is what keeps the open files from running out
        let trusted = ip.map(|i| self.is_trusted(i)).unwrap_or(false);
        let open = self.open_total.fetch_add(1, Relaxed) + 1;
        if self.cfg.max_total > 0 && open > self.cfg.max_total as u64 && !trusted {
            self.open_total.fetch_sub(1, Relaxed);
            self.totals.rejected_total_limit.fetch_add(1, Relaxed);
            self.totals.rejected_connections.fetch_add(1, Relaxed);
            let now = Instant::now();
            let mut l = self.total_reject_log.lock().unwrap();
            if l.map(|t| now.duration_since(t) >= Duration::from_secs(60)).unwrap_or(true) {
                *l = Some(now);
                let who = ip.map(|i| self.show_key(self.key(i))).unwrap_or_else(|| "(unknown)".into());
                eprintln!("netguard: connection from {who} refused: {} connections open in this process (limit {}, --max-conns)", open - 1, self.cfg.max_total);
            }
            return None;
        }
        let Some(ip) = ip else { return Some(Arc::new(ConnSlot::new(None, true))) };
        let k = self.key(ip);
        let mut m = self.clients.lock().unwrap();
        let c = m.entry(k).or_default();
        let over_all = !trusted && self.cfg.max_conns > 0 && c.conns >= self.cfg.max_conns;
        let over_unauth = !trusted && self.cfg.max_unauth > 0 && c.unauth >= self.cfg.max_unauth;
        if over_all || over_unauth {
            if over_unauth {
                self.totals.rejected_unauthenticated.fetch_add(1, Relaxed);
            }
            self.totals.rejected_connections.fetch_add(1, Relaxed);
            let now = Instant::now();
            if c.last_reject_log.map(|t| now.duration_since(t) >= Duration::from_secs(60)).unwrap_or(true) {
                c.last_reject_log = Some(now);
                if over_all {
                    eprintln!("netguard: connection from {} refused: {} connections already open from that address (limit {})", self.show_key(k), c.conns, self.cfg.max_conns);
                } else {
                    eprintln!("netguard: connection from {} refused: {} connections from that address have not authenticated (limit {})", self.show_key(k), c.unauth, self.cfg.max_unauth);
                }
            }
            drop(m);
            self.open_total.fetch_sub(1, Relaxed);
            return None;
        }
        c.conns += 1;
        if !trusted {
            c.unauth += 1;
        }
        Some(Arc::new(ConnSlot::new(Some(k), trusted)))
    }

    fn close_connection(&self, k: IpAddr, unauth: bool) {
        let mut m = self.clients.lock().unwrap();
        if let Some(c) = m.get_mut(&k) {
            c.conns = c.conns.saturating_sub(1);
            if unauth {
                c.unauth = c.unauth.saturating_sub(1);
            }
        }
    }

    /// The connection of address key `k` left (false) or re-entered (true) the not-authenticated count.
    fn set_unauth(&self, k: IpAddr, unauth: bool) {
        let mut m = self.clients.lock().unwrap();
        let c = m.entry(k).or_default();
        if unauth {
            c.unauth += 1;
        } else {
            c.unauth = c.unauth.saturating_sub(1);
        }
    }

    /// Take a slot of `db` for an authenticated connection; `Err(limit)` when the database is full.
    fn take_db(&self, db: &str, limited: bool) -> Result<(), u32> {
        let mut m = self.per_db.lock().unwrap();
        let n = m.entry(db.to_string()).or_insert(0);
        if limited && self.cfg.max_db > 0 && *n >= self.cfg.max_db {
            let cur = *n;
            if *n == 0 {
                m.remove(db);
            }
            drop(m);
            self.totals.refused_db_full.fetch_add(1, Relaxed);
            let now = Instant::now();
            let mut logs = self.db_reject_log.lock().unwrap();
            if logs.get(db).map(|t| now.duration_since(*t) >= Duration::from_secs(60)).unwrap_or(true) {
                if logs.len() >= 10_000 {
                    logs.clear();
                }
                logs.insert(db.to_string(), now);
                eprintln!("netguard: authentication on database {db} refused: {cur} connections already authenticated on it (limit {})", self.cfg.max_db);
            }
            return Err(self.cfg.max_db);
        }
        *n += 1;
        Ok(())
    }

    fn release_db(&self, db: &str) {
        let mut m = self.per_db.lock().unwrap();
        if let Some(n) = m.get_mut(db) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                m.remove(db);
            }
        }
    }

    /// Authenticated connections on `db` now.
    pub fn db_connections(&self, db: &str) -> u32 {
        self.per_db.lock().unwrap().get(db).copied().unwrap_or(0)
    }

    // ---- authentication

    /// Is `ip` blocked (on every database, or on `db`)? Trusted addresses never are.
    pub fn is_blocked(&self, ip: IpAddr, db: &str) -> bool {
        if self.is_trusted(ip) {
            return false;
        }
        let now = Instant::now();
        let m = self.clients.lock().unwrap();
        m.get(&self.key(ip)).map(|c| c.all.blocked(now) || c.dbs.get(db).map(|t| t.blocked(now)).unwrap_or(false)).unwrap_or(false)
    }

    /// A failed authentication from `ip` on `db`. Not counted for trusted addresses or while the address is blocked.
    pub fn record_failure(&self, ip: IpAddr, db: &str) {
        self.totals.auth_failures.fetch_add(1, Relaxed);
        if self.is_trusted(ip) {
            return;
        }
        let now = Instant::now();
        let k = self.key(ip);
        let mut m = self.clients.lock().unwrap();
        if !m.contains_key(&k) && m.len() >= self.cfg.max_tracked {
            Self::prune_map(&mut m, now, &self.cfg);
            if m.len() >= self.cfg.max_tracked {
                // still full: drop one address that is neither blocked nor connected
                if let Some(victim) = m.iter().find(|(_, c)| c.conns == 0 && !c.all.blocked(now) && c.dbs.values().all(|t| !t.blocked(now))).map(|(k, _)| *k) {
                    m.remove(&victim);
                } else {
                    return;
                }
            }
        }
        let c = m.entry(k).or_default();
        if c.all.blocked(now) || c.dbs.get(db).map(|t| t.blocked(now)).unwrap_or(false) {
            return;
        }
        let shown = self.show_key(k);
        if let Some((d, strike)) = c.all.fail(now, self.cfg.fail_max, &self.cfg) {
            self.totals.blocks.fetch_add(1, Relaxed);
            // the failures behind this block are paid for: the per-database counts start again
            for t in c.dbs.values_mut() {
                t.fails.clear();
            }
            eprintln!("netguard: client {shown} blocked for {} s (block {strike}) after {} failed authentications in {} s", d.as_secs(), self.cfg.fail_max, self.cfg.window.as_secs());
            return;
        }
        if c.dbs.len() >= self.cfg.max_dbs && !c.dbs.contains_key(db) {
            c.dbs.retain(|_, t| !t.idle(now, &self.cfg));
            if c.dbs.len() >= self.cfg.max_dbs {
                return;
            }
        }
        let t = c.dbs.entry(db.to_string()).or_default();
        if let Some((d, strike)) = t.fail(now, self.cfg.fail_max_db, &self.cfg) {
            self.totals.blocks.fetch_add(1, Relaxed);
            eprintln!("netguard: client {shown} blocked on database {db} for {} s (block {strike}) after {} failed authentications in {} s", d.as_secs(), self.cfg.fail_max_db, self.cfg.window.as_secs());
        }
    }

    pub fn note_refused_while_blocked(&self) {
        self.totals.refused_while_blocked.fetch_add(1, Relaxed);
    }

    /// Lift every block and forget every count of the address (its group for IPv6).
    pub fn unblock(&self, ip: IpAddr) -> bool {
        let mut m = self.clients.lock().unwrap();
        match m.get_mut(&self.key(ip)) {
            Some(c) => {
                c.all = Track::default();
                c.dbs.clear();
                true
            }
            None => false,
        }
    }

    /// Lift only the block (and forget the counts) of the pair (address, database): the address-wide block and the other
    /// databases' blocks stay. Returns whether the pair was tracked.
    pub fn unblock_db(&self, ip: IpAddr, db: &str) -> bool {
        let mut m = self.clients.lock().unwrap();
        m.get_mut(&self.key(ip)).map(|c| c.dbs.remove(db).is_some()).unwrap_or(false)
    }

    fn prune_map(m: &mut HashMap<IpAddr, Client>, now: Instant, cfg: &Cfg) {
        m.retain(|_, c| {
            c.dbs.retain(|_, t| !t.idle(now, cfg));
            c.conns > 0 || !c.all.idle(now, cfg) || !c.dbs.is_empty()
        });
    }

    pub fn prune(&self, now: Instant) {
        Self::prune_map(&mut self.clients.lock().unwrap(), now, &self.cfg);
        let refresh = self.cfg.refresh;
        self.lists.lock().unwrap().retain(|_, (t, _)| now.duration_since(*t) < refresh.max(Duration::from_secs(60)));
    }

    // ---- allow lists

    /// The decision for an authentication from `ip` on `db`, before any password is looked at.
    pub async fn verdict(&self, ip: Option<IpAddr>, db: &str) -> Verdict {
        if let Some(ip) = ip {
            if self.is_trusted(ip) {
                return Verdict::Allowed;
            }
            if self.is_blocked(ip, db) {
                self.note_refused_while_blocked();
                return Verdict::Blocked;
            }
        }
        match self.list_of(db).await {
            Ok(None) => Verdict::Allowed,
            Ok(Some(l)) if l.is_empty() => Verdict::Allowed,
            Ok(Some(l)) => {
                if ip.map(|ip| l.iter().any(|n| n.contains(ip))).unwrap_or(false) {
                    Verdict::Allowed
                } else {
                    self.totals.refused_not_in_list.fetch_add(1, Relaxed);
                    eprintln!("netguard: client {} not in the allow list of database {db}: authentication refused", ip.map(|i| i.to_string()).unwrap_or_else(|| "(unknown)".into()));
                    Verdict::NotInList
                }
            }
            Err(e) => {
                // the list could not be read and none is known: refuse (fail closed)
                eprintln!("netguard: allow list of database {db} could not be read ({e}): authentication refused");
                Verdict::NotInList
            }
        }
    }

    /// The list of a database: from the cache while fresh, else from its metadata.
    async fn list_of(&self, db: &str) -> anyhow::Result<Option<Arc<Vec<Net>>>> {
        if crate::ctl::meta::check_name(db).is_err() {
            return Ok(None); // admin, local, config, $external: no tenant, no list
        }
        // A fork in progress: the branch has no record yet, its source's list applies until the fork ends (ctl/fork.rs).
        if let Some(l) = crate::ctl::fork::list_while_forking(db) {
            return Ok(l);
        }
        let Some(ctl) = crate::ctl::get() else { return Ok(None) };
        let now = Instant::now();
        let cached = self.lists.lock().unwrap().get(db).cloned();
        if let Some((t, l)) = &cached {
            if now.duration_since(*t) < self.cfg.refresh {
                return Ok(l.clone());
            }
        }
        match crate::ctl::rec::get(&ctl.store, &crate::ctl::meta::meta_path(db)).await {
            Ok(m) => {
                let l = m.as_ref().and_then(|m| stored_entries(m)).transpose().map_err(|e| anyhow::anyhow!("stored list is not valid: {e}"))?.map(Arc::new);
                self.lists.lock().unwrap().insert(db.to_string(), (now, l.clone()));
                Ok(l)
            }
            Err(e) => match cached {
                Some((_, l)) => Ok(l), // keep using the last known list
                None => Err(e),
            },
        }
    }

    /// This process changed (or erased) a database's list: use the new one at once.
    pub fn set_cached(&self, db: &str, l: Option<Vec<Net>>) {
        self.lists.lock().unwrap().insert(db.to_string(), (Instant::now(), l.map(Arc::new)));
    }

    pub fn forget_cached(&self, db: &str) {
        self.lists.lock().unwrap().remove(db);
    }

    // ---- state for the admin port (addresses and counts only: no user names, no passwords)

    pub fn state(&self) -> serde_json::Value {
        use serde_json::json;
        let now = Instant::now();
        let wall = crate::ctl::rec::now_ms();
        let until = |u: Instant| {
            let rem = u.saturating_duration_since(now);
            (crate::ctl::rec::rfc3339(wall + rem.as_millis() as i64), (rem.as_millis() as u64).div_ceil(1000))
        };
        let m = self.clients.lock().unwrap();
        let mut blocked = Vec::new();
        let mut failing = Vec::new();
        let mut conns = Vec::new();
        for (k, c) in m.iter() {
            let ip = self.show_key(*k);
            let mut one = |db: Option<&str>, t: &Track| {
                if let Some(u) = t.blocked_until.filter(|u| *u > now) {
                    let (at, rem) = until(u);
                    blocked.push(json!({ "ip": ip, "database": db, "until": at, "remaining_secs": rem, "strike": t.strike }));
                }
                let n = t.fails.iter().filter(|f| now.duration_since(**f) < self.cfg.window).count();
                if n > 0 {
                    failing.push(json!({ "ip": ip, "database": db, "failures_in_window": n }));
                }
            };
            one(None, &c.all);
            for (db, t) in &c.dbs {
                one(Some(db), t);
            }
            if c.conns > 0 {
                conns.push(json!({ "ip": ip, "open": c.conns, "unauthenticated": c.unauth }));
            }
        }
        let tracked = m.len();
        drop(m);
        let mut per_db: Vec<serde_json::Value> = self.per_db.lock().unwrap().iter().map(|(d, n)| json!({ "database": d, "authenticated": n })).collect();
        per_db.sort_by_key(|v| std::cmp::Reverse(v["authenticated"].as_u64().unwrap_or(0)));
        per_db.truncate(200);
        failing.sort_by_key(|v| std::cmp::Reverse(v["failures_in_window"].as_u64().unwrap_or(0)));
        failing.truncate(200);
        conns.sort_by_key(|v| std::cmp::Reverse(v["open"].as_u64().unwrap_or(0)));
        conns.truncate(200);
        let t = &self.totals;
        json!({
            "config": {
                "trusted_networks": self.cfg.trusted.iter().map(|n| n.to_string()).collect::<Vec<_>>(),
                "auth_fail_max": self.cfg.fail_max, "auth_fail_max_per_db": self.cfg.fail_max_db, "auth_fail_window_secs": self.cfg.window.as_secs(),
                "auth_block_secs": self.cfg.block.as_secs(), "auth_block_max_secs": self.cfg.block_max.as_secs(), "auth_block_forget_secs": self.cfg.forget.as_secs(),
                "max_conns": self.cfg.max_total, "max_conns_per_ip": self.cfg.max_conns, "max_unauth_conns_per_ip": self.cfg.max_unauth, "max_conns_per_db": self.cfg.max_db, "handshake_timeout_secs": self.cfg.handshake.map(|d| d.as_secs()).unwrap_or(0),
                "unauth_idle_timeout_secs": self.cfg.unauth_idle.map(|d| d.as_secs()).unwrap_or(0),
                "pre_auth_max_message_bytes": self.cfg.pre_auth_max, "ip_allowlist_refresh_secs": self.cfg.refresh.as_secs(), "ipv6_group_prefix": self.cfg.v6_prefix,
                "netguard_max_tracked": self.cfg.max_tracked, "netguard_max_dbs_per_ip": self.cfg.max_dbs,
            },
            "blocked": blocked,
            "failing": failing,
            "connections": conns,
            "databases": per_db,
            "open_connections": self.open_total.load(Relaxed),
            "tracked_addresses": tracked,
            "totals": {
                "auth_failures": t.auth_failures.load(Relaxed), "refused_while_blocked": t.refused_while_blocked.load(Relaxed),
                "refused_not_in_list": t.refused_not_in_list.load(Relaxed), "blocks": t.blocks.load(Relaxed),
                "rejected_connections": t.rejected_connections.load(Relaxed), "rejected_unauthenticated": t.rejected_unauthenticated.load(Relaxed), "refused_db_full": t.refused_db_full.load(Relaxed), "handshake_timeouts": t.handshake_timeouts.load(Relaxed),
                "oversized_before_auth": t.oversized_before_auth.load(Relaxed),
                "rejected_total_limit": t.rejected_total_limit.load(Relaxed), "accept_errors": t.accept_errors.load(Relaxed),
                "idle_unauthenticated_closed": t.idle_unauthenticated_closed.load(Relaxed),
            },
        })
    }
}

/// The list stored in a database's metadata (`ip_allowlist`), if any.
pub fn stored_entries(meta: &serde_json::Value) -> Option<Result<Vec<Net>, String>> {
    let v = meta.get("ip_allowlist")?;
    if v.is_null() {
        return None;
    }
    Some(validate_entries(v))
}

/// Held for the life of a connection: its place in the per-address counts and, once it has authenticated, in the
/// per-database counts. Freed when dropped.
pub struct ConnSlot {
    key: Option<IpAddr>,
    trusted: bool,
    state: Mutex<SlotState>,
}

struct SlotState {
    /// Counted among the address's connections that have not authenticated.
    unauth: bool,
    /// Databases this connection holds an authenticated slot on.
    dbs: Vec<String>,
}

impl ConnSlot {
    fn new(key: Option<IpAddr>, trusted: bool) -> ConnSlot {
        ConnSlot { key, trusted, state: Mutex::new(SlotState { unauth: key.is_some() && !trusted, dbs: Vec::new() }) }
    }

    /// The connection authenticated on `db`. `Err(limit)`: the database already has its most authenticated connections;
    /// the authentication must be refused (nothing changes).
    pub fn authenticated_on(&self, db: &str) -> Result<(), u32> {
        let g = get();
        let mut st = self.state.lock().unwrap();
        if !st.dbs.iter().any(|d| d == db) {
            g.take_db(db, !self.trusted)?;
            st.dbs.push(db.to_string());
        }
        if st.unauth {
            st.unauth = false;
            if let Some(k) = self.key {
                g.set_unauth(k, false);
            }
        }
        Ok(())
    }

    /// The connection is no longer authenticated on `db` (logout, allow list change). With no database left it counts
    /// again as not authenticated (even beyond the limit: new connections from the address are then refused).
    pub fn left(&self, db: &str) {
        let g = get();
        let mut st = self.state.lock().unwrap();
        let Some(i) = st.dbs.iter().position(|d| d == db) else { return };
        st.dbs.swap_remove(i);
        g.release_db(db);
        if st.dbs.is_empty() && !st.unauth && !self.trusted {
            if let Some(k) = self.key {
                st.unauth = true;
                g.set_unauth(k, true);
            }
        }
    }
}

impl Drop for ConnSlot {
    fn drop(&mut self) {
        let g = get();
        let st = self.state.get_mut().unwrap();
        for db in st.dbs.drain(..) {
            g.release_db(&db);
        }
        if let Some(k) = self.key {
            g.close_connection(k, st.unauth);
        }
        g.open_total.fetch_sub(1, Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    fn n(s: &str) -> Net {
        Net::parse(s).unwrap()
    }

    #[test]
    fn parses_and_canonicalizes() {
        assert_eq!(n("10.0.0.0/8").to_string(), "10.0.0.0/8");
        assert_eq!(n("1.2.3.4").to_string(), "1.2.3.4/32");
        assert_eq!(n("2001:DB8::/32").to_string(), "2001:db8::/32");
        assert_eq!(n("2001:db8::1").to_string(), "2001:db8::1/128");
        assert_eq!(n("::ffff:10.0.0.0/104").to_string(), "10.0.0.0/8");
        assert_eq!(n("::ffff:1.2.3.4").to_string(), "1.2.3.4/32");
        assert_eq!(n("::ffff:0:0/96").to_string(), "0.0.0.0/0");
        assert_eq!(n("0.0.0.0/0").to_string(), "0.0.0.0/0");
        assert_eq!(n("::/0").to_string(), "::/0");
        assert_eq!(n("::1").to_string(), "::1/128");
    }

    #[test]
    fn refuses_bad_entries() {
        for bad in ["", " ", "10.0.0.0/33", "::/129", "10.0.0.1/8", "2001:db8::1/32", "300.1.1.1", "1.2.3", "010.0.0.1", "10.0.0.0/08", "10.0.0.0/+8",
            "10.0.0.0/-1", "10.0.0.0/", "/8", " 10.0.0.0/8", "10.0.0.0/8 ", "fe80::1%eth0", "::ffff:1.2.3.4/200", "::ffff:10.0.0.0/90", "abc", "*",
            "١٠.0.0.0/8", "1.2.3.4/32/32", "0x0a.0.0.1", "10.0.0.0/0008", "1:2:3:4:5:6:7:8:9"] {
            assert!(Net::parse(bad).is_err(), "{bad:?} accepted");
        }
        assert!(validate_entries(&serde_json::json!(["10.0.0.0/8", "::ffff:10.0.0.0/104"])).is_err());
        assert!(validate_entries(&serde_json::json!(["1.2.3.4", "1.2.3.4/32"])).is_err());
        let hundred: Vec<String> = (0..100).map(|i| format!("10.0.0.{i}")).collect();
        assert!(validate_entries(&serde_json::json!(hundred)).is_ok());
        let more: Vec<String> = (0..101).map(|i| format!("10.0.0.{i}")).collect();
        assert!(validate_entries(&serde_json::json!(more)).is_err());
        assert!(validate_entries(&serde_json::json!([5])).is_err());
        assert!(validate_entries(&serde_json::json!("10.0.0.0/8")).is_err());
    }

    #[test]
    fn matches_boundaries() {
        assert!(n("0.0.0.0/0").contains(ip("203.0.113.9")));
        assert!(n("0.0.0.0/0").contains(ip("::ffff:203.0.113.9")));
        assert!(!n("0.0.0.0/0").contains(ip("2001:db8::1")));
        assert!(n("::/0").contains(ip("2001:db8::1")));
        assert!(n("::/0").contains(ip("::1")));
        assert!(!n("::/0").contains(ip("203.0.113.9")));
        assert!(!n("::/0").contains(ip("::ffff:203.0.113.9")));      // a mapped peer is an IPv4 peer
        assert!(n("203.0.113.9/32").contains(ip("203.0.113.9")));
        assert!(!n("203.0.113.9/32").contains(ip("203.0.113.8")));
        assert!(n("203.0.113.9/32").contains(ip("::ffff:203.0.113.9")));
        assert!(n("::ffff:203.0.113.9").contains(ip("203.0.113.9")));
        assert!(n("2001:db8::1/128").contains(ip("2001:db8::1")));
        assert!(!n("2001:db8::1/128").contains(ip("2001:db8::2")));
        assert!(n("2001:db8::/32").contains(ip("2001:db8:ffff::1")));
        assert!(!n("2001:db8::/32").contains(ip("2001:db9::1")));
        assert!(n("::1/128").contains(ip("::1")));
        assert!(!n("::1/128").contains(ip("127.0.0.1")));
        assert!(!n("127.0.0.0/8").contains(ip("::1")));
        assert!(n("10.0.0.0/31").contains(ip("10.0.0.1")) && !n("10.0.0.0/31").contains(ip("10.0.0.2")));
        assert!(n("2001:db8::/127").contains(ip("2001:db8::1")) && !n("2001:db8::/127").contains(ip("2001:db8::2")));
        assert_eq!(canonical_ip(ip("::1")), ip("::1"));                 // not 0.0.0.1
        assert_eq!(canonical_ip(ip("::ffff:1.2.3.4")), ip("1.2.3.4"));
    }

    #[test]
    fn defaults_of_the_flags() {
        #[derive(clap::Parser)]
        struct P {
            #[command(flatten)]
            net: NetArgs,
        }
        let parsed = <P as clap::Parser>::parse_from(["x"]).net;
        let d = NetArgs::default();
        assert_eq!((parsed.auth_fail_max, parsed.auth_fail_max_per_db, parsed.auth_fail_window_secs), (100, 5, 60));
        assert_eq!((d.auth_fail_max, d.auth_fail_max_per_db, d.auth_fail_window_secs), (100, 5, 60));
        assert_eq!(format!("{parsed:?}"), format!("{d:?}"));
    }

    fn guard(f: impl FnOnce(&mut NetArgs)) -> NetGuard {
        let mut a = NetArgs::default();
        f(&mut a);
        NetGuard::new(&a).unwrap()
    }

    #[test]
    fn blocks_per_address_and_per_database() {
        let g = guard(|a| {
            a.auth_fail_max = 4;
            a.auth_fail_max_per_db = 2;
        });
        let a = ip("203.0.113.1");
        g.record_failure(a, "x");
        assert!(!g.is_blocked(a, "x"));
        g.record_failure(a, "x");
        assert!(g.is_blocked(a, "x") && !g.is_blocked(a, "y"));
        g.record_failure(a, "y");
        assert!(!g.is_blocked(a, "w"));
        g.record_failure(a, "z"); // the 4th failure of the address: blocked everywhere
        assert!(g.is_blocked(a, "w") && g.is_blocked(a, "anything"));
        assert!(!g.is_blocked(ip("203.0.113.2"), "x"));
        assert!(g.unblock(a) && !g.is_blocked(a, "x"));
    }

    #[test]
    fn ipv6_is_counted_by_its_prefix_and_trusted_is_never_blocked() {
        let g = guard(|a| a.auth_fail_max = 2);
        g.record_failure(ip("2001:db8:1:2::1"), "x");
        g.record_failure(ip("2001:db8:1:2::ffff"), "y");
        assert!(g.is_blocked(ip("2001:db8:1:2:aaaa::1"), "z"));
        assert!(!g.is_blocked(ip("2001:db8:1:3::1"), "z"));
        for _ in 0..50 {
            g.record_failure(ip("::1"), "x");
            g.record_failure(ip("127.0.0.1"), "x");
        }
        assert!(!g.is_blocked(ip("::1"), "x") && !g.is_blocked(ip("127.0.0.1"), "x"));
        // a mapped peer is the same client as its IPv4 address
        g.record_failure(ip("::ffff:198.51.100.7"), "x");
        g.record_failure(ip("198.51.100.7"), "x");
        assert!(g.is_blocked(ip("198.51.100.7"), "q"));
    }

    #[test]
    fn progressive_blocks() {
        let g = guard(|a| {
            a.auth_fail_max = 1;
            a.auth_block_secs = 10;
            a.auth_block_max_secs = 35;
        });
        let cfg = &g.cfg;
        let mut t = Track::default();
        let t0 = Instant::now();
        assert_eq!(t.fail(t0, 1, cfg).unwrap().0, Duration::from_secs(10));
        assert_eq!(t.fail(t0 + Duration::from_secs(11), 1, cfg).unwrap().0, Duration::from_secs(20));
        assert_eq!(t.fail(t0 + Duration::from_secs(40), 1, cfg).unwrap().0, Duration::from_secs(35));
        // after the forget period the doubling starts again
        assert_eq!(t.fail(t0 + Duration::from_secs(40 + 86400), 1, cfg).unwrap().0, Duration::from_secs(10));
    }

    #[test]
    fn connection_slots() {
        // the process guard (defaults: 4000 not authenticated per address, 5000 authenticated per database, no total per
        // address, loopback trusted)
        let g = get();
        let a = Some(ip("192.0.2.77"));
        let slots: Vec<_> = (0..4000).map(|_| g.open_connection(a).expect("slot")).collect();
        assert!(g.open_connection(a).is_none());
        assert!(g.open_connection(Some(ip("192.0.2.78"))).is_some());
        // one of them authenticates: its place among the not authenticated is free again
        slots[0].authenticated_on("cs_db").unwrap();
        let more = g.open_connection(a).expect("slot freed by the authentication");
        assert!(g.open_connection(a).is_none());
        // it logs out: it counts again (beyond the limit), so the next one is refused even after `more` closes
        drop(more);
        slots[0].left("cs_db");
        assert!(g.open_connection(a).is_none());
        drop(slots);
        assert!(g.open_connection(a).is_some());
        assert_eq!(g.db_connections("cs_db"), 0);
        let local: Vec<_> = (0..4500).map(|_| g.open_connection(Some(ip("127.0.0.1"))).expect("trusted")).collect();
        drop(local);
    }

    #[test]
    fn many_authenticated_connections_from_one_address() {
        // a NAT address of a serverless platform: 6000 connections, all authenticated, on 60 databases
        let g = get();
        let a = Some(ip("192.0.2.90"));
        let mut held = Vec::new();
        for i in 0..6000 {
            let s = g.open_connection(a).expect("authenticated connections do not count toward the per-address limit");
            s.authenticated_on(&format!("nat_db{}", i % 60)).unwrap();
            held.push(s);
        }
        assert_eq!(g.db_connections("nat_db7"), 100);
        // the same connection authenticating twice on one database (two users) counts once
        held[7].authenticated_on("nat_db7").unwrap();
        assert_eq!(g.db_connections("nat_db7"), 100);
        drop(held);
        assert_eq!(g.db_connections("nat_db7"), 0);
    }

    #[test]
    fn per_database_limit() {
        let g = get();
        let mut held = Vec::new();
        for i in 0..5000u32 {
            let s = g.open_connection(Some(IpAddr::V4(Ipv4Addr::from(0xC633_6400u32 + (i % 50))))).unwrap(); // 198.51.100.x
            s.authenticated_on("full_db").unwrap();
            held.push(s);
        }
        let extra = g.open_connection(Some(ip("198.51.100.200"))).unwrap();
        assert_eq!(extra.authenticated_on("full_db"), Err(5000));
        // refused: still not authenticated, still counted as such; another database is fine
        extra.authenticated_on("other_db").unwrap();
        // a trusted connection is counted but never refused
        let local = g.open_connection(Some(ip("127.0.0.1"))).unwrap();
        local.authenticated_on("full_db").unwrap();
        assert_eq!(g.db_connections("full_db"), 5001);
        drop(local);
        held.pop();
        let again = g.open_connection(Some(ip("198.51.100.201"))).unwrap();
        again.authenticated_on("full_db").unwrap();
        drop(held);
        drop(again);
        drop(extra);
        assert_eq!(g.db_connections("full_db"), 0);
        assert_eq!(g.db_connections("other_db"), 0);
    }

    #[test]
    fn total_limit_of_the_process() {
        let g = guard(|a| {
            a.max_conns = Some(3);
            a.max_unauth_conns_per_ip = 2;
        });
        let g: &'static NetGuard = Box::leak(Box::new(g));
        let mut held: Vec<_> = ["203.0.113.60", "203.0.113.61", "203.0.113.62"].iter().map(|a| std::mem::ManuallyDrop::new(g.open_connection(Some(ip(a))).unwrap())).collect();
        assert_eq!(g.open_connections(), 3);
        // full: refused from any outside address, before any per-address count
        assert!(g.open_connection(Some(ip("203.0.113.63"))).is_none());
        assert!(g.open_connection(None).is_none());
        assert_eq!(g.totals.rejected_total_limit.load(Relaxed), 2);
        assert_eq!(g.open_connections(), 3);
        // a trusted address is counted but never refused
        held.push(std::mem::ManuallyDrop::new(g.open_connection(Some(ip("127.0.0.1"))).unwrap()));
        assert_eq!(g.open_connections(), 4);
        // a refusal by the per-address limit gives its place back
        let g2: &'static NetGuard = Box::leak(Box::new(guard(|a| {
            a.max_conns = Some(10);
            a.max_unauth_conns_per_ip = 2;
        })));
        let a = Some(ip("203.0.113.70"));
        let h2: Vec<_> = (0..2).map(|_| std::mem::ManuallyDrop::new(g2.open_connection(a).unwrap())).collect();
        assert!(g2.open_connection(a).is_none());
        assert_eq!(g2.open_connections(), 2);
        assert_eq!(g2.totals.rejected_total_limit.load(Relaxed), 0);
        drop((held, h2));
    }

    #[test]
    fn total_limit_default_follows_the_open_files() {
        assert_eq!(default_max_conns(Some(20_000)), 19_000);
        assert_eq!(default_max_conns(Some(4000)), 3000);
        assert_eq!(default_max_conns(Some(3999)), 1999);
        assert_eq!(default_max_conns(Some(300)), 150);
        assert_eq!(default_max_conns(Some(1_048_576)), 1_000_000);
        assert_eq!(default_max_conns(None), 1_000_000);
        // the process guard of the unit tests has none (see `get`); a guard built from the defaults reads the limit
        let g = guard(|_| {});
        assert_eq!(g.cfg.max_total, default_max_conns(open_files_limit()));
        assert!(g.cfg.max_total > 0);
        assert_eq!(g.cfg.unauth_idle, Some(Duration::from_secs(120)));
        assert_eq!(guard(|a| a.max_conns = Some(0)).cfg.max_total, 0);
    }

    #[test]
    fn total_per_address_limit_when_set() {
        let g = guard(|a| {
            a.max_conns_per_ip = 3;
            a.max_unauth_conns_per_ip = 0;
        });
        let g: &'static NetGuard = Box::leak(Box::new(g));
        let k = ip("203.0.113.50");
        // the counting itself (the slots of a guard that is not the process's are not dropped through it)
        let held: Vec<_> = (0..3).map(|_| std::mem::ManuallyDrop::new(g.open_connection(Some(k)).unwrap())).collect();
        assert!(g.open_connection(Some(k)).is_none());
        assert_eq!(g.totals.rejected_connections.load(Relaxed), 1);
        assert_eq!(g.totals.rejected_unauthenticated.load(Relaxed), 0);
        drop(held);
    }
}
