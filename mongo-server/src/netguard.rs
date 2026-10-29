//! Network limits of the data port: per-database lists of allowed client networks, limits on failed authentications
//! (temporary blocks per address and per address and database), connections at once per address, the handshake timeout
//! and the size of messages before authentication.
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
    /// Failed authentications from one address within --auth-fail-window-secs that block the address (on every database).
    #[arg(long, default_value_t = 10)]
    pub auth_fail_max: u32,
    /// Failed authentications from one address on one database within the window that block the address on that database.
    #[arg(long, default_value_t = 5)]
    pub auth_fail_max_per_db: u32,
    #[arg(long, default_value_t = 60)]
    pub auth_fail_window_secs: u64,
    /// Length of a first block; every new block of the same address (or address and database) doubles it.
    #[arg(long, default_value_t = 300)]
    pub auth_block_secs: u64,
    /// Longest block.
    #[arg(long, default_value_t = 3600)]
    pub auth_block_max_secs: u64,
    /// Without a new block for this long, the doubling starts again from --auth-block-secs.
    #[arg(long, default_value_t = 86400)]
    pub auth_block_forget_secs: u64,
    /// Connections at once from one address (0: no limit). Further connections are closed at once.
    #[arg(long, default_value_t = 200)]
    pub max_conns_per_ip: u32,
    /// The TLS handshake and the first message must arrive within this many seconds of the connection, and before
    /// authentication (with --auth) every message must arrive whole within it once its first byte has (0: off).
    #[arg(long, default_value_t = 10)]
    pub handshake_timeout_secs: u64,
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
}

impl Default for NetArgs {
    fn default() -> Self {
        NetArgs {
            trusted_networks: "127.0.0.0/8,::1/128".into(),
            auth_fail_max: 10,
            auth_fail_max_per_db: 5,
            auth_fail_window_secs: 60,
            auth_block_secs: 300,
            auth_block_max_secs: 3600,
            auth_block_forget_secs: 86400,
            max_conns_per_ip: 200,
            handshake_timeout_secs: 10,
            pre_auth_max_message_bytes: 65536,
            ip_allowlist_refresh_secs: 5,
            ipv6_group_prefix: 64,
            netguard_max_tracked: 100_000,
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
    handshake: Option<Duration>,
    pre_auth_max: usize,
    refresh: Duration,
    v6_prefix: u8,
    max_tracked: usize,
}

#[derive(Default)]
pub struct Totals {
    pub auth_failures: AtomicU64,
    pub refused_while_blocked: AtomicU64,
    pub refused_not_in_list: AtomicU64,
    pub blocks: AtomicU64,
    pub rejected_connections: AtomicU64,
    pub handshake_timeouts: AtomicU64,
    pub oversized_before_auth: AtomicU64,
}

type ListCache = HashMap<String, (Instant, Option<Arc<Vec<Net>>>)>;

pub struct NetGuard {
    cfg: Cfg,
    clients: Mutex<HashMap<IpAddr, Client>>,
    lists: Mutex<ListCache>,
    pub totals: Totals,
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
    GUARD.get_or_init(|| NetGuard::new(&NetArgs::default()).expect("default network settings"))
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
                handshake: (a.handshake_timeout_secs > 0).then(|| Duration::from_secs(a.handshake_timeout_secs)),
                pre_auth_max: a.pre_auth_max_message_bytes.max(1024),
                refresh: Duration::from_secs(a.ip_allowlist_refresh_secs),
                v6_prefix: a.ipv6_group_prefix,
                max_tracked: a.netguard_max_tracked.max(1),
            },
            clients: Mutex::new(HashMap::new()),
            lists: Mutex::new(HashMap::new()),
            totals: Totals::default(),
        })
    }

    pub fn handshake_timeout(&self) -> Option<Duration> {
        self.cfg.handshake
    }

    pub fn pre_auth_max(&self) -> usize {
        self.cfg.pre_auth_max
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

    /// A connection from `ip` is opening. `None`: over the limit, close it.
    pub fn open_connection(&'static self, ip: Option<IpAddr>) -> Option<ConnSlot> {
        let Some(ip) = ip else { return Some(ConnSlot { key: None }) };
        let k = self.key(ip);
        let limited = self.cfg.max_conns > 0 && !self.is_trusted(ip);
        let mut m = self.clients.lock().unwrap();
        let c = m.entry(k).or_default();
        if limited && c.conns >= self.cfg.max_conns {
            self.totals.rejected_connections.fetch_add(1, Relaxed);
            let now = Instant::now();
            if c.last_reject_log.map(|t| now.duration_since(t) >= Duration::from_secs(60)).unwrap_or(true) {
                c.last_reject_log = Some(now);
                eprintln!("netguard: connection from {} refused: {} connections already open from that address (limit {})", self.show_key(k), c.conns, self.cfg.max_conns);
            }
            return None;
        }
        c.conns += 1;
        Some(ConnSlot { key: Some(k) })
    }

    fn close_connection(&self, k: IpAddr) {
        let mut m = self.clients.lock().unwrap();
        if let Some(c) = m.get_mut(&k) {
            c.conns = c.conns.saturating_sub(1);
        }
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
        if c.dbs.len() >= 1000 && !c.dbs.contains_key(db) {
            c.dbs.retain(|_, t| !t.idle(now, &self.cfg));
            if c.dbs.len() >= 1000 {
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
                conns.push(json!({ "ip": ip, "open": c.conns }));
            }
        }
        let tracked = m.len();
        drop(m);
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
                "max_conns_per_ip": self.cfg.max_conns, "handshake_timeout_secs": self.cfg.handshake.map(|d| d.as_secs()).unwrap_or(0),
                "pre_auth_max_message_bytes": self.cfg.pre_auth_max, "ip_allowlist_refresh_secs": self.cfg.refresh.as_secs(), "ipv6_group_prefix": self.cfg.v6_prefix,
            },
            "blocked": blocked,
            "failing": failing,
            "connections": conns,
            "tracked_addresses": tracked,
            "totals": {
                "auth_failures": t.auth_failures.load(Relaxed), "refused_while_blocked": t.refused_while_blocked.load(Relaxed),
                "refused_not_in_list": t.refused_not_in_list.load(Relaxed), "blocks": t.blocks.load(Relaxed),
                "rejected_connections": t.rejected_connections.load(Relaxed), "handshake_timeouts": t.handshake_timeouts.load(Relaxed),
                "oversized_before_auth": t.oversized_before_auth.load(Relaxed),
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

/// Held for the life of a connection; frees its slot in the per-address count.
pub struct ConnSlot {
    key: Option<IpAddr>,
}

impl Drop for ConnSlot {
    fn drop(&mut self) {
        if let Some(k) = self.key {
            get().close_connection(k);
        }
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
        g.record_failure(a, "z");
        assert!(!g.is_blocked(a, "w"));
        g.record_failure(a, "w");
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
        // the process guard (defaults: 200 per address, loopback trusted)
        let g = get();
        let a = Some(ip("192.0.2.77"));
        let slots: Vec<_> = (0..200).map(|_| g.open_connection(a).expect("slot")).collect();
        assert!(g.open_connection(a).is_none());
        assert!(g.open_connection(Some(ip("192.0.2.78"))).is_some());
        drop(slots);
        assert!(g.open_connection(a).is_some());
        let local: Vec<_> = (0..300).map(|_| g.open_connection(Some(ip("127.0.0.1"))).expect("trusted")).collect();
        drop(local);
    }
}
