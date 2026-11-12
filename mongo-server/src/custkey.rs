//! Customer keys and key layout v2 (compaction 2, step 2; plan section 3).
//!
//! A collection may declare its customer key, a field like a shard key: the `create` option `customerKey: "<field>"`
//! or `customerKey: {field: "<field>", order: "<time field>"}`, or the server option `--customer-key` for the
//! collections it names (`[db.]coll=field` or `[db.]coll=field:timefield`), applied when such a collection is still
//! empty. A customer key is never added to a collection that already has documents, and never to a capped,
//! time-series, clustered or system collection: with the option off, nothing changes for existing data.
//!
//! # Logical keys (what the rest of the server sees)
//!
//! Every other module keeps using its logical keys: `doc/{collection}/{document suffix}` for documents and
//! `idx/{collection}/{index}/{values}{document suffix}{u16}` for index entries. In a customer-keyed collection the
//! document suffix (`dsuf`) is
//!
//! `0x00` (the mark: a plain suffix starts with a type rank, never 0) · customer component · [time component] · `_id`
//!
//! where a component is the order-preserving, prefix-free index encoding of the value (`keyenc.rs`, ascending, no
//! collation; the customer value whole, so an array value is one component), and `_id` is its usual
//! `encode_sortable`. An index whose first field is the customer field (ascending, a regular index, no collation) is
//! "co-located": its logical prefix is `idx/{collection}/0x00{index}/`, recorded in its stored definition when it is
//! created (`IndexDef::colocated`).
//!
//! # Physical keys (what is stored)
//!
//! The storage boundary (`dbs.rs`) maps those two kinds of logical keys, and only those, to the v2 layout:
//!
//! - `doc/{coll}/0x00{customer}{rest}`            -> `u/{customer}{coll}/d/{rest}`
//! - `idx/{coll}/0x00{index}/{values}{suffix}{n}` -> `u/{lead}{coll}/x/{index}/{values}{suffix}{n}`
//!
//! `{lead}` is the first component of the entry's values, which for a co-located index is the customer value of the
//! entry (for a document whose customer value is an array, each entry goes under its own element's customer). So one
//! customer's documents and co-located index entries form one contiguous key range, across collections (plan:
//! `u/{customer}/{collection}/d/{_id}` and `u/{customer}/{collection}/x/{index id}/{values}{_id}`). Collections without
//! a customer key, and indexes not led by the customer field, stay in the shared keyspace exactly as before.
//!
//! The mapping is a pure function of the key in both directions ([`to_phys`], [`to_logical`]), and it preserves order
//! inside one collection's documents and inside one index: the customer component leads both the logical tail and the
//! physical key, and components are prefix-free. So a logical range read of one collection or one index is the
//! physical range between the mapped bounds, restricted to that collection or index ([`Seg`]); the reader skips the
//! keys of other collections of the same customers by seeking.
//!
//! # `_id`
//!
//! `_id` is unique per customer, not per collection (founder, 2026-11-07, like a sharded collection whose shard key is
//! not `_id`): the same `_id` under two customers is accepted, under one customer refused with the duplicate-key error
//! (11000) of MongoDB. The locator `loc/{coll}/{n:u32}{_id}{customer}` -> document suffix, written in the same batch as
//! the document, is the per-customer unique `_id` index and answers lookups by `_id` alone (a prefix read on
//! `loc/{coll}/{n}{_id}`, every customer holding that `_id`). It stays in the shared keyspace (plan section 3).

use bson::{Bson, Document};

/// Documents written customer first, and `_id` lookups through the locator, since the start (serverStatus
/// `fenvix.customerKeys`).
pub static DOCS_WRITTEN: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static LOCATOR_LOOKUPS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Where the customer key lives in the collection's stored options (`meta/copt/`), hidden from `listCollections`.
pub const RAW_KEY: &str = "fenvixCustomerKey";
/// The optional time field of the customer key (stored next to [`RAW_KEY`]).
pub const RAW_ORDER: &str = "fenvixCustomerOrder";

/// The first byte of a customer-keyed document suffix and of a co-located index name in a logical key.
pub const MARK: u8 = 0;

/// The customer key of a collection: the customer field and the optional time field that orders a customer's
/// documents (`customerKey: {field, order}`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ckey {
    pub field: String,
    pub order: Option<String>,
}

/// `--customer-key`: (database or `None` for any, collection, key).
static CONFIG: std::sync::OnceLock<Vec<(Option<String>, String, Ckey)>> = std::sync::OnceLock::new();

fn good_field(f: &str) -> bool {
    !f.is_empty() && !f.starts_with('$')
}

/// Parse `--customer-key`: comma separated `[db.]collection=field[:timefield]`.
pub fn configure(spec: &str) -> anyhow::Result<()> {
    let mut v = Vec::new();
    for part in spec.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let (ns, key) = part.split_once('=').ok_or_else(|| anyhow::anyhow!("--customer-key: '{part}' is not [db.]collection=field[:timefield]"))?;
        let (field, order) = match key.split_once(':') {
            Some((f, o)) => (f, Some(o.to_string())),
            None => (key, None),
        };
        anyhow::ensure!(good_field(field) && order.as_deref().map_or(true, good_field), "--customer-key: bad field in '{part}'");
        let (db, coll) = match ns.split_once('.') {
            Some((d, c)) => (Some(d.to_string()), c.to_string()),
            None => (None, ns.to_string()),
        };
        v.push((db, coll, Ckey { field: field.to_string(), order }));
    }
    let _ = CONFIG.set(v);
    Ok(())
}

/// integration-4: the default customer keys. Every agent checkpoint collection of `--agent-checkpoints` (by default the
/// LangGraph saver's `checkpoints` and `checkpoint_writes`) is customer-keyed by `thread_id` unless `--customer-key`
/// names that same `[db.]collection` itself: the command line wins. Derived documents (`agentck.rs`) apply only to a
/// collection keyed by `thread_id`, and the default `--customer-key` list is empty, so without this they never applied
/// with the product defaults (sessions/2026-11-11-bench-local/REPORT.md, section 5). A collection takes its key only
/// when it is created, first indexed or first written while empty: existing data never changes layout.
/// Returns the spec to give to [`configure`]: the command line's entries first, then the defaults it does not name.
pub fn with_agent_defaults(spec: &str, agent_checkpoints: &str) -> String {
    let named: Vec<String> = spec.split(',').map(str::trim).filter(|p| !p.is_empty())
        .filter_map(|p| p.split_once('=').map(|(ns, _)| ns.trim().to_string())).collect();
    let mut out: Vec<String> = spec.split(',').map(str::trim).filter(|p| !p.is_empty()).map(String::from).collect();
    for ns in agent_checkpoints.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let coll = ns.split_once('.').map_or(ns, |(_, c)| c);
        // named for this collection in any form (`coll`, `db.coll`, or the same `db.coll`): the command line decides
        if named.iter().any(|n| n == ns || n == coll || n.split_once('.').is_some_and(|(_, c)| c == coll)) {
            continue;
        }
        out.push(format!("{ns}=thread_id"));
    }
    out.join(",")
}

/// The customer key `--customer-key` asks for `db.coll`, if any.
pub fn configured(db: &str, coll: &str) -> Option<Ckey> {
    CONFIG.get()?.iter().find(|(d, c, _)| c == coll && d.as_deref().map_or(true, |d| d == db)).map(|(_, _, k)| k.clone())
}

/// The `customerKey` option of `create`: a field name, or `{field, order}`.
pub fn parse_option(v: &Bson) -> Result<Ckey, String> {
    match v {
        Bson::String(f) if good_field(f) => Ok(Ckey { field: f.clone(), order: None }),
        Bson::Document(d) => {
            let field = match d.get("field") {
                Some(Bson::String(f)) if good_field(f) => f.clone(),
                _ => return Err(format!("customerKey.field must be a field name, not {:?}", d.get("field"))),
            };
            let order = match d.get("order") {
                None | Some(Bson::Null) => None,
                Some(Bson::String(o)) if good_field(o) && *o != field => Some(o.clone()),
                Some(o) => return Err(format!("customerKey.order must be a field name other than the customer field, not {o}")),
            };
            if let Some(k) = d.keys().find(|k| !matches!(k.as_str(), "field" | "order")) {
                return Err(format!("customerKey: unknown field '{k}'"));
            }
            Ok(Ckey { field, order })
        }
        v => Err(format!("customerKey must be a field name or {{field, order}}, not {v}")),
    }
}

/// Put a customer key into stored collection options.
pub fn store(raw: &mut Document, k: &Ckey) {
    raw.insert(RAW_KEY, k.field.clone());
    match &k.order {
        Some(o) => {
            raw.insert(RAW_ORDER, o.clone());
        }
        None => {
            raw.remove(RAW_ORDER);
        }
    }
}

/// The customer key in stored collection options.
pub fn from_raw(raw: &Document) -> Option<Ckey> {
    let field = raw.get_str(RAW_KEY).ok()?.to_string();
    Some(Ckey { field, order: raw.get_str(RAW_ORDER).ok().map(str::to_string) })
}

/// May this collection take a customer key at all?
pub fn allowed(coll: &str, raw: &Document) -> bool {
    !coll.starts_with("system.") && !raw.contains_key("capped") && !raw.contains_key("timeseries") && !raw.contains_key("clusteredIndex") && !raw.contains_key("viewOn")
}

/// The component of a value: its ascending index encoding with no collation (prefix-free and order-preserving; the
/// same value always gets the same bytes, whatever its numeric type).
pub fn comp(v: &Bson) -> Vec<u8> {
    crate::collation::sync_scope(None, || crate::keyenc::encode(v))
}

/// The value of a field of `d` (missing: null; several through an array: the array of them).
pub fn value(field: &str, d: &Document) -> Bson {
    let vals = crate::query::path_values(d, field);
    match vals.len() {
        0 => Bson::Null,
        1 => vals[0].clone(),
        _ => Bson::Array(vals.into_iter().cloned().collect()),
    }
}

/// The customer component of a document.
pub fn cust_comp(k: &Ckey, d: &Document) -> Vec<u8> {
    comp(&value(&k.field, d))
}

/// The document suffix of `d` (what follows the collection's document prefix): with a customer key, the mark, the
/// customer component, the time component if the key has one, then the `_id` encoding; without, the `_id` encoding.
pub fn dsuf(key: Option<&Ckey>, d: &Document) -> Vec<u8> {
    let id = crate::layout::encode_sortable(d.get("_id").unwrap_or(&Bson::Null));
    match key {
        Some(k) => {
            let mut s = vec![MARK];
            s.extend(cust_comp(k, d));
            if let Some(o) = &k.order {
                s.extend(comp(&value(o, d)));
            }
            s.extend(id);
            s
        }
        None => id,
    }
}

/// The parts of a customer-keyed document suffix: (customer component, `_id` encoding). `None` for a plain suffix.
/// `timed`: the collection's key has a time field (one more component before the `_id`).
pub fn split_dsuf(suf: &[u8], timed: bool) -> Option<(&[u8], &[u8])> {
    if suf.first() != Some(&MARK) {
        return None;
    }
    let t = &suf[1..];
    let n = crate::keyenc::skip(t)?;
    let mut rest = &t[n..];
    if timed {
        let m = crate::keyenc::skip(rest)?;
        rest = &rest[m..];
    }
    Some((&t[..n], rest))
}

/// The document key of `d` in `coll`.
pub fn doc_key(coll: &str, key: Option<&Ckey>, d: &Document) -> Vec<u8> {
    let mut k = crate::layout::doc_prefix(coll);
    k.extend(dsuf(key, d));
    k
}

pub fn loc_prefix(coll: &str) -> Vec<u8> {
    let mut k = b"loc/".to_vec();
    k.extend(crate::layout::escape_collection(coll));
    k.push(b'/');
    k
}

/// The prefix of the locators of one `_id` (every customer holding it).
pub fn loc_id_prefix(coll: &str, id_enc: &[u8]) -> Vec<u8> {
    let mut k = loc_prefix(coll);
    k.extend((id_enc.len() as u32).to_be_bytes());
    k.extend(id_enc);
    k
}

/// The locator key of a document: its `_id` under its customer (value: the document suffix).
pub fn loc_key_of(coll: &str, key: &Ckey, d: &Document) -> Vec<u8> {
    let id = crate::layout::encode_sortable(d.get("_id").unwrap_or(&Bson::Null));
    let mut k = loc_id_prefix(coll, &id);
    k.extend(cust_comp(key, d));
    k
}

/// The locator key of the document with this suffix (`None` for a plain suffix).
pub fn loc_key_of_suffix(coll: &str, timed: bool, suf: &[u8]) -> Option<Vec<u8>> {
    let (c, id) = split_dsuf(suf, timed)?;
    let mut k = loc_id_prefix(coll, id);
    k.extend(c);
    Some(k)
}

/// The customer fields of a change event's `documentKey`, as mongod writes them for a sharded collection whose shard
/// key is the customer key (MEASURED against mongos 7.0.34): the customer field, then the time field if the key has
/// one, each under its full (dotted) path, a field the document lacks left out; `_id` follows them.
pub fn doc_key_fields(k: &Ckey, d: &Document) -> Document {
    let mut out = Document::new();
    for f in std::iter::once(&k.field).chain(k.order.as_ref()) {
        if !crate::query::path_values(d, f).is_empty() {
            out.insert(f.clone(), value(f, d));
        }
    }
    out
}

/// The locator key of `_id` (encoded) under the customer value `cust` (as [`value`] gives it).
pub fn loc_key_of_value(coll: &str, id_enc: &[u8], cust: &Bson) -> Vec<u8> {
    let mut k = loc_id_prefix(coll, id_enc);
    k.extend(comp(cust));
    k
}

/// The collation `_id` index of a customer-keyed collection with a default collation is per customer, like the
/// locator: `idc/{coll}/{n:u32}{collation key of _id}{customer}` -> document suffix. (A plain collection keeps
/// `idc/{coll}/{collation key of _id}`, `collopts.rs`.) The prefix of every customer holding a collation-equal `_id`:
pub fn idc_id_prefix(coll: &str, c: &crate::collation::Collation, id: &Bson) -> Vec<u8> {
    let ck = crate::layout::encode_sortable_with(id, Some(c));
    let mut k = crate::collopts::idc_prefix(coll);
    k.extend((ck.len() as u32).to_be_bytes());
    k.extend(ck);
    k
}

/// The collation `_id` index key of `d` in a collection with a default collation: per customer with a customer key,
/// per collection without.
pub fn idc_key_of(coll: &str, c: &crate::collation::Collation, key: Option<&Ckey>, d: &Document) -> Vec<u8> {
    let id = d.get("_id").unwrap_or(&Bson::Null);
    match key {
        Some(k) => {
            let mut p = idc_id_prefix(coll, c, id);
            p.extend(cust_comp(k, d));
            p
        }
        None => crate::collopts::idc_key(coll, c, id),
    }
}

/// The customer key of a collection from its options.
pub fn of_opts(o: Option<&std::sync::Arc<crate::collopts::CollOpts>>) -> Option<Ckey> {
    o.and_then(|o| o.customer.clone())
}

/// The customer key of a collection, for reads.
pub async fn of(g: &mut impl crate::coll::Rd, coll: &str) -> anyhow::Result<Option<Ckey>> {
    Ok(of_opts(g.coll_opts(coll).await?.as_ref()))
}

/// The customer key of a collection about to be written (or about to get an index). A collection named by
/// `--customer-key` that has no customer key and no documents takes it now (stored with its options before the first
/// document or index is written).
pub async fn for_write(g: &mut crate::dbs::Entry, ns: &str, coll: &str) -> anyhow::Result<Option<Ckey>> {
    let r = for_write_inner(g, ns, coll).await?;
    // agent checkpoints (`agentck.rs`): a registered collection is rewritten only when its customer key is `thread_id`
    crate::agentck::note_key(ns.split('.').next().unwrap_or(""), coll, r.as_ref().map(|k| k.field.as_str()));
    Ok(r)
}

async fn for_write_inner(g: &mut crate::dbs::Entry, ns: &str, coll: &str) -> anyhow::Result<Option<Ckey>> {
    let opts = g.coll_opts(coll).await?;
    if let Some(f) = of_opts(opts.as_ref()) {
        return Ok(Some(f));
    }
    let db = ns.split('.').next().unwrap_or("");
    let Some(key) = configured(db, coll) else { return Ok(None) };
    if g.ckey_decided(coll) {
        return Ok(None);
    }
    let raw = opts.as_ref().map(|o| o.raw.clone()).unwrap_or_default();
    let prefix = crate::layout::doc_prefix(coll);
    let empty = g.scan_range_live(&prefix, crate::keyenc::prefix_end(&prefix).as_deref(), Some(1)).await?.is_empty();
    g.note_ckey_decided(coll);
    if !empty || !allowed(coll, &raw) {
        return Ok(None);
    }
    let mut raw = raw;
    store(&mut raw, &key);
    g.apply(vec![tenant_bench::lifecycle::Op::Put(crate::collopts::meta_key(coll), raw.to_vec()?)]).await?;
    g.set_coll_opts(coll, Some(crate::collopts::CollOpts::parse(raw)));
    Ok(Some(key))
}

/// The stored options as a client sees them (without the customer key).
pub fn strip(raw: &mut Document) {
    raw.remove(RAW_KEY);
    raw.remove(RAW_ORDER);
}

/// Is an index with this key pattern and collation co-located with the documents of a collection whose customer key is
/// `key` (first field the customer field, ascending, regular, no collation)?
pub fn colocates(key: &Ckey, pattern: &Document, regular: bool, collation: bool) -> bool {
    if !regular || collation {
        return false;
    }
    match pattern.iter().next() {
        Some((f, dir)) => {
            *f == key.field
                && match dir {
                    Bson::Int32(1) | Bson::Int64(1) => true,
                    Bson::Double(d) => *d == 1.0,
                    _ => false,
                }
        }
        None => false,
    }
}

// ---- layout v2: logical <-> physical keys ---------------------------------------------------------------------------

/// Physical prefix of the customer space.
pub const U: &[u8] = b"u/";
/// The end of the customer space (`prefix_end(U)`).
pub const U_END: &[u8] = b"u0";

fn split_slash(b: &[u8]) -> Option<(&[u8], &[u8])> {
    let p = b.iter().position(|&c| c == b'/')?;
    Some((&b[..p], &b[p + 1..]))
}

/// The physical key of a logical key (`None`: stored as is). Only customer-keyed documents and co-located index
/// entries move (module docs).
pub fn to_phys(k: &[u8]) -> Option<Vec<u8>> {
    if let Some(rest) = k.strip_prefix(b"doc/") {
        let (coll, t) = split_slash(rest)?;
        let t = t.strip_prefix(&[MARK])?;
        let n = crate::keyenc::skip(t)?;
        let mut p = Vec::with_capacity(k.len() + 4);
        p.extend(U);
        p.extend(&t[..n]);
        p.extend(coll);
        p.extend(b"/d/");
        p.extend(&t[n..]);
        return Some(p);
    }
    if let Some(rest) = k.strip_prefix(b"idx/") {
        let (coll, t) = split_slash(rest)?;
        let t = t.strip_prefix(&[MARK])?;
        let (name, vals) = split_slash(t)?;
        let n = crate::keyenc::skip(vals)?;
        let mut p = Vec::with_capacity(k.len() + n + 4);
        p.extend(U);
        p.extend(&vals[..n]);
        p.extend(coll);
        p.extend(b"/x/");
        p.extend(name);
        p.push(b'/');
        p.extend(vals);
        return Some(p);
    }
    None
}

/// A physical key read from the customer space, parsed: (customer component, the rest after it).
fn split_u(p: &[u8]) -> Option<(&[u8], &[u8])> {
    let t = p.strip_prefix(U)?;
    let n = crate::keyenc::skip(t)?;
    Some((&t[..n], &t[n..]))
}

/// The logical key of a physical key (`None`: the key is the same in both, it is not in the customer space).
pub fn to_logical(p: &[u8]) -> Option<Vec<u8>> {
    let (c, r) = split_u(p)?;
    let (coll, r2) = split_slash(r)?;
    if let Some(rest) = r2.strip_prefix(b"d/") {
        let mut k = Vec::with_capacity(p.len() + 4);
        k.extend(b"doc/");
        k.extend(coll);
        k.push(b'/');
        k.push(MARK);
        k.extend(c);
        k.extend(rest);
        return Some(k);
    }
    if let Some(r3) = r2.strip_prefix(b"x/") {
        let (name, tail) = split_slash(r3)?;
        let mut k = Vec::with_capacity(p.len() + 4);
        k.extend(b"idx/");
        k.extend(coll);
        k.push(b'/');
        k.push(MARK);
        k.extend(name);
        k.push(b'/');
        k.extend(tail);
        return Some(k);
    }
    None
}

/// One mapped area of the logical keyspace: the documents of a customer-keyed collection, or the entries of one
/// co-located index. Inside it, logical order is physical order restricted to the area.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Seg {
    /// Logical prefix of the area (`doc/{coll}/0x00` or `idx/{coll}/0x00{index}/`).
    pub lp: Vec<u8>,
    /// What follows the customer component in the physical key (`{coll}/d/` or `{coll}/x/{index}/`).
    pub infix: Vec<u8>,
    /// Documents: the logical tail after the customer component is the physical tail. Index entries: the physical tail is
    /// the whole logical tail (the values start with the customer component).
    pub doc: bool,
}

impl Seg {
    pub fn docs(coll_esc: &[u8]) -> Seg {
        let mut lp = b"doc/".to_vec();
        lp.extend(coll_esc);
        lp.push(b'/');
        lp.push(MARK);
        let mut infix = coll_esc.to_vec();
        infix.extend(b"/d/");
        Seg { lp, infix, doc: true }
    }

    pub fn index(coll_esc: &[u8], name_esc: &[u8]) -> Seg {
        let mut lp = b"idx/".to_vec();
        lp.extend(coll_esc);
        lp.push(b'/');
        lp.push(MARK);
        lp.extend(name_esc);
        lp.push(b'/');
        let mut infix = coll_esc.to_vec();
        infix.extend(b"/x/");
        infix.extend(name_esc);
        infix.push(b'/');
        Seg { lp, infix, doc: false }
    }

    /// The end of the logical area (exclusive).
    pub fn lp_end(&self) -> Vec<u8> {
        crate::keyenc::prefix_end(&self.lp).expect("a segment prefix has an end")
    }

    /// The physical bound for a logical bound inside the area (`lp <= x <= lp_end`).
    pub fn phys_bound(&self, x: &[u8]) -> Vec<u8> {
        if x <= self.lp.as_slice() {
            return U.to_vec();
        }
        if x >= self.lp_end().as_slice() {
            return U_END.to_vec();
        }
        let tail = &x[self.lp.len()..];
        let mut p = U.to_vec();
        match crate::keyenc::skip(tail) {
            Some(n) => {
                p.extend(&tail[..n]);
                p.extend(&self.infix);
                p.extend(if self.doc { &tail[n..] } else { tail });
            }
            // between customers: every key of a customer whose component sorts at or after `tail`
            None => p.extend(tail),
        }
        p
    }

    /// The physical upper bound (exclusive) of logical keys below `x`: [`Seg::phys_bound`], except when `x`'s tail is
    /// exactly one customer component c. Then every key of the area below `x` belongs to a customer sorting before c,
    /// and no key of the area lies in [`u/c`, `phys_bound(x)`): that interval holds only c's other areas. The bound is
    /// `u/c`, so a read of one customer does not touch the next customer's keys (compaction 2 step 6: a reverse index
    /// read of one customer read the next customer's documents first, which cost the counter a block and a GET).
    pub fn phys_upper(&self, x: &[u8]) -> Vec<u8> {
        if x > self.lp.as_slice() && x < self.lp_end().as_slice() {
            let tail = &x[self.lp.len()..];
            if crate::keyenc::skip(tail) == Some(tail.len()) {
                let mut p = U.to_vec();
                p.extend(tail);
                return p;
            }
        }
        self.phys_bound(x)
    }

    /// The physical upper bound (exclusive) of a read of [lo, hi) inside this area: [`Seg::phys_upper`], tightened to the
    /// end of one customer's part of the area when [lo, hi) holds keys of that customer only (lo starts with its
    /// component c, hi <= lp + prefix_end(c)). Every area key of the read is then `u/c{infix}...`, below
    /// `prefix_end(u/c{infix})`. A one-customer read then stays inside its area, which the counter of step 6 slices by.
    pub fn phys_upper_within(&self, lo: &[u8], hi: &[u8]) -> Vec<u8> {
        let p = self.phys_upper(hi);
        if lo <= self.lp.as_slice() || lo >= self.lp_end().as_slice() {
            return p;
        }
        let tail = &lo[self.lp.len()..];
        let Some(n) = crate::keyenc::skip(tail) else { return p };
        let c = &tail[..n];
        let Some(ce) = crate::keyenc::prefix_end(c) else { return p };
        let mut cust_end = self.lp.clone();
        cust_end.extend(ce);
        if hi > cust_end.as_slice() {
            return p;
        }
        let mut a = U.to_vec();
        a.extend(c);
        a.extend(&self.infix);
        match crate::keyenc::prefix_end(&a) {
            Some(e) if e < p => e,
            _ => p,
        }
    }

    /// Where a physical key of the customer space stands against this area: `Ok(logical key)` inside it,
    /// `Err(Before)` before the area within its customer (`seek`: the area's start for that customer), `Err(After)`
    /// after it within its customer (`area_end`: the end of the area for that customer). `customer` is `u/{customer}`.
    pub fn place(&self, p: &[u8]) -> Result<Vec<u8>, Place> {
        let Some((c, r)) = split_u(p) else { return Err(Place::Skip) };
        if r.starts_with(&self.infix) {
            let tail = &r[self.infix.len()..];
            let mut k = self.lp.clone();
            if self.doc {
                k.extend(c);
            }
            k.extend(tail);
            return Ok(k);
        }
        let mut cu = U.to_vec();
        cu.extend(c);
        if r < self.infix.as_slice() {
            let mut s = cu.clone();
            s.extend(&self.infix);
            Err(Place::Before { seek: s, customer: cu })
        } else {
            let mut s = cu.clone();
            s.extend(&self.infix);
            Err(Place::After { customer: cu, area_end: crate::keyenc::prefix_end(&s).unwrap_or_else(|| U_END.to_vec()) })
        }
    }
}

/// See [`Seg::place`].
pub enum Place {
    Before { seek: Vec<u8>, customer: Vec<u8> },
    After { customer: Vec<u8>, area_end: Vec<u8> },
    /// Not a customer-space key (never in a mapped scan).
    Skip,
}

/// The area a logical range read lies in by its bounds alone, when they say so: `Some(Some(seg))` inside one mapped
/// area, `Some(None)` inside one area that is never mapped (a plain index, a plain collection's documents, or any key
/// outside `doc/` and `idx/`), `None` when the bounds alone do not tell (the caller asks the catalog).
pub fn area_of(lo: &[u8], hi: Option<&[u8]>) -> Option<Option<Seg>> {
    let Some(hi) = hi else {
        // unbounded above: only a range that starts past every mapped area is plain
        return if lo >= b"idx0".as_slice() { Some(None) } else { None };
    };
    // wholly outside doc/ and idx/
    let doc_hit = lo < b"doc0".as_slice() && hi > b"doc/".as_slice();
    let idx_hit = lo < b"idx0".as_slice() && hi > b"idx/".as_slice();
    if !doc_hit && !idx_hit {
        return Some(None);
    }
    if let Some(rest) = lo.strip_prefix(b"doc/") {
        let (coll, t) = split_slash(rest)?;
        let mut cp = b"doc/".to_vec();
        cp.extend(coll);
        cp.push(b'/');
        if t.first() == Some(&MARK) {
            let s = Seg::docs(coll);
            if hi <= s.lp_end().as_slice() {
                return Some(Some(s));
            }
        } else if !t.is_empty() {
            // starts past the mapped area of this collection (if any): plain while it stays in the collection
            if hi <= crate::keyenc::prefix_end(&cp)?.as_slice() {
                return Some(None);
            }
        }
        return None;
    }
    if let Some(rest) = lo.strip_prefix(b"idx/") {
        let (coll, t) = split_slash(rest)?;
        if t.first() == Some(&MARK) {
            let (name, _) = split_slash(&t[1..])?;
            let s = Seg::index(coll, name);
            if hi <= s.lp_end().as_slice() {
                return Some(Some(s));
            }
            return None;
        }
        if let Some((name, _)) = split_slash(t) {
            if !name.is_empty() {
                let mut ip = b"idx/".to_vec();
                ip.extend(coll);
                ip.push(b'/');
                ip.extend(name);
                ip.push(b'/');
                if hi <= crate::keyenc::prefix_end(&ip)?.as_slice() {
                    return Some(None);
                }
            }
        }
        return None;
    }
    None
}

/// The collection (escaped) a logical range inside one collection's documents or indexes is about, if any:
/// (`doc` or `idx` area, escaped collection name).
pub fn coll_of_range(lo: &[u8], hi: Option<&[u8]>) -> Option<(bool, Vec<u8>)> {
    let hi = hi?;
    for (pre, doc) in [(b"doc/".as_slice(), true), (b"idx/".as_slice(), false)] {
        if let Some(rest) = lo.strip_prefix(pre) {
            let (coll, _) = split_slash(rest)?;
            let mut cp = pre.to_vec();
            cp.extend(coll);
            cp.push(b'/');
            if hi <= crate::keyenc::prefix_end(&cp)?.as_slice() {
                return Some((doc, coll.to_vec()));
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    fn k(field: &str) -> Ckey {
        Ckey { field: field.into(), order: None }
    }

    #[test]
    fn one_customer_shares_a_prefix_whatever_the_number_type() {
        let a = dsuf(Some(&k("u")), &doc! { "_id": 1, "u": 7 });
        let b = dsuf(Some(&k("u")), &doc! { "_id": 2, "u": 7.0 });
        let c = dsuf(Some(&k("u")), &doc! { "_id": 3, "u": 7_i64 });
        let mut p = vec![MARK];
        p.extend(comp(&Bson::Int32(7)));
        assert!(a.starts_with(&p) && b.starts_with(&p) && c.starts_with(&p));
        // a customer's component is never a prefix of another's, and order follows the values
        let s1 = comp(&Bson::String("ab".into()));
        let s2 = comp(&Bson::String("abc".into()));
        assert!(!s2.starts_with(&s1));
        assert!(s1 < s2);
        assert_eq!(dsuf(None, &doc! { "_id": 5 }), crate::layout::encode_sortable(&Bson::Int32(5)));
    }

    #[test]
    fn missing_is_null_and_the_time_field_sits_before_the_id() {
        assert_eq!(dsuf(Some(&k("u")), &doc! { "_id": 1 }), dsuf(Some(&k("u")), &doc! { "_id": 1, "u": Bson::Null }));
        let t = Ckey { field: "u".into(), order: Some("ts".into()) };
        let d1 = dsuf(Some(&t), &doc! { "_id": 9, "u": "a", "ts": 1 });
        let d2 = dsuf(Some(&t), &doc! { "_id": 1, "u": "a", "ts": 2 });
        assert!(d1 < d2, "a customer's documents in time order");
        let (c, id) = split_dsuf(&d1, true).unwrap();
        assert_eq!(c, comp(&Bson::String("a".into())).as_slice());
        assert_eq!(id, crate::layout::encode_sortable(&Bson::Int32(9)).as_slice());
    }

    /// Compaction 2 step 6: the run format's customer rule for the server's keys (`runfmt::CustomerKind::V2K`) finds
    /// exactly the customer component of every physical key, documents and co-located index entries, for every type a
    /// customer value can have; step 1's rule (V2, a length byte) does not.
    #[test]
    fn run_format_finds_the_customer_component_of_server_keys() {
        use tenant_bench::runfmt::CustomerKind;
        let values = [Bson::Int32(7), Bson::Int64(1 << 40), Bson::Double(2.5), Bson::String("alice".into()), Bson::String("a\u{0}b".into()), Bson::ObjectId(bson::oid::ObjectId::new())];
        let mut v2_wrong = 0;
        for v in &values {
            let d = doc! { "_id": 3, "u": v.clone() };
            let docp = to_phys(&doc_key("messages", Some(&k("u")), &d)).unwrap();
            let mut want = U.to_vec();
            want.extend(comp(v));
            let r = CustomerKind::V2K.customer_of(&docp).unwrap();
            assert_eq!(&docp[..r.end], &want[..], "{v:?}");
            assert_eq!(r.start, 2);
            let mut ix = b"idx/messages/".to_vec();
            ix.push(MARK);
            ix.extend(b"u_1_ts_-1/");
            ix.extend(comp(v));
            ix.extend(comp(&Bson::Int32(99)));
            ix.extend(crate::layout::encode_sortable(&Bson::Int32(3)));
            let ixp = to_phys(&ix).unwrap();
            let r = CustomerKind::V2K.customer_of(&ixp).unwrap();
            assert_eq!(&ixp[..r.end], &want[..], "{v:?} index entry");
            if CustomerKind::V2.customer_of(&docp).map(|r| docp[..r.end].to_vec()) != Some(want.clone()) {
                v2_wrong += 1;
            }
        }
        assert_eq!(v2_wrong, values.len(), "step 1's rule misreads every server component");
        assert_eq!(CustomerKind::V2K.customer_of(b"loc/messages/x"), None);
        assert_eq!(CustomerKind::V2K.customer_of(b"u/"), None);
    }

    /// Compaction 2 step 6 fix round (review FIX 1): a collection or index name with a '/' is escaped in the server's
    /// keys, so the counter's area rule (`fx::counter::area_prefix`, the first '/' after the customer component ends the
    /// collection name) finds the area of every server key, documents and co-located index entries alike.
    #[test]
    fn counter_finds_the_area_of_a_collection_named_with_a_slash() {
        use tenant_bench::fx::counter::area_prefix;
        use tenant_bench::runfmt::CustomerKind;
        let d = doc! { "_id": 3, "u": 7 };
        let docp = to_phys(&doc_key("zz/q", Some(&k("u")), &d)).unwrap();
        let mut want = U.to_vec();
        want.extend(comp(&Bson::Int32(7)));
        want.extend(b"zz%2Fq/d/");
        assert_eq!(area_prefix(CustomerKind::V2K, &docp), Some(want));
        let mut ix = b"idx/zz%2Fq/".to_vec();
        ix.push(MARK);
        ix.extend(b"a%2Fb_1/");
        ix.extend(comp(&Bson::Int32(7)));
        ix.extend(crate::layout::encode_sortable(&Bson::Int32(3)));
        let ixp = to_phys(&ix).unwrap();
        let mut want = U.to_vec();
        want.extend(comp(&Bson::Int32(7)));
        want.extend(b"zz%2Fq/x/a%2Fb_1/");
        assert_eq!(area_prefix(CustomerKind::V2K, &ixp), Some(want));
        assert_eq!(crate::layout::escape_collection("zz/q"), b"zz%2Fq".to_vec());
    }

    #[test]
    fn config_matches_db_and_collection() {
        let _ = configure("chat.messages=userId, orders=customerId:ts");
        assert_eq!(configured("chat", "messages"), Some(k("userId")));
        assert_eq!(configured("other", "messages"), None);
        assert_eq!(configured("any", "orders"), Some(Ckey { field: "customerId".into(), order: Some("ts".into()) }));
    }

    #[test]
    fn keys_map_both_ways_and_keep_order() {
        let key = k("u");
        let mut logical = Vec::new();
        for u in ["a", "ab", "b", ""] {
            for id in [1, 2, 30] {
                logical.push(doc_key("m", Some(&key), &doc! { "_id": id, "u": u }));
                logical.push(doc_key("n", Some(&key), &doc! { "_id": id, "u": u }));
                let mut ik = Seg::index(b"m", b"u_1").lp;
                ik.extend(comp(&Bson::String(u.into())));
                ik.extend(dsuf(Some(&key), &doc! { "_id": id, "u": u }));
                ik.extend(3u16.to_be_bytes());
                logical.push(ik);
            }
        }
        for l in &logical {
            let p = to_phys(l).unwrap();
            assert!(p.starts_with(U));
            assert_eq!(to_logical(&p).as_ref(), Some(l));
        }
        // inside one area, physical order is logical order
        for seg in [Seg::docs(b"m"), Seg::index(b"m", b"u_1")] {
            let mut mine: Vec<&Vec<u8>> = logical.iter().filter(|l| l.starts_with(&seg.lp)).collect();
            mine.sort();
            let mut phys: Vec<Vec<u8>> = mine.iter().map(|l| to_phys(l).unwrap()).collect();
            let unsorted = phys.clone();
            phys.sort();
            assert_eq!(phys, unsorted);
            for l in &mine {
                assert!(matches!(seg.place(&to_phys(l).unwrap()), Ok(ref x) if x == *l));
            }
        }
        assert_eq!(to_phys(b"doc/m/\x04abc"), None);
        assert_eq!(to_phys(b"idx/m/k_1/abc"), None);
        assert_eq!(to_logical(b"meta/collections"), None);
    }

    #[test]
    fn bounds_between_customers_keep_order() {
        let key = k("u");
        let seg = Seg::docs(b"m");
        let l1 = doc_key("m", Some(&key), &doc! { "_id": 1, "u": "abc" });
        // a bound that is a partial customer component
        let mut b = seg.lp.clone();
        b.extend(&comp(&Bson::String("ab".into()))[..2]);
        let pb = seg.phys_bound(&b);
        assert_eq!(l1 >= b, to_phys(&l1).unwrap() >= pb);
        assert_eq!(seg.phys_bound(&seg.lp), U.to_vec());
        assert_eq!(seg.phys_bound(&seg.lp_end()), U_END.to_vec());
    }

    #[test]
    fn areas_from_bounds() {
        let s = Seg::docs(b"m");
        assert_eq!(area_of(&s.lp, Some(&s.lp_end())), Some(Some(s.clone())));
        let i = Seg::index(b"m", b"u_1");
        let mut lo = i.lp.clone();
        lo.push(4);
        assert_eq!(area_of(&lo, Some(&i.lp_end())), Some(Some(i)));
        assert_eq!(area_of(b"idx/m/k_1/\x04", Some(b"idx/m/k_10")), Some(None));
        assert_eq!(area_of(b"nat/m/", Some(b"nat/m0")), Some(None));
        assert_eq!(area_of(b"doc/m/", Some(b"doc/m0")), None);
        assert_eq!(area_of(b"", None), None);
    }
}


#[cfg(test)]
mod default_key_tests {
    use super::with_agent_defaults;

    #[test]
    fn saver_collections_are_keyed_by_thread_id_by_default() {
        assert_eq!(with_agent_defaults("", "checkpoints,checkpoint_writes"), "checkpoints=thread_id,checkpoint_writes=thread_id");
    }

    #[test]
    fn the_command_line_wins() {
        assert_eq!(with_agent_defaults("checkpoints=user_id", "checkpoints,checkpoint_writes"), "checkpoints=user_id,checkpoint_writes=thread_id");
        assert_eq!(with_agent_defaults("app.checkpoints=user_id:ts", "checkpoints,checkpoint_writes"), "app.checkpoints=user_id:ts,checkpoint_writes=thread_id");
        assert_eq!(with_agent_defaults("messaggi=cliente:ts", "checkpoints,checkpoint_writes"), "messaggi=cliente:ts,checkpoints=thread_id,checkpoint_writes=thread_id");
    }

    #[test]
    fn no_agent_collections_no_default() {
        assert_eq!(with_agent_defaults("a=b", ""), "a=b");
        assert_eq!(with_agent_defaults("", "app.checkpoints"), "app.checkpoints=thread_id");
    }
}
