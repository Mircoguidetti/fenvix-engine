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
