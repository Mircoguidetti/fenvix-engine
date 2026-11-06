//! Customer-first keys (per-customer compaction, step 1).
//!
//! A collection may declare its customer key, a field like a shard key (`create` option `customerKey: "<field>"`, or
//! the server option `--customer-key` for the collections it names, applied when such a collection is still empty).
//! Its documents are then stored under
//!
//! `doc/{collection}/{customer component}{_id encoding}`
//!
//! instead of `doc/{collection}/{_id encoding}`, so one customer's documents sit together in every SST and a read of a
//! customer's recent documents touches one block per sorted run instead of one block per document.
//!
//! The bytes after the collection's document prefix (the "document suffix", `dsuf`) are what every other structure
//! already stores as the document's id: index entries end with it, the natural-order entries and the collation `_id`
//! index map to it. So index scans, natural order and transactions keep working unchanged; they carry the customer
//! component along. An index whose first field is the customer key has its entries grouped by customer as well.
//!
//! Only a lookup by `_id` alone needs to know the customer: `loc/{collection}/{_id encoding}` -> `dsuf` (the locator)
//! is written in the same batch as the document, and is also the collection's unique `_id` index (two customers cannot
//! hold the same `_id`).
//!
//! The customer component is a pure function of the document (the customer field's value, missing = null, arrays
//! whole), so the layout changes no answer: queries never narrow by it, they read through the same indexes as before.
//! A customer key is never added to a collection that already has documents, and never to a capped, time-series,
//! clustered or system collection: with the option off, nothing changes for existing data.

use bson::{Bson, Document};

/// Documents written customer first, and `_id` lookups through the locator, since the start (serverStatus
/// `fenvix.customerKeys`).
pub static DOCS_WRITTEN: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static LOCATOR_LOOKUPS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Where the customer key lives in the collection's stored options (`meta/copt/`), hidden from `listCollections`.
pub const RAW_KEY: &str = "fenvixCustomerKey";

/// `--customer-key`: (database or `None` for any, collection, field).
static CONFIG: std::sync::OnceLock<Vec<(Option<String>, String, String)>> = std::sync::OnceLock::new();

/// Parse `--customer-key`: comma separated `[db.]collection=field`.
pub fn configure(spec: &str) -> anyhow::Result<()> {
    let mut v = Vec::new();
    for part in spec.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let (ns, field) = part.split_once('=').ok_or_else(|| anyhow::anyhow!("--customer-key: '{part}' is not [db.]collection=field"))?;
        anyhow::ensure!(!field.is_empty() && !field.starts_with('$'), "--customer-key: bad field in '{part}'");
        let (db, coll) = match ns.split_once('.') {
            Some((d, c)) => (Some(d.to_string()), c.to_string()),
            None => (None, ns.to_string()),
        };
        v.push((db, coll, field.to_string()));
    }
    let _ = CONFIG.set(v);
    Ok(())
}

/// The customer key `--customer-key` asks for `db.coll`, if any.
pub fn configured(db: &str, coll: &str) -> Option<String> {
    CONFIG.get()?.iter().find(|(d, c, _)| c == coll && d.as_deref().map_or(true, |d| d == db)).map(|(_, _, f)| f.clone())
}

/// May this collection take a customer key at all?
pub fn allowed(coll: &str, raw: &Document) -> bool {
    !coll.starts_with("system.") && !raw.contains_key("capped") && !raw.contains_key("timeseries") && !raw.contains_key("clusteredIndex") && !raw.contains_key("viewOn")
}

/// The longest customer component kept as is; longer ones are cut and followed by a hash (still a function of the value).
const MAX_COMP: usize = 64;

/// The customer component of a value: one length byte, then an order-preserving encoding of the value (one customer's
/// keys share an exact byte prefix, and the same customer always gets the same bytes, whatever its numeric type).
pub fn comp(v: &Bson) -> Vec<u8> {
    let mut e = crate::layout::encode_sortable(v);
    if e.len() > MAX_COMP {
        use sha2::Digest;
        let h = sha2::Sha256::digest(&e);
        e.truncate(MAX_COMP - 8);
        e.extend_from_slice(&h[..8]);
    }
    let mut out = Vec::with_capacity(e.len() + 1);
    out.push(e.len() as u8);
    out.extend(e);
    out
}

/// The value of the customer field of `d` (missing: null; several through an array: the array of them).
pub fn value(field: &str, d: &Document) -> Bson {
    let vals = crate::query::path_values(d, field);
    match vals.len() {
        0 => Bson::Null,
        1 => vals[0].clone(),
        _ => Bson::Array(vals.into_iter().cloned().collect()),
    }
}

/// The document suffix of `d` (what follows the collection's document prefix): with a customer key, the customer
/// component then the `_id` encoding; without, the `_id` encoding.
pub fn dsuf(field: Option<&str>, d: &Document) -> Vec<u8> {
    let id = crate::layout::encode_sortable(d.get("_id").unwrap_or(&Bson::Null));
    match field {
        Some(f) => {
            let mut k = comp(&value(f, d));
            k.extend(id);
            k
        }
        None => id,
    }
}

/// The document key of `d` in `coll`.
pub fn doc_key(coll: &str, field: Option<&str>, d: &Document) -> Vec<u8> {
    let mut k = crate::layout::doc_prefix(coll);
    k.extend(dsuf(field, d));
    k
}

pub fn loc_prefix(coll: &str) -> Vec<u8> {
    let mut k = b"loc/".to_vec();
    k.extend(crate::layout::escape_collection(coll));
    k.push(b'/');
    k
}

/// The locator key of an `_id` (value: the document suffix).
pub fn loc_key(coll: &str, id: &Bson) -> Vec<u8> {
    let mut k = loc_prefix(coll);
    k.extend(crate::layout::encode_sortable(id));
    k
}

/// The customer key of a collection from its options.
pub fn of_opts(o: Option<&std::sync::Arc<crate::collopts::CollOpts>>) -> Option<String> {
    o.and_then(|o| o.customer.clone())
}

/// The customer key of a collection, for reads.
pub async fn of(g: &mut impl crate::coll::Rd, coll: &str) -> anyhow::Result<Option<String>> {
    Ok(of_opts(g.coll_opts(coll).await?.as_ref()))
}

/// The customer key of a collection about to be written. A collection named by `--customer-key` that has no options
/// yet and no documents takes it now (stored with its options before the first document is written).
pub async fn for_write(g: &mut crate::dbs::Entry, ns: &str, coll: &str) -> anyhow::Result<Option<String>> {
    let opts = g.coll_opts(coll).await?;
    if let Some(f) = of_opts(opts.as_ref()) {
        return Ok(Some(f));
    }
    let db = ns.split('.').next().unwrap_or("");
    let Some(field) = configured(db, coll) else { return Ok(None) };
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
    raw.insert(RAW_KEY, field.clone());
    g.apply(vec![tenant_bench::lifecycle::Op::Put(crate::collopts::meta_key(coll), raw.to_vec()?)]).await?;
    g.set_coll_opts(coll, Some(crate::collopts::CollOpts::parse(raw)));
    Ok(Some(field))
}

/// The stored options as a client sees them (without the customer key).
pub fn strip(raw: &mut Document) {
    raw.remove(RAW_KEY);
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    #[test]
    fn the_fence_layout_prototype_writes_the_same_document_keys() {
        // tenant_bench::fence (the fence-layout measurement) builds customer-first keys itself; they must be these bytes
        for u in [0i64, 7, 19_999, -5, 1 << 40] {
            let oid = bson::oid::ObjectId::from_bytes([0x69, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
            let v = if u.abs() < i32::MAX as i64 { Bson::Int32(u as i32) } else { Bson::Int64(u) };
            let ours = doc_key("messages", Some("userId"), &doc! { "_id": oid, "userId": v });
            assert_eq!(ours, tenant_bench::fence::doc_key("messages", u, &oid.bytes()), "customer {u}");
        }
    }

    #[test]
    fn one_customer_shares_a_prefix_whatever_the_number_type() {
        let a = dsuf(Some("u"), &doc! { "_id": 1, "u": 7 });
        let b = dsuf(Some("u"), &doc! { "_id": 2, "u": 7.0 });
        let c = dsuf(Some("u"), &doc! { "_id": 3, "u": 7_i64 });
        let p = comp(&Bson::Int32(7));
        assert!(a.starts_with(&p) && b.starts_with(&p) && c.starts_with(&p));
        // a customer's component is never a prefix of another's
        let s1 = comp(&Bson::String("ab".into()));
        let s2 = comp(&Bson::String("abc".into()));
        assert!(!s2.starts_with(&s1));
    }

    #[test]
    fn missing_is_null_and_long_values_are_bounded() {
        assert_eq!(dsuf(Some("u"), &doc! { "_id": 1 }), dsuf(Some("u"), &doc! { "_id": 1, "u": Bson::Null }));
        let long = "x".repeat(1000);
        let c = comp(&Bson::String(long.clone()));
        assert_eq!(c.len(), MAX_COMP + 1);
        assert_ne!(c, comp(&Bson::String(format!("{long}y"))));
        assert_eq!(dsuf(None, &doc! { "_id": 5 }), crate::layout::encode_sortable(&Bson::Int32(5)));
    }

    #[test]
    fn config_matches_db_and_collection() {
        let _ = configure("chat.messages=userId, orders=customerId");
        assert_eq!(configured("chat", "messages").as_deref(), Some("userId"));
        assert_eq!(configured("other", "messages"), None);
        assert_eq!(configured("any", "orders").as_deref(), Some("customerId"));
    }
}
