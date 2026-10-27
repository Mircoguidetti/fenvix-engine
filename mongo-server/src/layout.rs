//! Key layout inside a tenant (one slatedb database per MongoDB database):
//!
//! * `doc/{collection}/{id}`        one BSON document per key; `{id}` is [`encode_sortable`] of `_id`
//! * `meta/collections`             BSON `{ names: [..] }`
//! * `meta/indexes/{collection}`    BSON `{ indexes: [ {v, key, name, unique?, ..} ] }`
//!
//! Collection names are escaped (`%` and `/`) so one collection's prefix never covers another's.

use bson::Bson;

pub const META_COLLECTIONS: &[u8] = b"meta/collections";

pub fn escape_collection(name: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(name.len());
    for b in name.bytes() {
        match b {
            b'%' => out.extend_from_slice(b"%25"),
            b'/' => out.extend_from_slice(b"%2F"),
            _ => out.push(b),
        }
    }
    out
}

pub fn doc_prefix(collection: &str) -> Vec<u8> {
    let mut k = b"doc/".to_vec();
    k.extend(escape_collection(collection));
    k.push(b'/');
    k
}

pub fn doc_key(collection: &str, id: &Bson) -> Vec<u8> {
    let mut k = doc_prefix(collection);
    k.extend(encode_sortable(id));
    k
}

/// Prefix of all index entries of a collection.
pub fn idx_collection_prefix(collection: &str) -> Vec<u8> {
    let mut k = b"idx/".to_vec();
    k.extend(escape_collection(collection));
    k.push(b'/');
    k
}

pub fn index_key(collection: &str) -> Vec<u8> {
    let mut k = b"meta/indexes/".to_vec();
    k.extend(escape_collection(collection));
    k
}

fn sortable_i64(v: i64) -> [u8; 8] {
    ((v as u64) ^ (1 << 63)).to_be_bytes()
}

fn sortable_f64(v: f64) -> [u8; 8] {
    let v = if v == 0.0 { 0.0 } else { v };
    let bits = v.to_bits();
    let t = if bits >> 63 == 1 { !bits } else { bits | (1 << 63) };
    t.to_be_bytes()
}

/// Order-preserving, equality-preserving encoding of a BSON value: byte order follows MongoDB's
/// comparison order for the types that can be `_id`s, and `1`, `1_i64` and `1.0` encode identically.
pub fn encode_sortable(v: &Bson) -> Vec<u8> {
    let rank = crate::query::type_rank(v);
    let mut out = vec![rank];
    match v {
        Bson::Int32(i) => {
            out.extend(sortable_f64(*i as f64));
            out.extend(sortable_i64(*i as i64));
        }
        Bson::Int64(i) => {
            out.extend(sortable_f64(*i as f64));
            out.extend(sortable_i64(*i));
        }
        Bson::Double(f) => {
            out.extend(sortable_f64(*f));
            if f.is_finite() && f.fract() == 0.0 && f.abs() < 9.2e18 {
                out.extend(sortable_i64(*f as i64));
            } else {
                out.extend([0u8; 8]);
            }
        }
        Bson::Decimal128(_) => {
            let f = crate::query::as_f64(v);
            out.extend(sortable_f64(f));
            if f.is_finite() && f.fract() == 0.0 && f.abs() < 9.2e18 {
                out.extend(sortable_i64(f as i64));
            } else {
                out.extend([0u8; 8]);
            }
        }
        Bson::String(s) | Bson::Symbol(s) => out.extend(s.as_bytes()),
        Bson::ObjectId(o) => out.extend(o.bytes()),
        Bson::Boolean(b) => out.push(*b as u8),
        Bson::DateTime(d) => out.extend(sortable_i64(d.timestamp_millis())),
        Bson::Timestamp(t) => {
            out.extend(t.time.to_be_bytes());
            out.extend(t.increment.to_be_bytes());
        }
        Bson::Null | Bson::Undefined | Bson::MinKey | Bson::MaxKey => {}
        Bson::Document(d) => {
            for (k, val) in d {
                out.extend(k.as_bytes());
                out.push(0);
                let e = encode_sortable(val);
                out.extend((e.len() as u32).to_be_bytes());
                out.extend(e);
            }
        }
        Bson::Array(a) => {
            for val in a {
                let e = encode_sortable(val);
                out.extend((e.len() as u32).to_be_bytes());
                out.extend(e);
            }
        }
        Bson::Binary(b) => {
            out.push(u8::from(b.subtype));
            out.extend(&b.bytes);
        }
        other => {
            // Rare types: fall back to a stable textual form (unique, not order-preserving).
            out.extend(format!("{other:?}").into_bytes());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::oid::ObjectId;

    #[test]
    fn numbers_equal_across_types_and_sorted() {
        assert_eq!(encode_sortable(&Bson::Int32(7)), encode_sortable(&Bson::Double(7.0)));
        assert_eq!(encode_sortable(&Bson::Int64(7)), encode_sortable(&Bson::Int32(7)));
        let mut vals = vec![Bson::Double(-1.5), Bson::Int32(-2), Bson::Int32(0), Bson::Double(0.5), Bson::Int64(3)];
        let enc: Vec<_> = vals.iter().map(encode_sortable).collect();
        vals.sort_by(|a, b| crate::query::compare(a, b));
        let enc_sorted: Vec<_> = vals.iter().map(encode_sortable).collect();
        let mut by_bytes = enc.clone();
        by_bytes.sort();
        assert_eq!(by_bytes, enc_sorted);
    }

    #[test]
    fn large_int64_ids_stay_distinct() {
        let a = Bson::Int64((1 << 60) + 1);
        let b = Bson::Int64(1 << 60);
        assert_ne!(encode_sortable(&a), encode_sortable(&b));
    }

    #[test]
    fn types_group_in_bson_order() {
        let n = encode_sortable(&Bson::Int32(9999));
        let s = encode_sortable(&Bson::String("a".into()));
        let o = encode_sortable(&Bson::ObjectId(ObjectId::new()));
        assert!(n < s && s < o);
    }

    #[test]
    fn collection_prefixes_do_not_overlap() {
        let a = doc_prefix("a");
        let ab = doc_key("a/b", &Bson::Int32(1));
        assert!(!ab.starts_with(&a));
    }
}
