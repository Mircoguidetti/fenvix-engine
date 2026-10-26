//! Order-preserving, prefix-free encoding of BSON values for secondary-index keys.
//!
//! `encode(a) < encode(b)` (bytewise) exactly when MongoDB orders `a` before `b`, and no encoding is
//! a prefix of a different value's encoding, so components can be concatenated into compound keys and
//! a range over one component is a range over the concatenation. A descending component is the
//! bytewise complement of the ascending one (complement reverses the order and keeps prefix-freedom).
//!
//! Numbers of every BSON numeric type that are equal encode identically (`1`, `1_i64`, `1.0`).

use crate::query::type_rank;
use bson::Bson;

fn sortable_i64(v: i64) -> [u8; 8] {
    ((v as u64) ^ (1 << 63)).to_be_bytes()
}

fn sortable_f64(v: f64) -> [u8; 8] {
    let v = if v == 0.0 { 0.0 } else { v };
    let bits = v.to_bits();
    let t = if bits >> 63 == 1 { !bits } else { bits | (1 << 63) };
    t.to_be_bytes()
}

/// `0x00` becomes `0x00 0xFF`; the string ends with `0x00 0x01`.
fn escaped(bytes: &[u8], out: &mut Vec<u8>) {
    for &b in bytes {
        out.push(b);
        if b == 0 {
            out.push(0xFF);
        }
    }
    out.extend([0, 1]);
}

fn number(v: &Bson, out: &mut Vec<u8>) {
    let (f, exact): (f64, Option<i64>) = match v {
        Bson::Int32(i) => (*i as f64, Some(*i as i64)),
        Bson::Int64(i) => (*i as f64, Some(*i)),
        Bson::Double(f) => (*f, if f.is_finite() && f.fract() == 0.0 && f.abs() < 9.2e18 { Some(*f as i64) } else { None }),
        Bson::Decimal128(d) => {
            let f: f64 = d.to_string().parse().unwrap_or(f64::NAN);
            (f, None)
        }
        _ => (f64::NAN, None),
    };
    if f.is_nan() {
        out.push(0); // NaN sorts below every other number
        return;
    }
    out.push(1);
    out.extend(sortable_f64(f));
    match exact {
        Some(i) => out.extend(sortable_i64(i)),
        None => out.extend([0u8; 8]),
    }
}

fn body(v: &Bson, out: &mut Vec<u8>) {
    match v {
        Bson::MinKey | Bson::MaxKey => {}
        Bson::Null => out.push(0),
        Bson::Undefined => out.push(1),
        Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_) => number(v, out),
        Bson::String(s) | Bson::Symbol(s) => escaped(s.as_bytes(), out),
        Bson::Document(d) => {
            for (k, val) in d {
                out.push(type_rank(val));
                escaped(k.as_bytes(), out);
                body(val, out);
            }
            out.push(0);
        }
        Bson::Array(a) => {
            for val in a {
                encode_into(val, out);
            }
            out.push(0);
        }
        Bson::Binary(b) => {
            out.extend((b.bytes.len() as u32).to_be_bytes());
            out.push(u8::from(b.subtype));
            out.extend(&b.bytes);
        }
        Bson::ObjectId(o) => out.extend(o.bytes()),
        Bson::Boolean(b) => out.push(*b as u8),
        Bson::DateTime(d) => out.extend(sortable_i64(d.timestamp_millis())),
        Bson::Timestamp(t) => {
            out.extend(t.time.to_be_bytes());
            out.extend(t.increment.to_be_bytes());
        }
        Bson::RegularExpression(r) => {
            escaped(r.pattern.as_str().as_bytes(), out);
            escaped(r.options.as_str().as_bytes(), out);
        }
        Bson::JavaScriptCode(c) => escaped(c.as_bytes(), out),
        other => escaped(format!("{other:?}").as_bytes(), out),
    }
}

pub fn encode_into(v: &Bson, out: &mut Vec<u8>) {
    out.push(type_rank(v));
    body(v, out);
}

/// One ascending index-key component.
pub fn encode(v: &Bson) -> Vec<u8> {
    let mut out = Vec::with_capacity(16);
    encode_into(v, &mut out);
    out
}

/// One index-key component in the direction of its key-pattern field.
pub fn component(v: &Bson, ascending: bool) -> Vec<u8> {
    let mut e = encode(v);
    if !ascending {
        invert(&mut e);
    }
    e
}

pub fn invert(b: &mut [u8]) {
    for x in b {
        *x = !*x;
    }
}

/// Smallest byte string greater than every string that starts with `p`; `None` if there is none.
pub fn prefix_end(p: &[u8]) -> Option<Vec<u8>> {
    let mut e = p.to_vec();
    while let Some(last) = e.pop() {
        if last != 0xFF {
            e.push(last + 1);
            return Some(e);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::{doc, oid::ObjectId, DateTime};
    use std::cmp::Ordering;

    fn sample() -> Vec<Bson> {
        vec![
            Bson::MinKey,
            Bson::Null,
            Bson::Undefined,
            Bson::Double(f64::NAN),
            Bson::Double(f64::NEG_INFINITY),
            Bson::Int64(i64::MIN),
            Bson::Double(-1.5),
            Bson::Int32(-2),
            Bson::Int32(0),
            Bson::Double(0.25),
            Bson::Int32(1),
            Bson::Int64(1),
            Bson::Double(1.0),
            Bson::Int64((1 << 60) + 1),
            Bson::Int64(1 << 60),
            Bson::Double(f64::INFINITY),
            Bson::String("".into()),
            Bson::String("a".into()),
            Bson::String("a\0".into()),
            Bson::String("a\0b".into()),
            Bson::String("ab".into()),
            Bson::String("b".into()),
            Bson::Document(doc! {}),
            Bson::Document(doc! { "a": 1 }),
            Bson::Document(doc! { "a": 1, "b": 2 }),
            Bson::Document(doc! { "a": "x" }),
            Bson::Document(doc! { "b": 1 }),
            Bson::Array(vec![]),
            Bson::Array(vec![Bson::Int32(1)]),
            Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]),
            Bson::Array(vec![Bson::Int32(2)]),
            Bson::ObjectId(ObjectId::from_bytes([1; 12])),
            Bson::ObjectId(ObjectId::from_bytes([2; 12])),
            Bson::Boolean(false),
            Bson::Boolean(true),
            Bson::DateTime(DateTime::from_millis(-5)),
            Bson::DateTime(DateTime::from_millis(1_700_000_000_000)),
            Bson::MaxKey,
        ]
    }

    #[test]
    fn bytes_follow_mongodb_order() {
        let vals = sample();
        for a in &vals {
            for b in &vals {
                let want = crate::query::compare(a, b);
                let got = encode(a).cmp(&encode(b));
                // NaN and null/undefined compare equal in `compare`; the encoding keeps them apart.
                if want != Ordering::Equal {
                    assert_eq!(got, want, "{a:?} vs {b:?}");
                }
                let mut ea = encode(a);
                let mut eb = encode(b);
                invert(&mut ea);
                invert(&mut eb);
                if want != Ordering::Equal {
                    assert_eq!(ea.cmp(&eb), want.reverse(), "desc {a:?} vs {b:?}");
                }
            }
        }
    }

    #[test]
    fn equal_numbers_encode_identically() {
        assert_eq!(encode(&Bson::Int32(7)), encode(&Bson::Double(7.0)));
        assert_eq!(encode(&Bson::Int64(7)), encode(&Bson::Int32(7)));
        assert_ne!(encode(&Bson::Int64((1 << 60) + 1)), encode(&Bson::Int64(1 << 60)));
    }

    #[test]
    fn components_are_prefix_free() {
        let vals = sample();
        for a in &vals {
            for b in &vals {
                let (ea, eb) = (encode(a), encode(b));
                if ea != eb {
                    assert!(!eb.starts_with(&ea), "{a:?} is a prefix of {b:?}");
                }
            }
        }
    }

    #[test]
    fn compound_keys_sort_component_by_component() {
        let k = |a: &str, b: i32| {
            let mut x = component(&Bson::String(a.into()), true);
            x.extend(component(&Bson::Int32(b), false));
            x
        };
        // ascending a, descending b
        assert!(k("a", 5) < k("a", 1));
        assert!(k("a", 1) < k("ab", 9));
        assert!(k("a", 1) < k("b", 9));
    }

    #[test]
    fn prefix_end_bounds_a_prefix() {
        assert_eq!(prefix_end(&[1, 2]), Some(vec![1, 3]));
        assert_eq!(prefix_end(&[1, 0xFF]), Some(vec![2]));
        assert_eq!(prefix_end(&[0xFF]), None);
    }
}
