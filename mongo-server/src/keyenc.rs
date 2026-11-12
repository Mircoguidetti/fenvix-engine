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
    let int_of = |f: f64| if f.is_finite() && f.fract() == 0.0 && f.abs() < 9.2e18 { Some(f as i64) } else { None };
    let mut tail = None;
    let (f, exact): (f64, Option<i64>) = match v {
        Bson::Int32(i) => (*i as f64, Some(*i as i64)),
        Bson::Int64(i) => (*i as f64, Some(*i)),
        Bson::Double(f) => (*f, int_of(*f)),
        Bson::Decimal128(d) => {
            let f: f64 = d.to_string().parse().unwrap_or(f64::NAN);
            // A decimal equal to a double or to a 64-bit integer encodes as that number (one key, as on mongod); any
            // other decimal (0.1, 1E+400) is a different key (review of 62b8619, P8): see below.
            match crate::query::decimal_key_tail(v, f) {
                (Some(i), None) => (f, Some(i)),
                (_, t) => {
                    tail = t;
                    (f, int_of(f))
                }
            }
        }
        _ => (f64::NAN, None),
    };
    if let Some(t) = tail {
        // its nearest double, then a mark where a number has its integer or zeros (`decimal_mark`: no number with that
        // nearest double has it, so no number's key is a prefix of this one), then its exact value (escaped, terminated)
        out.push(1);
        out.extend(sortable_f64(f));
        out.extend(decimal_mark(f));
        escaped(t.as_bytes(), out);
        return;
    }
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

/// The 8 bytes after the nearest double `f` that mark a Decimal128 with an exact-value tail. A number's own 8 bytes are
/// its integer (when `f` is a whole number below 9.2e18 in size) or zeros: the integers with nearest double `f` are near
/// `f`, so 0xFF x 8 (Int64::MAX) cannot be one of them below 9.2e18, and 0x80 00.. (the integer 0) cannot be one at or
/// above it.
fn decimal_mark(f: f64) -> [u8; 8] {
    if f.abs() < 9.2e18 { [0xFF; 8] } else { sortable_i64(0) }
}

/// The nearest double of a number key's 8 double bytes (inverse of `sortable_f64`).
fn f64_of_sortable(b: &[u8]) -> f64 {
    let t = u64::from_be_bytes(b.try_into().unwrap());
    let bits = if t >> 63 == 1 { t & !(1 << 63) } else { !t };
    f64::from_bits(bits)
}

/// The first bytes of every key of a number whose nearest double is that of `v` (an index range widened to them reads
/// every number equal to or within one double step of a bound; documents are matched against the filter after).
pub fn number_group(v: &Bson) -> Option<Vec<u8>> {
    let e = encode(v);
    (e.len() >= 10 && e[1] == 1).then(|| e[..10].to_vec())
}

fn body(v: &Bson, out: &mut Vec<u8>) {
    match v {
        Bson::MinKey | Bson::MaxKey => {}
        Bson::Undefined => out.push(0),
        Bson::Null => out.push(1),
        Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_) => number(v, out),
        Bson::String(s) | Bson::Symbol(s) => match crate::collation::sort_key_current(s) {
            // an index with a collation holds the collation keys of its strings
            Some(k) => escaped(&k, out),
            None => escaped(s.as_bytes(), out),
        },
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

/// The length of the one ascending component at the start of `b` (the inverse of [`encode`]'s framing: the encoding is
/// prefix-free, so a component ends where its own structure says). `None` if `b` does not start with a whole component.
/// Layout v2 (`custkey.rs`) uses it to find the customer component inside a key.
pub fn skip(b: &[u8]) -> Option<usize> {
    let mut i = 0;
    skip_val(b, &mut i)?;
    Some(i)
}

fn take(b: &[u8], i: &mut usize, n: usize) -> Option<()> {
    if *i + n > b.len() {
        return None;
    }
    *i += n;
    Some(())
}

fn skip_escaped(b: &[u8], i: &mut usize) -> Option<()> {
    loop {
        let x = *b.get(*i)?;
        *i += 1;
        if x == 0 {
            let y = *b.get(*i)?;
            *i += 1;
            match y {
                1 => return Some(()),
                0xFF => continue,
                _ => return None,
            }
        }
    }
}

fn skip_val(b: &[u8], i: &mut usize) -> Option<()> {
    let rank = *b.get(*i)?;
    *i += 1;
    skip_body(rank, b, i)
}

fn skip_body(rank: u8, b: &[u8], i: &mut usize) -> Option<()> {
    match rank {
        1 | 20 => Some(()),
        2 => take(b, i, 1),
        3 => {
            let x = *b.get(*i)?;
            *i += 1;
            if x == 1 {
                take(b, i, 16)?;
                let f = f64_of_sortable(&b[*i - 16..*i - 8]);
                if b[*i - 8..*i] == decimal_mark(f) {
                    skip_escaped(b, i)?; // a decimal's exact value
                }
                Some(())
            } else {
                Some(())
            }
        }
        4 | 12 | 13 | 14 | 15 => {
            skip_escaped(b, i)?;
            if rank == 12 {
                skip_escaped(b, i)?;
            }
            Some(())
        }
        5 => loop {
            let r = *b.get(*i)?;
            *i += 1;
            if r == 0 {
                return Some(());
            }
            skip_escaped(b, i)?;
            skip_body(r, b, i)?;
        },
        6 => loop {
            if *b.get(*i)? == 0 {
                *i += 1;
                return Some(());
            }
            skip_val(b, i)?;
        },
        7 => {
            let n = u32::from_be_bytes(b.get(*i..*i + 4)?.try_into().ok()?) as usize;
            take(b, i, 4 + 1 + n)
        }
        8 => take(b, i, 12),
        9 => take(b, i, 1),
        10 | 11 => take(b, i, 8),
        _ => None,
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
    fn skip_finds_the_end_of_every_component() {
        let mut vals = sample();
        vals.push(Bson::Undefined);
        vals.push(Bson::Binary(bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: vec![0, 1, 2, 0] }));
        vals.push(Bson::Timestamp(bson::Timestamp { time: 5, increment: 6 }));
        vals.push(Bson::RegularExpression(bson::Regex { pattern: "ab".try_into().unwrap(), options: "i".try_into().unwrap() }));
        vals.push(Bson::Document(doc! { "a": [1, { "b\0": Bson::Null }], "c": "x\0y" }));
        vals.push(Bson::Decimal128("1.5".parse().unwrap()));
        // decimals with an exact-value tail (review of 62b8619, P8), next to the numbers sharing their nearest double
        for d in ["0.1", "10.0000000000000000001", "1E+400", "-1E+400", "9223372036854775807.5", "-9223372036854775808.5"] {
            vals.push(Bson::Decimal128(d.parse().unwrap()));
        }
        vals.push(Bson::Int64(i64::MAX));
        vals.push(Bson::Int64(i64::MIN));
        vals.push(Bson::Double(0.1));
        for v in &vals {
            let e = encode(v);
            assert_eq!(skip(&e), Some(e.len()), "{v:?}");
            let mut t = e.clone();
            t.extend([7, 0, 1, 2]);
            assert_eq!(skip(&t), Some(e.len()), "{v:?} with a tail");
            if e.len() > 1 {
                assert_eq!(skip(&e[..e.len() - 1]).filter(|n| *n == e.len()), None, "{v:?} cut");
            }
        }
    }

    #[test]
    fn decimals_are_one_key_only_with_equal_numbers() {
        let dec = |s: &str| Bson::Decimal128(s.parse().unwrap());
        let pairs_distinct = [(dec("0.1"), Bson::Double(0.1)), (dec("10.0000000000000000001"), Bson::Int32(10)), (dec("1E+400"), Bson::Double(f64::INFINITY)), (dec("9223372036854775807.5"), Bson::Int64(i64::MAX)), (dec("0.1"), dec("0.10000000000000001"))];
        for (a, b) in &pairs_distinct {
            let (ea, eb) = (encode(a), encode(b));
            assert_ne!(ea, eb, "{a:?} {b:?}");
            assert!(!ea.starts_with(&eb) && !eb.starts_with(&ea), "prefix: {a:?} {b:?}");
        }
        for (a, b) in [(dec("9007199254740993"), Bson::Int64(9007199254740993)), (dec("10.00"), Bson::Int32(10)), (dec("0.5"), Bson::Double(0.5)), (dec("Infinity"), Bson::Double(f64::INFINITY))] {
            assert_eq!(encode(&a), encode(&b), "{a:?} {b:?}");
        }
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
