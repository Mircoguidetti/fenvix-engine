//! Aggregation stages added in results9: `$bucketAuto`, `$densify`, `$fill`, `$setWindowFields`, `$graphLookup`, `$redact`,
//! `$documents`, `$planCacheStats`, plus the `$median` / `$percentile` helpers shared by accumulators and expressions.

use crate::agg::{accumulate, run_pipeline, Foreign};
use crate::error::MongoError;
use crate::query::{as_f64, compare, equal, is_numeric};
use bson::{doc, Bson, DateTime, Decimal128, Document};
use chrono::{Datelike, TimeZone, Timelike, Utc};
use std::cmp::Ordering;

type R<T> = Result<T, MongoError>;

fn err(code: i32, msg: impl Into<String>) -> MongoError {
    let name: &'static str = match code {
        9 => "FailedToParse",
        14 => "TypeMismatch",
        2 => "BadValue",
        40414 => "IDLFailedToParse",
        40415 => "IDLUnknownField",
        _ => Box::leak(format!("Location{code}").into_boxed_str()),
    };
    MongoError::new(code, name, msg.into())
}

fn ev(e: &Bson, d: &Document) -> R<Option<Bson>> {
    crate::expr::eval(e, &crate::expr::Scope::new(d))
}

fn num_i64(b: &Bson) -> Option<i64> {
    match b {
        Bson::Int32(i) => Some(*i as i64),
        Bson::Int64(i) => Some(*i),
        Bson::Double(f) if f.fract() == 0.0 && f.is_finite() => Some(*f as i64),
        _ => None,
    }
}

fn get_path(d: &Document, path: &str) -> Option<Bson> {
    crate::expr::field_path(d, path)
}

fn set_path(d: &mut Document, path: &str, v: Bson) -> R<()> {
    let parts: Vec<&str> = path.split('.').collect();
    crate::update::set_path(d, &parts, v)
}

// ---------------------------------------------------------------------------------------------
// $median / $percentile
// ---------------------------------------------------------------------------------------------

/// Nearest-rank percentiles of the numeric values (`p` in `[0, 1]`); `null` when there are none.
pub fn percentiles(values: &[Bson], ps: &[f64]) -> Vec<Bson> {
    let mut nums: Vec<f64> = values.iter().filter(|v| is_numeric(v)).map(as_f64).filter(|f| !f.is_nan()).collect();
    nums.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    ps.iter()
        .map(|p| {
            if nums.is_empty() {
                return Bson::Null;
            }
            let rank = ((p * nums.len() as f64).ceil() as usize).clamp(1, nums.len());
            let x = nums[rank - 1];
            // keep integer inputs integral
            Bson::Double(x)
        })
        .collect()
}

/// Parse `{input, p, method}` of `$percentile` (`p` required) or `$median` (`p` is 0.5).
pub fn percentile_spec(op: &str, spec: &Bson) -> R<(Bson, Vec<f64>, bool)> {
    let d = spec.as_document().ok_or_else(|| err(7429513, format!("{op} requires an object as its argument")))?;
    for k in d.keys() {
        if !matches!(k.as_str(), "input" | "p" | "method") {
            return Err(err(7429513, format!("{op} found an unknown argument: {k}")));
        }
    }
    let input = d.get("input").cloned().ok_or_else(|| err(7429513, format!("{op} requires an 'input' field")))?;
    match d.get("method") {
        Some(Bson::String(m)) if m == "approximate" => {}
        Some(Bson::String(m)) => return Err(err(2, format!("Currently only 'approximate' can be used as a $percentile 'method' but received '{m}'"))),
        Some(_) => return Err(err(7429501, "$percentile 'method' must be a string")),
        None => return Err(err(40414, format!("BSON field '{op}.method' is missing but a required field"))),
    }
    if op == "$median" {
        if d.contains_key("p") {
            return Err(err(40415, "BSON field '$median.p' is an unknown field."));
        }
        return Ok((input, vec![0.5], false));
    }
    let ps = match d.get("p") {
        Some(Bson::Array(a)) if !a.is_empty() => a.clone(),
        Some(Bson::Array(_)) => return Err(err(7750301, "'p' must not be empty")),
        _ => return Err(err(40414, "BSON field '$percentile.p' is missing but a required field")),
    };
    let mut out = Vec::new();
    for p in &ps {
        match p {
            x if is_numeric(x) && (0.0..=1.0).contains(&as_f64(x)) => out.push(as_f64(x)),
            _ => return Err(err(7750303, "'p' must be an array of numbers between 0 and 1")),
        }
    }
    Ok((input, out, true))
}

// ---------------------------------------------------------------------------------------------
// $bucketAuto
// ---------------------------------------------------------------------------------------------

pub fn bucket_auto(arg: &Bson, docs: Vec<Document>, foreign: &Foreign) -> R<Vec<Document>> {
    let spec = arg.as_document().ok_or_else(|| err(40240, "the $bucketAuto specification must be an object"))?;
    let mut group_by = None;
    let mut buckets: Option<i64> = None;
    let mut output = None;
    let mut granularity: Option<String> = None;
    for (k, v) in spec {
        match k.as_str() {
            "groupBy" => group_by = Some(v.clone()),
            "buckets" => match v {
                x if is_numeric(x) => match num_i64(x) {
                    Some(n) if n > 0 => buckets = Some(n),
                    Some(n) => return Err(err(40243, format!("The $bucketAuto 'buckets' field must be greater than 0, but found: {n}"))),
                    None => return Err(err(40242, "The $bucketAuto 'buckets' field must be representable as a 32-bit integer")),
                },
                other => return Err(err(40241, format!("The $bucketAuto 'buckets' field must be a numeric value, but found type: {}", crate::update::type_name(other)))),
            },
            "output" => match v {
                Bson::Document(d) => output = Some(d.clone()),
                _ => return Err(err(40244, "The $bucketAuto 'output' field must be an object")),
            },
            "granularity" => match v {
                Bson::String(s) => granularity = Some(s.clone()),
                _ => return Err(err(40261, "The $bucketAuto 'granularity' field must be a string")),
            },
            other => return Err(err(40245, format!("Unrecognized option to $bucketAuto: {other}"))),
        }
    }
    let group_by = group_by.ok_or_else(|| err(40246, "$bucketAuto requires 'groupBy' and 'buckets' to be specified"))?;
    let buckets = buckets.ok_or_else(|| err(40246, "$bucketAuto requires 'groupBy' and 'buckets' to be specified"))? as usize;
    let grid = match granularity.as_deref() {
        None => None,
        Some(name) => Some(Rounder::named(name).ok_or_else(|| err(40257, format!("Unknown rounding granularity '{name}'")))?),
    };
    let mut keyed: Vec<(Bson, Document)> = Vec::with_capacity(docs.len());
    for d in docs {
        let k = ev(&group_by, &d)?;
        if grid.is_some() {
            match &k {
                Some(v) if is_numeric(v) => {
                    if as_f64(v).is_nan() {
                        return Err(err(40259, "$bucketAuto can specify a 'granularity' with numeric boundaries only, but found a NaN"));
                    }
                    if as_f64(v) < 0.0 {
                        return Err(err(40260, "$bucketAuto can specify a 'granularity' with non-negative numbers only, but found a negative number"));
                    }
                }
                Some(v) => return Err(err(40258, format!("$bucketAuto can specify a 'granularity' with numeric boundaries only, but found a value with type: {}", crate::update::type_name(v)))),
                None => return Err(err(40258, "$bucketAuto can specify a 'granularity' with numeric boundaries only, but found a value with type: missing")),
            }
        }
        keyed.push((k.unwrap_or(Bson::Null), d));
    }
    keyed.sort_by(|a, b| compare(&a.0, &b.0));
    let n = keyed.len();
    if n == 0 {
        return Ok(vec![]);
    }
    // bucket boundaries as indexes: each of the first buckets takes round(n / buckets) documents, equal keys stay together
    let size = ((n as f64 / buckets as f64) + 0.5).floor() as usize;
    let size = size.max(1);
    let out_spec = match output {
        Some(o) if !o.is_empty() => o,
        _ => doc! { "count": { "$sum": 1 } },
    };
    let mut ranges: Vec<(usize, usize)> = Vec::new();
    let mut bounds: Option<(Bson, Vec<Bson>)> = None;
    let mut start = 0;
    if let Some(g) = &grid {
        // With a granularity a bucket takes its share of documents, ends at the first series number above its last value
        // (below the next document's value when that last value is zero) and takes every following document below it.
        let min0 = g.round_down(&keyed[0].0)?;
        let mut ends: Vec<Bson> = Vec::new();
        while start < n {
            let mut end = if ranges.len() + 1 == buckets { n } else { (start + size).min(n) };
            while end < n && end > start && equal(&keyed[end].0, &keyed[end - 1].0) {
                end += 1;
            }
            let last = &keyed[end - 1].0;
            let mut boundary = if end < n && as_f64(last) == 0.0 { g.round_down(&keyed[end].0)? } else { g.round_up(last)? };
            if ranges.len() + 1 != buckets {
                while end < n && compare(&keyed[end].0, &boundary) == Ordering::Less {
                    end += 1;
                }
            }
            if end == n && compare(&keyed[n - 1].0, &boundary) != Ordering::Less {
                boundary = g.round_up(&keyed[n - 1].0)?;
            }
            ranges.push((start, end));
            ends.push(boundary);
            start = end;
        }
        bounds = Some((min0, ends));
    } else {
        while start < n {
            let last_bucket = ranges.len() + 1 == buckets;
            let mut end = if last_bucket { n } else { (start + size).min(n) };
            while end < n && end > start && equal(&keyed[end].0, &keyed[end - 1].0) {
                end += 1;
            }
            ranges.push((start, end));
            start = end;
        }
    }
    let mut res = Vec::new();
    for (i, (s, e)) in ranges.iter().enumerate() {
        let (min, max) = match &bounds {
            Some((min0, ends)) => (if i == 0 { min0.clone() } else { ends[i - 1].clone() }, ends[i].clone()),
            None => (keyed[*s].0.clone(), if i + 1 < ranges.len() { keyed[ranges[i + 1].0].0.clone() } else { keyed[n - 1].0.clone() }),
        };
        let members: Vec<Document> = keyed[*s..*e].iter().map(|(_, d)| d.clone()).collect();
        let mut gspec = out_spec.clone();
        gspec.insert("_id", Bson::Null);
        let g = run_pipeline(members, &[Bson::Document(doc! { "$group": gspec })], foreign)?;
        let mut o = doc! { "_id": { "min": min, "max": max } };
        if let Some(gd) = g.into_iter().next() {
            for (k, v) in gd {
                if k != "_id" {
                    o.insert(k, v);
                }
            }
        }
        res.push(o);
    }
    Ok(res)
}

// Preferred-number series as MongoDB's $bucketAuto rounders define them (read off a mongod 7.0): the integer mantissas,
// the number of digits of each, so that `mantissa * 10^(exponent - digits + 1)` reproduces its floating-point results.
const SERIES: &[(&str, i32, &[i64])] = &[
    ("R5", 2, &[10, 16, 25, 40, 63]),
    ("R10", 3, &[100, 125, 160, 200, 250, 315, 400, 500, 630, 800]),
    ("R20", 3, &[100, 112, 125, 140, 160, 180, 200, 224, 250, 280, 315, 355, 400, 450, 500, 560, 630, 710, 800, 900]),
    ("R40", 3, &[100, 106, 112, 118, 125, 132, 140, 150, 160, 170, 180, 190, 200, 212, 224, 236, 250, 265, 280, 300, 315, 355, 375, 400, 425, 450, 475, 500, 530, 560, 600, 630, 670, 710, 750, 800, 850, 900, 950]),
    ("R80", 3, &[103, 109, 115, 122, 128, 136, 145, 155, 165, 175, 185, 195, 206, 218, 230, 243, 258, 272, 290, 307, 325, 345, 365, 387, 412, 437, 462, 487, 515, 545, 575, 615, 650, 690, 730, 775, 825, 875, 925, 975]),
    ("1-2-5", 2, &[10, 20, 50]),
    ("E6", 2, &[10, 15, 22, 33, 47, 68]),
    ("E12", 2, &[10, 12, 15, 18, 22, 27, 33, 39, 47, 56, 68, 82]),
    ("E24", 2, &[10, 11, 12, 13, 15, 16, 18, 20, 22, 24, 27, 30, 33, 36, 39, 43, 47, 51, 56, 62, 68, 75, 82, 91]),
    ("E48", 3, &[100, 105, 110, 115, 121, 127, 133, 140, 147, 154, 162, 169, 178, 187, 196, 205, 215, 226, 237, 249, 261, 274, 287, 301, 316, 332, 348, 365, 383, 402, 422, 442, 464, 487, 511, 536, 562, 590, 619, 649, 681, 715, 750, 787, 825, 866, 909, 953]),
    ("E96", 3, &[100, 102, 105, 107, 110, 113, 115, 118, 121, 124, 127, 130, 133, 137, 140, 143, 147, 150, 154, 158, 162, 165, 169, 174, 178, 182, 187, 191, 196, 200, 205, 210, 215, 221, 226, 232, 237, 243, 249, 255, 261, 267, 274, 280, 287, 294, 301, 309, 316, 324, 332, 340, 348, 357, 365, 374, 383, 392, 402, 412, 422, 432, 442, 453, 464, 475, 487, 499, 511, 523, 536, 549, 562, 576, 590, 604, 619, 634, 649, 665, 681, 698, 715, 732, 750, 768, 787, 806, 825, 845, 866, 887, 909, 931, 953, 976]),
    ("E192", 3, &[100, 101, 102, 104, 105, 106, 107, 109, 110, 111, 113, 114, 115, 117, 118, 120, 121, 123, 124, 126, 127, 129, 130, 132, 133, 135, 137, 138, 140, 142, 143, 145, 147, 149, 150, 152, 154, 156, 158, 160, 162, 164, 165, 167, 169, 172, 174, 176, 178, 180, 182, 184, 187, 189, 191, 193, 196, 198, 200, 203, 205, 208, 210, 213, 215, 218, 221, 223, 226, 229, 232, 234, 237, 240, 243, 246, 249, 252, 255, 258, 261, 264, 267, 271, 274, 277, 280, 284, 287, 291, 294, 298, 301, 305, 309, 312, 316, 320, 324, 328, 332, 336, 340, 344, 348, 352, 357, 361, 365, 370, 374, 379, 383, 388, 392, 397, 402, 407, 412, 417, 422, 427, 432, 437, 442, 448, 453, 459, 464, 470, 475, 481, 487, 493, 499, 505, 511, 517, 523, 530, 536, 542, 549, 556, 562, 569, 576, 583, 590, 597, 604, 612, 619, 626, 634, 642, 649, 657, 665, 673, 681, 690, 698, 706, 715, 723, 732, 741, 750, 759, 768, 777, 787, 796, 806, 816, 825, 835, 845, 856, 866, 876, 887, 898, 909, 920, 931, 942, 953, 965, 976, 988]),
];

/// A `$bucketAuto` granularity: round a number to the next series number above it or the next one below it.
enum Rounder {
    Pow2,
    /// Mantissas of one decade and the number of digits they are written with.
    Preferred(&'static [i64], i32),
}

impl Rounder {
    fn named(name: &str) -> Option<Rounder> {
        if name == "POWERSOF2" {
            return Some(Rounder::Pow2);
        }
        SERIES.iter().find(|(n, _, _)| *n == name).map(|(_, d, m)| Rounder::Preferred(m, *d))
    }

    /// `10^e` the way MongoDB builds it: by repeated multiplication or division, which is where the trailing digits of
    /// `6.300000000000001` come from.
    fn scale(e: i32) -> f64 {
        let mut m = 1.0f64;
        for _ in 0..e.unsigned_abs() {
            m = if e < 0 { m / 10.0 } else { m * 10.0 };
        }
        m
    }

    fn members(table: &[i64], digits: i32, e: i32) -> impl Iterator<Item = f64> + '_ {
        let mult = Self::scale(e - digits + 1);
        table.iter().map(move |m| *m as f64 * mult)
    }

    fn finite(v: &Bson) -> R<f64> {
        let x = as_f64(v);
        if x.is_infinite() {
            return Err(err(11785400, "A granularity rounder can only round finite numbers"));
        }
        Ok(x)
    }

    fn up(&self, v: &Bson) -> R<Bson> {
        let x = as_f64(v);
        if x == 0.0 {
            return Ok(v.clone());
        }
        if let Bson::Decimal128(d) = v {
            return self.decimal(d, true);
        }
        match self {
            Rounder::Pow2 => Ok(pow2_bson(2f64.powf(Self::finite(v)?.log2().floor() + 1.0))),
            Rounder::Preferred(_, _) if x.is_infinite() => Ok(Bson::Double(x)),
            Rounder::Preferred(t, d) => {
                let e = x.log10().floor() as i32;
                for dec in e - 1..=e + 2 {
                    if let Some(m) = Self::members(t, *d, dec).find(|m| *m > x) {
                        return Ok(Bson::Double(m));
                    }
                }
                Ok(Bson::Double(x))
            }
        }
    }

    fn down(&self, v: &Bson) -> R<Bson> {
        let x = as_f64(v);
        if x == 0.0 {
            return Ok(v.clone());
        }
        if let Bson::Decimal128(d) = v {
            return self.decimal(d, false);
        }
        match self {
            Rounder::Pow2 => {
                let l = Self::finite(v)?.log2();
                Ok(pow2_bson(2f64.powf(if l.fract() == 0.0 { l - 1.0 } else { l.floor() })))
            }
            Rounder::Preferred(_, _) if x.is_infinite() => Ok(Bson::Double(x)),
            Rounder::Preferred(t, d) => {
                let e = x.log10().floor() as i32;
                for dec in (e - 2..=e + 1).rev() {
                    if let Some(m) = Self::members(t, *d, dec).filter(|m| *m < x).last() {
                        return Ok(Bson::Double(m));
                    }
                }
                Ok(Bson::Double(x))
            }
        }
    }
}

/// A positive decimal as `d1.d2d3... * 10^adj`, digits without leading or trailing zeros.
#[derive(Clone, Debug)]
struct Dec {
    digits: Vec<u8>,
    adj: i32,
}

impl Dec {
    fn parse(s: &str) -> Option<Dec> {
        let s = s.trim_start_matches('-');
        let (mant, exp) = match s.split_once(['E', 'e']) {
            Some((m, e)) => (m, e.parse::<i32>().ok()?),
            None => (s, 0),
        };
        let (int, frac) = mant.split_once('.').unwrap_or((mant, ""));
        let all: Vec<u8> = int.bytes().chain(frac.bytes()).map(|c| c.wrapping_sub(b'0')).collect();
        if all.iter().any(|d| *d > 9) {
            return None;
        }
        let lead = all.iter().position(|d| *d != 0)?;
        let mut digits: Vec<u8> = all[lead..].to_vec();
        while digits.last() == Some(&0) {
            digits.pop();
        }
        let adj = int.len() as i32 - 1 - lead as i32 + exp;
        Some(Dec { digits, adj })
    }

    fn of_mantissa(m: i64, adj: i32) -> Dec {
        let mut digits: Vec<u8> = m.to_string().bytes().map(|c| c - b'0').collect();
        while digits.last() == Some(&0) {
            digits.pop();
        }
        Dec { digits, adj }
    }

    fn cmp(&self, o: &Dec) -> Ordering {
        self.adj.cmp(&o.adj).then_with(|| {
            let n = self.digits.len().max(o.digits.len());
            (0..n).map(|i| self.digits.get(i).unwrap_or(&0).cmp(o.digits.get(i).unwrap_or(&0))).find(|c| *c != Ordering::Equal).unwrap_or(Ordering::Equal)
        })
    }

    /// `2^k` exactly.
    fn pow2(k: i32) -> Dec {
        // base 10^9 limbs, least significant first; 2^k for k < 0 is 5^|k| * 10^k
        let (base, n) = if k >= 0 { (2u64, k) } else { (5u64, -k) };
        let mut limbs: Vec<u64> = vec![1];
        for _ in 0..n {
            let mut carry = 0u64;
            for l in limbs.iter_mut() {
                let v = *l * base + carry;
                *l = v % 1_000_000_000;
                carry = v / 1_000_000_000;
            }
            if carry > 0 {
                limbs.push(carry);
            }
        }
        let mut text = limbs.last().unwrap().to_string();
        for l in limbs.iter().rev().skip(1) {
            text.push_str(&format!("{l:09}"));
        }
        let mut digits: Vec<u8> = text.bytes().map(|c| c - b'0').collect();
        let adj = digits.len() as i32 - 1 + if k >= 0 { 0 } else { k };
        while digits.last() == Some(&0) {
            digits.pop();
        }
        Dec { digits, adj }
    }

    /// The Decimal128 with `precision` significant digits (zero padded, half-even rounded) of this number.
    fn to_sig_digits(&self, precision: usize) -> Bson {
        let mut d: Vec<u8> = self.digits.clone();
        let mut adj = self.adj;
        if d.len() > precision {
            let tail = &d[precision..];
            let up = match tail[0].cmp(&5) {
                Ordering::Greater => true,
                Ordering::Less => false,
                Ordering::Equal => tail[1..].iter().any(|x| *x != 0) || d[precision - 1] % 2 == 1,
            };
            d.truncate(precision);
            if up {
                let mut i = precision;
                loop {
                    if i == 0 {
                        d.insert(0, 1);
                        d.truncate(precision);
                        adj += 1;
                        break;
                    }
                    i -= 1;
                    if d[i] == 9 {
                        d[i] = 0;
                    } else {
                        d[i] += 1;
                        break;
                    }
                }
            }
        }
        d.resize(precision, 0);
        let coeff: String = d.iter().map(|x| (b'0' + x) as char).collect();
        dec_bson(&format!("{coeff}E{}", adj - precision as i32 + 1))
    }
}

fn dec_bson(s: &str) -> Bson {
    s.parse::<Decimal128>().map(Bson::Decimal128).unwrap_or(Bson::Null)
}

impl Rounder {
    /// Rounding a Decimal128 keeps the type and, like mongod, a fixed number of digits: the preferred series are written
    /// with 15 significant digits below ten, from ten up, 13 decimals (12 from a hundred up for the three-digit series), powers of two with 34 significant digits.
    fn decimal(&self, d: &Decimal128, up: bool) -> R<Bson> {
        let text = d.to_string();
        if text.contains("NaN") {
            return Err(err(40259, "$bucketAuto can specify a 'granularity' with numeric boundaries only, but found a NaN"));
        }
        // beyond the double range mongod treats the number as infinite
        if text.contains("Infinity") || text.parse::<f64>().map(f64::is_infinite).unwrap_or(false) {
            return match self {
                Rounder::Pow2 => Err(err(11785400, "A granularity rounder can only round finite numbers")),
                _ => Ok(Bson::Decimal128(*d)),
            };
        }
        let Some(x) = Dec::parse(&text) else { return Ok(Bson::Decimal128(*d)) };
        match self {
            Rounder::Pow2 => {
                // exponent estimate from the leading digits, corrected against the exact powers
                let lead: f64 = x.digits.iter().take(15).fold(0.0, |a, d| a * 10.0 + *d as f64) / 10f64.powi(x.digits.len().min(15) as i32 - 1);
                let mut k = ((lead.log10() + x.adj as f64) / 2f64.log10()).floor() as i32;
                while Dec::pow2(k).cmp(&x) == Ordering::Greater {
                    k -= 1;
                }
                while Dec::pow2(k + 1).cmp(&x) != Ordering::Greater {
                    k += 1;
                }
                // 2^k <= x < 2^(k+1)
                let r = if up { k + 1 } else if Dec::pow2(k).cmp(&x) == Ordering::Equal { k - 1 } else { k };
                Ok(Dec::pow2(r).to_sig_digits(34))
            }
            Rounder::Preferred(t, dg) => {
                let pick = |dec: i32| -> Vec<Dec> { t.iter().map(|m| Dec::of_mantissa(*m, dec)).collect() };
                let chosen = if up {
                    (x.adj - 1..=x.adj + 2).find_map(|dec| pick(dec).into_iter().find(|m| m.cmp(&x) == Ordering::Greater))
                } else {
                    (x.adj - 2..=x.adj + 1).rev().find_map(|dec| pick(dec).into_iter().rev().find(|m| m.cmp(&x) == Ordering::Less))
                };
                let Some(m) = chosen else { return Ok(Bson::Decimal128(*d)) };
                let decimals = if m.adj >= 2 && *dg == 3 { 12 } else if m.adj >= 1 { 13 } else { 14 - m.adj };
                let shift = m.adj - m.digits.len() as i32 + 1 + decimals;
                let coeff: String = m.digits.iter().map(|x| (b'0' + x) as char).chain(std::iter::repeat('0').take(shift.max(0) as usize)).collect();
                Ok(dec_bson(&format!("{coeff}E-{decimals}")))
            }
        }
    }
}

impl Rounder {
    fn round_up(&self, v: &Bson) -> R<Bson> {
        self.up(v)
    }

    fn round_down(&self, v: &Bson) -> R<Bson> {
        self.down(v)
    }
}

/// Powers of two come back as integers when they are whole.
fn pow2_bson(f: f64) -> Bson {
    if f.fract() == 0.0 && f.abs() < 9.2e18 {
        if f.abs() < 2147483648.0 { Bson::Int32(f as i32) } else { Bson::Int64(f as i64) }
    } else {
        Bson::Double(f)
    }
}

// ---------------------------------------------------------------------------------------------
// $densify
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq)]
enum Unit {
    Ms,
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

fn add_unit(ms: i64, unit: Unit, step: i64) -> i64 {
    match unit {
        Unit::Ms => ms + step,
        Unit::Second => ms + step * 1000,
        Unit::Minute => ms + step * 60_000,
        Unit::Hour => ms + step * 3_600_000,
        Unit::Day => ms + step * 86_400_000,
        Unit::Week => ms + step * 7 * 86_400_000,
        Unit::Month | Unit::Quarter | Unit::Year => {
            let months = match unit {
                Unit::Month => step,
                Unit::Quarter => step * 3,
                _ => step * 12,
            };
            let dt = Utc.timestamp_millis_opt(ms).single().unwrap_or_default();
            let total = dt.year() as i64 * 12 + dt.month0() as i64 + months;
            let (y, m) = (total.div_euclid(12), total.rem_euclid(12) as u32 + 1);
            let mut day = dt.day();
            loop {
                if let Some(nd) = chrono::NaiveDate::from_ymd_opt(y as i32, m, day) {
                    let t = nd.and_hms_milli_opt(dt.hour(), dt.minute(), dt.second(), dt.timestamp_subsec_millis()).unwrap();
                    return Utc.from_utc_datetime(&t).timestamp_millis();
                }
                day -= 1;
            }
        }
    }
}

fn parse_unit(s: &str) -> R<Unit> {
    Ok(match s {
        "millisecond" => Unit::Ms,
        "second" => Unit::Second,
        "minute" => Unit::Minute,
        "hour" => Unit::Hour,
        "day" => Unit::Day,
        "week" => Unit::Week,
        "month" => Unit::Month,
        "quarter" => Unit::Quarter,
        "year" => Unit::Year,
        other => return Err(err(9, format!("unknown time unit value: {other}"))),
    })
}

pub fn densify(arg: &Bson, docs: Vec<Document>) -> R<Vec<Document>> {
    let spec = arg.as_document().ok_or_else(|| err(5733400, "the $densify stage specification must be an object"))?;
    for k in spec.keys() {
        if !matches!(k.as_str(), "field" | "partitionByFields" | "range") {
            return Err(err(40415, format!("BSON field '$densify.{k}' is an unknown field.")));
        }
    }
    let field = spec.get_str("field").map_err(|_| err(40414, "BSON field '$densify.field' is missing but a required field"))?.to_string();
    let range = match spec.get("range") {
        Some(Bson::Document(r)) => r.clone(),
        Some(_) => return Err(err(14, "BSON field '$densify.range' is the wrong type")),
        None => return Err(err(40414, "BSON field '$densify.range' is missing but a required field")),
    };
    let parts: Vec<String> = match spec.get("partitionByFields") {
        None => vec![],
        Some(Bson::Array(a)) => a.iter().map(|x| x.as_str().map(str::to_string).ok_or_else(|| err(14, "partitionByFields must be strings"))).collect::<R<_>>()?,
        Some(_) => return Err(err(14, "partitionByFields must be an array")),
    };
    let step = range.get("step").cloned().ok_or_else(|| err(40414, "BSON field 'range.step' is missing but a required field"))?;
    if !is_numeric(&step) || as_f64(&step) <= 0.0 {
        return Err(err(5733401, "The step parameter in a range statement must be a strictly positive numeric value"));
    }
    let unit = match range.get("unit") {
        None => None,
        Some(Bson::String(u)) => Some(parse_unit(u)?),
        Some(_) => return Err(err(14, "range.unit must be a string")),
    };
    let bounds = range.get("bounds").cloned().ok_or_else(|| err(40414, "BSON field 'range.bounds' is missing but a required field"))?;
    enum B {
        Full,
        Partition,
        Explicit(Bson, Bson),
    }
    let b = match &bounds {
        Bson::String(s) if s == "full" => B::Full,
        Bson::String(s) if s == "partition" => B::Partition,
        Bson::String(_) => return Err(err(5946802, "Bounds string must either be 'full' or 'partition'")),
        Bson::Array(a) if a.len() == 2 => {
            if compare(&a[0], &a[1]) == Ordering::Greater {
                return Err(err(5733402, "The lower bound must be less than or equal to the upper bound"));
            }
            B::Explicit(a[0].clone(), a[1].clone())
        }
        _ => return Err(err(5733403, "Bounds must be a 2 element array")),
    };
    let date_mode = matches!(&b, B::Explicit(Bson::DateTime(_), _));
    // documents without the field come first, unchanged
    let mut missing: Vec<Document> = Vec::new();
    let mut with: Vec<(Vec<Bson>, Bson, Document)> = Vec::new();
    for d in docs {
        match get_path(&d, &field) {
            None => missing.push(d),
            Some(v) => {
                if !is_numeric(&v) && !matches!(v, Bson::DateTime(_)) {
                    return Err(err(5733201, "Densify field type must be numeric or a date"));
                }
                let key: Vec<Bson> = parts.iter().map(|p| get_path(&d, p).unwrap_or(Bson::Null)).collect();
                with.push((key, v, d));
            }
        }
    }
    let is_date = date_mode || with.iter().any(|(_, v, _)| matches!(v, Bson::DateTime(_)));
    if is_date && unit.is_none() {
        return Err(err(6053600, "Encountered date densify value in collection when step does not have a date unit."));
    }
    if !is_date && unit.is_some() {
        return Err(err(6053600, "Encountered numeric densify value in collection when step has a date unit."));
    }
    // order: by partition, then by value
    with.sort_by(|a, b| {
        for (x, y) in a.0.iter().zip(&b.0) {
            let o = compare(x, y);
            if o != Ordering::Equal {
                return o;
            }
        }
        compare(&a.1, &b.1)
    });
    let gmin = with.iter().map(|(_, v, _)| v.clone()).min_by(compare);
    let gmax = with.iter().map(|(_, v, _)| v.clone()).max_by(compare);
    let to_ms = |v: &Bson| match v {
        Bson::DateTime(d) => d.timestamp_millis(),
        _ => 0,
    };
    let step_f = as_f64(&step);
    let mut out: Vec<Document> = missing;
    if matches!(b, B::Full) && !parts.is_empty() && !with.is_empty() {
        // full bounds with partitions: documents stream in field order and every partition is densified from the global
        // minimum, up to the next document of that partition, and finally up to the global maximum
        with.sort_by(|a, c| compare(&a.1, &c.1));
        let (gmin, gmax) = (gmin.clone().unwrap(), gmax.clone().unwrap());
        let next_of = |cur: &Bson| -> R<Bson> {
            Ok(match cur {
                Bson::DateTime(d) => {
                    let st = num_i64(&step).ok_or_else(|| err(5733401, "step must be an integer for date densification"))?;
                    Bson::DateTime(DateTime::from_millis(add_unit(d.timestamp_millis(), unit.unwrap(), st)))
                }
                Bson::Double(f) => Bson::Double(f + step_f),
                other => {
                    let int_step = matches!(step, Bson::Int32(_) | Bson::Int64(_));
                    if int_step { Bson::Int32((as_f64(other) + step_f) as i32) } else { Bson::Double(as_f64(other) + step_f) }
                }
            })
        };
        let mut order: Vec<Vec<Bson>> = Vec::new();
        let mut cursor: Vec<Bson> = Vec::new();
        let new_doc = |key: &Vec<Bson>, v: &Bson| -> Document {
            let mut nd = Document::new();
            for (p, k) in parts.iter().zip(key) {
                let _ = set_path(&mut nd, p, k.clone());
            }
            let _ = set_path(&mut nd, &field, v.clone());
            nd
        };
        for (key, v, d) in &with {
            let pi = match order.iter().position(|k| k == key) {
                Some(i) => i,
                None => {
                    order.push(key.clone());
                    cursor.push(gmin.clone());
                    order.len() - 1
                }
            };
            while compare(&cursor[pi], v) == Ordering::Less {
                let exists = with.iter().any(|(k2, v2, _)| k2 == key && equal(v2, &cursor[pi]));
                if !exists {
                    out.push(new_doc(key, &cursor[pi]));
                }
                cursor[pi] = next_of(&cursor[pi])?;
            }
            if equal(&cursor[pi], v) {
                cursor[pi] = next_of(&cursor[pi])?;
            }
            out.push(d.clone());
        }
        for (pi, key) in order.iter().enumerate() {
            while compare(&cursor[pi], &gmax) == Ordering::Less {
                let exists = with.iter().any(|(k2, v2, _)| k2 == key && equal(v2, &cursor[pi]));
                if !exists {
                    out.push(new_doc(key, &cursor[pi]));
                }
                cursor[pi] = next_of(&cursor[pi])?;
            }
        }
        return Ok(out);
    }
    let mut i = 0;
    while i < with.len() {
        let mut j = i;
        while j < with.len() && with[j].0 == with[i].0 {
            j += 1;
        }
        let group = &with[i..j];
        let key = group[0].0.clone();
        let (lo, hi): (Bson, Bson) = match &b {
            B::Full => (gmin.clone().unwrap(), gmax.clone().unwrap()),
            B::Partition => (group[0].1.clone(), group[group.len() - 1].1.clone()),
            B::Explicit(l, h) => (l.clone(), h.clone()),
        };
        let explicit = matches!(b, B::Explicit(..));
        // grid points
        let mut grid: Vec<Bson> = Vec::new();
        if is_date {
            let u = unit.unwrap();
            let st = num_i64(&step).ok_or_else(|| err(5733401, "step must be an integer for date densification"))?;
            let (mut cur, end) = (to_ms(&lo), to_ms(&hi));
            while if explicit { cur < end } else { cur < end } {
                grid.push(Bson::DateTime(DateTime::from_millis(cur)));
                let next = add_unit(cur, u, st);
                if next <= cur {
                    break;
                }
                cur = next;
            }
        } else {
            let (mut cur, end) = (as_f64(&lo), as_f64(&hi));
            let int_mode = matches!(lo, Bson::Int32(_) | Bson::Int64(_)) && matches!(step, Bson::Int32(_) | Bson::Int64(_));
            while cur < end {
                grid.push(if int_mode { Bson::Int32(cur as i32) } else { Bson::Double(cur) });
                cur += step_f;
            }
        }
        // merge: existing documents and the grid points no document has
        let mut gi = 0;
        let mut emit_grid_upto = |limit: Option<&Bson>, out: &mut Vec<Document>, gi: &mut usize, inclusive_skip: Option<&Bson>| {
            while *gi < grid.len() {
                let g = &grid[*gi];
                if let Some(l) = limit {
                    if compare(g, l) != Ordering::Less {
                        break;
                    }
                }
                let _ = inclusive_skip;
                let exists = group.iter().any(|(_, v, _)| equal(v, g));
                if !exists {
                    let mut nd = Document::new();
                    for (p, v) in parts.iter().zip(&key) {
                        let _ = set_path(&mut nd, p, v.clone());
                    }
                    let gv = match (&group[0].1, g) {
                        (Bson::Int64(_), Bson::Int32(x)) => Bson::Int64(*x as i64),
                        (Bson::Int32(_), _) | (Bson::Int64(_), _) | (Bson::Double(_), _) | (Bson::Decimal128(_), _) | (Bson::DateTime(_), _) => g.clone(),
                        _ => g.clone(),
                    };
                    let _ = set_path(&mut nd, &field, gv);
                    out.push(nd);
                }
                *gi += 1;
            }
        };
        for (_, v, d) in group {
            emit_grid_upto(Some(v), &mut out, &mut gi, None);
            out.push(d.clone());
        }
        emit_grid_upto(None, &mut out, &mut gi, None);
        i = j;
    }
    Ok(out)
}

// ---------------------------------------------------------------------------------------------
// $fill
// ---------------------------------------------------------------------------------------------

fn nullish(v: &Option<Bson>) -> bool {
    matches!(v, None | Some(Bson::Null) | Some(Bson::Undefined))
}

fn partition_key(d: &Document, by: &Option<Bson>, fields: &[String]) -> R<Vec<u8>> {
    let mut key = Vec::new();
    if let Some(e) = by {
        key.extend(crate::layout::encode_group_key(&ev(e, d)?.unwrap_or(Bson::Null)));
    }
    for f in fields {
        key.extend(crate::layout::encode_group_key(&get_path(d, f).unwrap_or(Bson::Null)));
    }
    Ok(key)
}

pub fn fill(arg: &Bson, docs: Vec<Document>) -> R<Vec<Document>> {
    let spec = arg.as_document().ok_or_else(|| err(40196, "the $fill stage specification must be an object"))?;
    for k in spec.keys() {
        if !matches!(k.as_str(), "partitionBy" | "partitionByFields" | "sortBy" | "output") {
            return Err(err(40415, format!("BSON field '$fill.{k}' is an unknown field.")));
        }
    }
    let output = match spec.get("output") {
        Some(Bson::Document(o)) if !o.is_empty() => o.clone(),
        Some(Bson::Document(_)) => return Err(err(6050203, "The output field in '$fill' must contain an object with at least one element")),
        Some(_) => return Err(err(14, "BSON field '$fill.output' is the wrong type")),
        None => return Err(err(40414, "BSON field '$fill.output' is missing but a required field")),
    };
    if spec.contains_key("partitionBy") && spec.contains_key("partitionByFields") {
        return Err(err(6050204, "Specify either partitionBy or partitionByFields, not both"));
    }
    let part_by = spec.get("partitionBy").cloned();
    let part_fields: Vec<String> = match spec.get("partitionByFields") {
        Some(Bson::Array(a)) => a.iter().filter_map(|x| x.as_str().map(str::to_string)).collect(),
        _ => vec![],
    };
    let sort = match spec.get("sortBy") {
        Some(Bson::Document(s)) => Some(crate::query::parse_sort(s)?),
        Some(_) => return Err(err(14, "sortBy must be an object")),
        None => None,
    };
    let mut methods: Vec<(String, Option<String>, Option<Bson>)> = Vec::new();
    for (f, v) in &output {
        let Bson::Document(m) = v else { return Err(err(14, format!("The fill output specification for '{f}' must be an object"))) };
        match (m.get("method"), m.get("value")) {
            (Some(_), Some(_)) => return Err(err(6050201, "Each fill output specification must have exactly one of 'method' or 'value' fields, not both")),
            (None, None) => {}
            (Some(Bson::String(mt)), None) if mt == "locf" || mt == "linear" => methods.push((f.clone(), Some(mt.clone()), None)),
            (Some(_), None) => return Err(err(6050202, "Method must be either locf or linear")),
            (None, Some(v)) => methods.push((f.clone(), None, Some(v.clone()))),
        }
    }
    if methods.iter().any(|(_, m, _)| m.as_deref() == Some("linear")) && sort.as_ref().map(|s| s.len() != 1).unwrap_or(true) {
        return Err(err(605001, "linear fill requires a single-field sortBy"));
    }
    // order: by partition, then sortBy
    let mut keyed: Vec<(Vec<u8>, Document)> = Vec::with_capacity(docs.len());
    for d in docs {
        keyed.push((partition_key(&d, &part_by, &part_fields)?, d));
    }
    if part_by.is_some() || !part_fields.is_empty() {
        let mut order: Vec<Vec<u8>> = Vec::new();
        for (k, _) in &keyed {
            if !order.contains(k) {
                order.push(k.clone());
            }
        }
        let mut grouped: Vec<(Vec<u8>, Document)> = Vec::new();
        for k in &order {
            let mut members: Vec<Document> = keyed.iter().filter(|(kk, _)| kk == k).map(|(_, d)| d.clone()).collect();
            if let Some(s) = &sort {
                crate::query::sort_docs(&mut members, s);
            }
            grouped.extend(members.into_iter().map(|d| (k.clone(), d)));
        }
        keyed = grouped;
    } else if let Some(s) = &sort {
        let mut all: Vec<Document> = keyed.into_iter().map(|(_, d)| d).collect();
        crate::query::sort_docs(&mut all, s);
        keyed = all.into_iter().map(|d| (Vec::new(), d)).collect();
    }
    let n = keyed.len();
    for (field, method, value) in &methods {
        let mut i = 0;
        while i < n {
            let mut j = i;
            while j < n && keyed[j].0 == keyed[i].0 {
                j += 1;
            }
            match method.as_deref() {
                None => {
                    let e = value.as_ref().unwrap();
                    for k in i..j {
                        if nullish(&get_path(&keyed[k].1, field)) {
                            let v = ev(e, &keyed[k].1)?.unwrap_or(Bson::Null);
                            set_path(&mut keyed[k].1, field, v)?;
                        }
                    }
                }
                Some("locf") => {
                    let mut last: Option<Bson> = None;
                    for k in i..j {
                        let cur = get_path(&keyed[k].1, field);
                        if nullish(&cur) {
                            if let Some(l) = &last {
                                set_path(&mut keyed[k].1, field, l.clone())?;
                            } else if cur.is_none() {
                                set_path(&mut keyed[k].1, field, Bson::Null)?;
                            }
                        } else {
                            last = cur;
                        }
                    }
                }
                _ => {
                    let sf = sort.as_ref().unwrap()[0].0.clone();
                    let xs: Vec<Option<f64>> = (i..j).map(|k| get_path(&keyed[k].1, &sf).filter(|v| is_numeric(v) || matches!(v, Bson::DateTime(_))).map(|v| if let Bson::DateTime(d) = v { d.timestamp_millis() as f64 } else { as_f64(&v) })).collect();
                    let known: Vec<usize> = (i..j).filter(|k| !nullish(&get_path(&keyed[*k].1, field))).collect();
                    for k in i..j {
                        if !nullish(&get_path(&keyed[k].1, field)) {
                            continue;
                        }
                        let prev = known.iter().rev().find(|p| **p < k);
                        let next = known.iter().find(|p| **p > k);
                        if let (Some(p), Some(nx)) = (prev, next) {
                            let (x0, x1, xk) = (xs[p - i], xs[nx - i], xs[k - i]);
                            let (y0, y1) = (get_path(&keyed[*p].1, field).map(|v| as_f64(&v)), get_path(&keyed[*nx].1, field).map(|v| as_f64(&v)));
                            if let (Some(x0), Some(x1), Some(xk), Some(y0), Some(y1)) = (x0, x1, xk, y0, y1) {
                                let y = y0 + (y1 - y0) * (xk - x0) / (x1 - x0);
                                set_path(&mut keyed[k].1, field, Bson::Double(y))?;
                                continue;
                            }
                        }
                        if get_path(&keyed[k].1, field).is_none() {
                            set_path(&mut keyed[k].1, field, Bson::Null)?;
                        }
                    }
                }
            }
            i = j;
        }
    }
    Ok(keyed.into_iter().map(|(_, d)| d).collect())
}

// ---------------------------------------------------------------------------------------------
// $setWindowFields
// ---------------------------------------------------------------------------------------------

#[derive(Clone)]
enum Bound {
    Unbounded,
    Current,
    Offset(f64),
}

fn parse_bound(b: &Bson) -> R<Bound> {
    match b {
        Bson::String(s) if s == "unbounded" => Ok(Bound::Unbounded),
        Bson::String(s) if s == "current" => Ok(Bound::Current),
        x if is_numeric(x) => Ok(Bound::Offset(as_f64(x))),
        _ => Err(err(9, "a window bound must be 'unbounded', 'current' or a number")),
    }
}

pub fn set_window_fields(arg: &Bson, docs: Vec<Document>, foreign: &Foreign) -> R<Vec<Document>> {
    let spec = arg.as_document().ok_or_else(|| err(40196, "the $setWindowFields stage specification must be an object"))?;
    for k in spec.keys() {
        if !matches!(k.as_str(), "partitionBy" | "sortBy" | "output") {
            return Err(err(40415, format!("BSON field '$setWindowFields.{k}' is an unknown field.")));
        }
    }
    let output = match spec.get("output") {
        Some(Bson::Document(o)) => o.clone(),
        Some(_) => return Err(err(14, "BSON field '$setWindowFields.output' is the wrong type")),
        None => return Err(err(40414, "BSON field '$setWindowFields.output' is missing but a required field")),
    };
    let part_by = spec.get("partitionBy").cloned();
    let sort = match spec.get("sortBy") {
        Some(Bson::Document(s)) => Some(crate::query::parse_sort(s)?),
        Some(_) => return Err(err(14, "sortBy must be an object")),
        None => None,
    };
    // partitions in order of first appearance; inside a partition ordered by sortBy
    let mut order: Vec<Vec<u8>> = Vec::new();
    let mut parts: Vec<Vec<Document>> = Vec::new();
    for d in docs {
        let k = match &part_by {
            Some(e) => crate::layout::encode_group_key(&ev(e, &d)?.unwrap_or(Bson::Null)),
            None => Vec::new(),
        };
        match order.iter().position(|x| *x == k) {
            Some(i) => parts[i].push(d),
            None => {
                order.push(k);
                parts.push(vec![d]);
            }
        }
    }
    let mut out = Vec::new();
    let mut idx: Vec<usize> = (0..parts.len()).collect();
    idx.sort_by(|a, b| order[*a].cmp(&order[*b]));
    let mut parts: Vec<Option<Vec<Document>>> = parts.into_iter().map(Some).collect();
    for pi in idx {
        let mut part = parts[pi].take().unwrap();
        if let Some(s) = &sort {
            crate::query::sort_docs(&mut part, s);
        }
        let n = part.len();
        let mut results: Vec<Document> = part.clone();
        for (field, fspec) in &output {
            let Bson::Document(fd) = fspec else { return Err(err(40415, format!("the output spec of '{field}' must be an object"))) };
            let window = fd.get("window").cloned();
            let (op, opv) = fd.iter().find(|(k, _)| k.as_str() != "window").ok_or_else(|| err(40415, "a window output needs an operator"))?;
            let window_kind = match &window {
                None => None,
                Some(Bson::Document(w)) => {
                    let (kind, b) = w.iter().next().ok_or_else(|| err(9, "empty window"))?;
                    let Bson::Array(a) = b else { return Err(err(9, "window bounds must be an array")) };
                    if a.len() != 2 {
                        return Err(err(9, "window bounds must have two elements"));
                    }
                    Some((kind.clone(), parse_bound(&a[0])?, parse_bound(&a[1])?, w.get_str("unit").ok().map(str::to_string)))
                }
                Some(_) => return Err(err(9, "window must be an object")),
            };
            for i in 0..n {
                let (lo, hi) = window_range(&part, i, &window_kind, &sort)?;
                let members: Vec<&Document> = part[lo..hi.max(lo)].iter().collect();
                let v = match op.as_str() {
                    "$documentNumber" => Bson::Int32(i as i32 + 1),
                    "$rank" | "$denseRank" => {
                        let sk = sort.as_ref().ok_or_else(|| err(5371602, format!("{op} requires a sortBy")))?;
                        let key = |d: &Document| -> Vec<Option<Bson>> { sk.iter().map(|(f, _)| get_path(d, f)).collect() };
                        let mut rank = 1i64;
                        let mut dense = 1i64;
                        for k in 1..=i {
                            if key(&part[k]).iter().zip(key(&part[k - 1]).iter()).any(|(a, b)| match (a, b) { (Some(a), Some(b)) => !crate::query::equal(a, b), (None, None) => false, _ => true }) {
                                rank = k as i64 + 1;
                                dense += 1;
                            }
                        }
                        Bson::Int32(if op == "$rank" { rank as i32 } else { dense as i32 })
                    }
                    "$shift" => {
                        let sd = opv.as_document().ok_or_else(|| err(5371601, "$shift requires an object"))?;
                        let by = sd.get("by").and_then(num_i64).ok_or_else(|| err(5371601, "$shift requires an integer 'by'"))?;
                        let idx = i as i64 + by;
                        if idx >= 0 && (idx as usize) < n {
                            ev(sd.get("output").unwrap_or(&Bson::Null), &part[idx as usize])?.unwrap_or(Bson::Null)
                        } else {
                            match sd.get("default") {
                                Some(d) => ev(d, &part[i])?.unwrap_or(Bson::Null),
                                None => Bson::Null,
                            }
                        }
                    }
                    "$locf" => {
                        let mut v = ev(opv, &part[i])?;
                        let mut k = i;
                        while nullish(&v) && k > 0 {
                            k -= 1;
                            v = ev(opv, &part[k])?;
                        }
                        v.unwrap_or(Bson::Null)
                    }
                    "$expMovingAvg" => {
                        let sd = opv.as_document().ok_or_else(|| err(5433604, "$expMovingAvg requires an object"))?;
                        let input = sd.get("input").cloned().unwrap_or(Bson::Null);
                        let alpha = match (sd.get("N"), sd.get("alpha")) {
                            (Some(nv), None) => 2.0 / (as_f64(nv) + 1.0),
                            (None, Some(a)) => as_f64(a),
                            _ => return Err(err(5433604, "$expMovingAvg requires exactly one of 'N' or 'alpha'")),
                        };
                        let mut acc: Option<f64> = None;
                        for k in 0..=i {
                            if let Some(x) = ev(&input, &part[k])?.filter(is_numeric) {
                                let x = as_f64(&x);
                                acc = Some(match acc {
                                    None => x,
                                    Some(prev) => alpha * x + (1.0 - alpha) * prev,
                                });
                            }
                        }
                        acc.map(Bson::Double).unwrap_or(Bson::Null)
                    }
                    "$derivative" | "$integral" => {
                        let sd = opv.as_document().ok_or_else(|| err(5624900, format!("{op} requires an object")))?;
                        let input = sd.get("input").cloned().unwrap_or(Bson::Null);
                        let sf = sort.as_ref().and_then(|s| s.first()).map(|s| s.0.clone()).ok_or_else(|| err(5624900, format!("{op} requires a sortBy")))?;
                        let pts: Vec<(f64, f64)> = members
                            .iter()
                            .filter_map(|m| {
                                let x = get_path(m, &sf)?;
                                let x = if let Bson::DateTime(d) = x { d.timestamp_millis() as f64 } else { as_f64(&x) };
                                let y = ev(&input, m).ok().flatten().filter(is_numeric).map(|v| as_f64(&v))?;
                                Some((x, y))
                            })
                            .collect();
                        let unit_ms = match sd.get_str("unit").ok() {
                            Some(u) => Some(match parse_unit(u)? {
                                Unit::Ms => 1.0,
                                Unit::Second => 1000.0,
                                Unit::Minute => 60_000.0,
                                Unit::Hour => 3_600_000.0,
                                Unit::Day => 86_400_000.0,
                                Unit::Week => 604_800_000.0,
                                _ => return Err(err(5624900, "unsupported unit")),
                            }),
                            None => None,
                        };
                        if op == "$derivative" {
                            match (pts.first(), pts.last()) {
                                (Some(a), Some(b)) if pts.len() >= 2 && b.0 != a.0 => {
                                    let dx = (b.0 - a.0) / unit_ms.unwrap_or(1.0);
                                    Bson::Double((b.1 - a.1) / dx)
                                }
                                _ => Bson::Null,
                            }
                        } else {
                            let mut s = 0.0;
                            for w in pts.windows(2) {
                                s += (w[1].0 - w[0].0) / unit_ms.unwrap_or(1.0) * (w[0].1 + w[1].1) / 2.0;
                            }
                            if pts.len() < 2 { Bson::Int32(0) } else { Bson::Double(s) }
                        }
                    }
                    "$linearFill" => {
                        let cur = ev(opv, &part[i])?;
                        if !nullish(&cur) {
                            cur.unwrap()
                        } else {
                            let sf = sort.as_ref().and_then(|s| s.first()).map(|s| s.0.clone()).ok_or_else(|| err(5371601, "$linearFill requires a sortBy"))?;
                            let xof = |d: &Document| get_path(d, &sf).map(|v| if let Bson::DateTime(t) = v { t.timestamp_millis() as f64 } else { as_f64(&v) });
                            let prev = (0..i).rev().find_map(|k| ev(opv, &part[k]).ok().flatten().filter(|v| !matches!(v, Bson::Null)).map(|v| (k, v)));
                            let next = (i + 1..n).find_map(|k| ev(opv, &part[k]).ok().flatten().filter(|v| !matches!(v, Bson::Null)).map(|v| (k, v)));
                            match (prev, next, xof(&part[i])) {
                                (Some((p, pv)), Some((q, qv)), Some(xk)) => {
                                    let (x0, x1) = (xof(&part[p]).unwrap_or(0.0), xof(&part[q]).unwrap_or(1.0));
                                    Bson::Double(as_f64(&pv) + (as_f64(&qv) - as_f64(&pv)) * (xk - x0) / (x1 - x0))
                                }
                                _ => Bson::Null,
                            }
                        }
                    }
                    "$stdDevPop" if members.len() == 1 => Bson::Int32(0),
                    other => {
                        let owned: Vec<Document> = members.iter().map(|d| (*d).clone()).collect();
                        let r = accumulate(other, opv, &owned).map_err(|e| if e.code == 15952 { err(9, format!("Unrecognized window function, {other}")) } else { e })?;
                        match (other, r) {
                            ("$addToSet", Bson::Array(mut a)) => {
                                a.sort_by(compare);
                                Bson::Array(a)
                            }
                            (_, r) => r,
                        }
                    }
                };
                set_path(&mut results[i], field, v)?;
            }
        }
        out.extend(results);
    }
    let _ = foreign;
    Ok(out)
}

/// The `[lo, hi)` document range of the window of document `i`.
fn window_range(part: &[Document], i: usize, w: &Option<(String, Bound, Bound, Option<String>)>, sort: &Option<Vec<(String, bool)>>) -> R<(usize, usize)> {
    let n = part.len();
    let Some((kind, lo, hi, unit)) = w else { return Ok((0, n)) };
    match kind.as_str() {
        "documents" => {
            let at = |b: &Bound, is_hi: bool| -> usize {
                match b {
                    Bound::Unbounded => {
                        if is_hi { n } else { 0 }
                    }
                    Bound::Current => if is_hi { i + 1 } else { i },
                    Bound::Offset(o) => {
                        let v = i as i64 + *o as i64 + if is_hi { 1 } else { 0 };
                        v.clamp(0, n as i64) as usize
                    }
                }
            };
            Ok((at(lo, false), at(hi, true)))
        }
        "range" => {
            let sf = sort.as_ref().and_then(|s| s.first()).map(|s| s.0.clone()).ok_or_else(|| err(5339902, "a range window requires a sortBy with one field"))?;
            let xs: Vec<Option<f64>> = part.iter().map(|d| get_path(d, &sf).map(|v| if let Bson::DateTime(t) = v { t.timestamp_millis() as f64 } else { as_f64(&v) })).collect();
            let scale = match unit.as_deref() {
                None => 1.0,
                Some(u) => match parse_unit(u)? {
                    Unit::Ms => 1.0,
                    Unit::Second => 1000.0,
                    Unit::Minute => 60_000.0,
                    Unit::Hour => 3_600_000.0,
                    Unit::Day => 86_400_000.0,
                    Unit::Week => 604_800_000.0,
                    _ => return Err(err(5339902, "unsupported unit for a range window")),
                },
            };
            let cur = xs[i].unwrap_or(0.0);
            let lo_v = match lo {
                Bound::Unbounded => f64::NEG_INFINITY,
                Bound::Current => cur,
                Bound::Offset(o) => cur + o * scale,
            };
            let hi_v = match hi {
                Bound::Unbounded => f64::INFINITY,
                Bound::Current => cur,
                Bound::Offset(o) => cur + o * scale,
            };
            let start = xs.iter().position(|x| x.map(|x| x >= lo_v).unwrap_or(false)).unwrap_or(n);
            let end = xs.iter().rposition(|x| x.map(|x| x <= hi_v).unwrap_or(false)).map(|p| p + 1).unwrap_or(0);
            Ok((start, end))
        }
        _ => Err(err(5339900, "a window needs 'documents' or 'range'")),
    }
}

// ---------------------------------------------------------------------------------------------
// $graphLookup, $redact
// ---------------------------------------------------------------------------------------------

pub fn graph_lookup(arg: &Bson, docs: Vec<Document>, foreign: &Foreign) -> R<Vec<Document>> {
    let spec = arg.as_document().ok_or_else(|| err(9, "the $graphLookup stage specification must be an object"))?;
    let mut from = None;
    let (mut start, mut cf, mut ct, mut as_) = (None, None, None, None);
    let mut max_depth: Option<i64> = None;
    let mut depth_field: Option<String> = None;
    let mut restrict: Option<Document> = None;
    for (k, v) in spec {
        match k.as_str() {
            "from" => from = v.as_str().map(str::to_string),
            "startWith" => start = Some(v.clone()),
            "connectFromField" => cf = v.as_str().map(str::to_string),
            "connectToField" => ct = v.as_str().map(str::to_string),
            "as" => as_ = v.as_str().map(str::to_string),
            "maxDepth" => max_depth = Some(num_i64(v).filter(|n| *n >= 0).ok_or_else(|| err(40101, "maxDepth must be a non-negative integer"))?),
            "depthField" => depth_field = v.as_str().map(str::to_string),
            "restrictSearchWithMatch" => restrict = v.as_document().cloned(),
            other => return Err(err(40104, format!("Unknown argument to $graphLookup: {other}"))),
        }
    }
    let (Some(from), Some(start), Some(cf), Some(ct), Some(as_)) = (from, start, cf, ct, as_) else {
        return Err(err(9, "$graphLookup requires 'from', 'startWith', 'connectFromField', 'connectToField' and 'as'"));
    };
    let empty = Vec::new();
    let fdocs = foreign.get(&from).unwrap_or(&empty);
    let mut out = Vec::with_capacity(docs.len());
    for mut d in docs {
        let sv = ev(&start, &d)?;
        let mut frontier: Vec<Bson> = match sv {
            Some(Bson::Array(a)) => a,
            Some(v) => vec![v],
            None => vec![],
        };
        let mut found: Vec<(usize, i64)> = Vec::new();
        let mut depth = 0i64;
        while !frontier.is_empty() && max_depth.map(|m| depth <= m).unwrap_or(true) {
            let mut next: Vec<Bson> = Vec::new();
            for (idx, fd) in fdocs.iter().enumerate() {
                if found.iter().any(|(i, _)| *i == idx) {
                    continue;
                }
                let targets: Vec<Bson> = match get_path(fd, &ct) {
                    Some(Bson::Array(a)) => a,
                    Some(v) => vec![v],
                    None => continue,
                };
                if !targets.iter().any(|t| frontier.iter().any(|f| equal(f, t))) {
                    continue;
                }
                if let Some(r) = &restrict {
                    if !crate::query::matches(fd, r)? {
                        continue;
                    }
                }
                found.push((idx, depth));
                match get_path(fd, &cf) {
                    Some(Bson::Array(a)) => next.extend(a),
                    Some(v) => next.push(v),
                    None => {}
                }
            }
            frontier = next;
            depth += 1;
        }
        let mut arr = Vec::new();
        for (idx, dep) in found {
            let mut fd = fdocs[idx].clone();
            if let Some(df) = &depth_field {
                set_path(&mut fd, df, Bson::Int64(dep))?;
            }
            arr.push(Bson::Document(fd));
        }
        set_path(&mut d, &as_, Bson::Array(arr))?;
        out.push(d);
    }
    Ok(out)
}

pub fn redact(arg: &Bson, docs: Vec<Document>) -> R<Vec<Document>> {
    fn walk(d: &Document, expr: &Bson) -> R<Option<Document>> {
        let verdict = crate::expr::eval(expr, &crate::expr::Scope::new(d))?;
        match verdict {
            Some(Bson::String(s)) if s == "keep" => Ok(Some(d.clone())),
            Some(Bson::String(s)) if s == "prune" => Ok(None),
            Some(Bson::String(s)) if s == "descend" => {
                let mut out = Document::new();
                for (k, v) in d {
                    match v {
                        Bson::Document(sub) => {
                            if let Some(x) = walk(sub, expr)? {
                                out.insert(k.clone(), Bson::Document(x));
                            }
                        }
                        Bson::Array(a) => {
                            let mut items = Vec::new();
                            for e in a {
                                match e {
                                    Bson::Document(sub) => {
                                        if let Some(x) = walk(sub, expr)? {
                                            items.push(Bson::Document(x));
                                        }
                                    }
                                    other => items.push(other.clone()),
                                }
                            }
                            out.insert(k.clone(), Bson::Array(items));
                        }
                        other => {
                            out.insert(k.clone(), other.clone());
                        }
                    }
                }
                Ok(Some(out))
            }
            _ => Err(err(17053, "$redact's expression should not return anything aside from the variables $$KEEP, $$DESCEND, and $$PRUNE, but returned a different value")),
        }
    }
    let mut out = Vec::new();
    for d in docs {
        if let Some(x) = walk(&d, arg)? {
            out.push(x);
        }
    }
    Ok(out)
}
