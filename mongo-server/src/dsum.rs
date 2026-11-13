//! `$sum` / `$avg` accumulation as mongod 7.0 does it, so that the answer is mongod's to the last bit.
//!
//! Source read (mongod r7.0.14): `src/mongo/util/summation.h` and `summation.cpp` (`DoubleDoubleSummation`),
//! `src/mongo/db/pipeline/accumulator_sum.cpp` and `accumulator_avg.cpp` (the type promotion and the result), and the
//! SBE copy of the same rules in `src/mongo/db/exec/sbe/vm/arith.cpp` (`aggDoubleDoubleSumImpl`) and `vm.cpp`
//! (`builtinDoubleDoubleSumFinalize`).
//!
//! - Every int, long and double goes into one double-double sum (`_sum`, `_addend`): a Dekker fast-two-sum of the input
//!   with the addend, then a Knuth two-sum with the running sum. A long is added as two doubles (its high and low 32
//!   bits), so long sums stay exact up to 2^106.
//! - The double result is `_sum`, the running high part, NOT the correctly rounded total (`_sum + _addend`): mongod's
//!   answer can differ from the exact sum in the last bit (P-fix-read-amplification: 123 doubles where the exact sum is
//!   78996.43000000001 and mongod answers 78996.43). Before, this file used Kahan's summation, which gives the exact sum
//!   in that case.
//! - The type of the result is the widest input type: int (an int if the rounded sum fits 32 bits, else a long), long,
//!   double, decimal. An int or long sum that does not fit 64 bits is a double. Decimals are summed apart, each addition
//!   rounded to 34 digits; the decimal result adds the double-double sum converted to decimal (each part with 34 digits).
//! - `$avg` divides the double result by the count, or the decimal total by the count when a decimal was seen.

use bigdecimal::BigDecimal;
use bson::Bson;

/// The widest numeric type seen (mongod's `Value::getWidestNumeric`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Ty {
    Int,
    Long,
    Double,
    Decimal,
}

/// A decimal value of the sum: finite, or one of the IEEE special values.
#[derive(Clone, Debug)]
enum Dec {
    Fin(BigDecimal),
    PosInf,
    NegInf,
    NaN,
}

/// Decimal128 has 34 significant digits.
const DEC_DIGITS: u64 = 34;

impl Dec {
    fn zero() -> Dec {
        Dec::Fin(BigDecimal::from(0))
    }

    /// IEEE 754 decimal addition: the exact sum, rounded to 34 digits (half to even) when it needs more.
    fn add(&self, o: &Dec) -> Dec {
        match (self, o) {
            (Dec::NaN, _) | (_, Dec::NaN) => Dec::NaN,
            (Dec::PosInf, Dec::NegInf) | (Dec::NegInf, Dec::PosInf) => Dec::NaN,
            (Dec::PosInf, _) | (_, Dec::PosInf) => Dec::PosInf,
            (Dec::NegInf, _) | (_, Dec::NegInf) => Dec::NegInf,
            (Dec::Fin(a), Dec::Fin(b)) => Dec::Fin(round34(&(a + b))),
        }
    }

    fn of_bson(v: &Bson) -> Dec {
        use std::str::FromStr;
        let Bson::Decimal128(d) = v else { return Dec::NaN };
        let t = d.to_string();
        match t.as_str() {
            "Infinity" => Dec::PosInf,
            "-Infinity" => Dec::NegInf,
            _ if t.ends_with("NaN") => Dec::NaN,
            _ => BigDecimal::from_str(&t).map(Dec::Fin).unwrap_or(Dec::NaN),
        }
    }

    /// A double as mongod converts it here (`Decimal128(double, kRoundTo34Digits)`, Intel's `binary64_to_bid128`): the
    /// double's exact value, rounded to 34 significant digits.
    fn of_f64(f: f64) -> Dec {
        if f.is_nan() {
            return Dec::NaN;
        }
        if f.is_infinite() {
            return if f > 0.0 { Dec::PosInf } else { Dec::NegInf };
        }
        match BigDecimal::try_from(f) {
            Ok(d) => Dec::Fin(round34(&d)),
            Err(_) => Dec::NaN,
        }
    }

    fn div(&self, n: i64) -> Dec {
        match self {
            Dec::Fin(a) => {
                // enough digits that the rounding to 34 is the rounding of the exact quotient (half to even)
                let q = a.with_scale_round(a.fractional_digit_count().max(0) + 80, bigdecimal::RoundingMode::HalfEven) / BigDecimal::from(n);
                Dec::Fin(round34(&q.normalized()))
            }
            other => other.clone(),
        }
    }

    fn to_bson(&self) -> Bson {
        let s = match self {
            Dec::NaN => "NaN".to_string(),
            Dec::PosInf => "Infinity".to_string(),
            Dec::NegInf => "-Infinity".to_string(),
            Dec::Fin(d) => {
                let (int, scale) = d.as_bigint_and_exponent();
                format!("{int}E{}", -scale)
            }
        };
        s.parse::<bson::Decimal128>().map(Bson::Decimal128).unwrap_or(Bson::Double(f64::NAN))
    }
}

/// `d` with at most 34 significant digits (half to even); unchanged when it has fewer.
fn round34(d: &BigDecimal) -> BigDecimal {
    if d.digits() > DEC_DIGITS {
        d.with_precision_round(std::num::NonZeroU64::new(DEC_DIGITS).unwrap(), bigdecimal::RoundingMode::HalfEven)
    } else {
        d.clone()
    }
}

/// mongod's `DoubleDoubleSummation` (`src/mongo/util/summation.h`), operation for operation.
#[derive(Clone, Copy, Debug, Default)]
struct DoubleDouble {
    sum: f64,
    addend: f64,
    /// a plain sum, the answer when the compensated one is NaN (infinities)
    special: f64,
}

/// Dekker's fast two-sum, assuming |b| <= |a|.
#[inline]
fn fast2sum(a: f64, b: f64) -> (f64, f64) {
    let s = a + b;
    let z = s - a;
    let t = b - z;
    (s, t)
}

/// Knuth's two-sum.
#[inline]
fn two_sum(a: f64, b: f64) -> (f64, f64) {
    let s = a + b;
    let a_prime = s - b;
    let b_prime = s - a_prime;
    let delta_a = a - a_prime;
    let delta_b = b - b_prime;
    (s, delta_a + delta_b)
}

impl DoubleDouble {
    fn add_double(&mut self, x: f64) {
        self.special += x;
        let (x, addend) = fast2sum(x, self.addend);
        self.addend = addend;
        let (sum, x) = two_sum(self.sum, x);
        self.sum = sum;
        self.addend += x;
    }

    /// A 64-bit integer as two doubles, so the sum stays exact.
    fn add_long(&mut self, x: i64) {
        let high = x / (1i64 << 32) * (1i64 << 32);
        let low = x - high;
        self.add_double(low as f64);
        self.add_double(high as f64);
    }

    fn get_double(&self) -> f64 {
        if self.sum.is_nan() {
            self.special
        } else {
            self.sum
        }
    }

    fn fits_long(&self) -> bool {
        let (min, max) = (i64::MIN as f64, i64::MAX as f64);
        if self.sum > min && self.sum < max {
            return true;
        }
        if self.sum == max {
            return self.addend < -0.5;
        }
        if self.sum == min {
            return self.addend >= -0.5;
        }
        false
    }

    /// The sum rounded to the nearest integer, halves away from zero (C's `llround`). Only when `fits_long`.
    fn get_long(&self) -> i64 {
        if self.sum == i64::MAX as f64 {
            return (self.addend.round() as i64).wrapping_sub(i64::MIN);
        }
        let sum = self.sum.round() as i64;
        sum.wrapping_add(((self.sum - sum as f64) + self.addend).round() as i64)
    }

    fn get_decimal(&self) -> Dec {
        if !self.sum.is_finite() {
            Dec::of_f64(self.special)
        } else {
            Dec::of_f64(self.sum).add(&Dec::of_f64(self.addend))
        }
    }
}

#[derive(Clone, Debug)]
pub struct DSum {
    ty: Ty,
    nd: DoubleDouble,
    dec: Dec,
}

impl Default for DSum {
    fn default() -> Self {
        Self::new()
    }
}

impl DSum {
    pub fn new() -> Self {
        DSum { ty: Ty::Int, nd: DoubleDouble::default(), dec: Dec::zero() }
    }

    /// One numeric input (the caller skips the others, as mongod does).
    pub fn add(&mut self, v: &Bson) {
        match v {
            Bson::Int32(i) => {
                self.ty = self.ty.max(Ty::Int);
                self.nd.add_double(*i as f64);
            }
            Bson::Int64(i) => {
                self.ty = self.ty.max(Ty::Long);
                self.nd.add_long(*i);
            }
            Bson::Double(x) => {
                self.ty = self.ty.max(Ty::Double);
                self.nd.add_double(*x);
            }
            Bson::Decimal128(_) => {
                self.ty = Ty::Decimal;
                self.dec = self.dec.add(&Dec::of_bson(v));
            }
            other => {
                // not a number: callers filter these out; as a double, as before
                self.ty = self.ty.max(Ty::Double);
                self.nd.add_double(crate::query::as_f64(other));
            }
        }
    }

    /// The `$sum` result (`AccumulatorSum::getValue`).
    pub fn value(&self) -> Bson {
        match self.ty {
            Ty::Int if self.nd.fits_long() => {
                let l = self.nd.get_long();
                match i32::try_from(l) {
                    Ok(i) => Bson::Int32(i),
                    Err(_) => Bson::Int64(l),
                }
            }
            Ty::Int | Ty::Long if self.nd.fits_long() => Bson::Int64(self.nd.get_long()),
            Ty::Int | Ty::Long | Ty::Double => Bson::Double(self.nd.get_double()),
            Ty::Decimal => self.dec.add(&self.nd.get_decimal()).to_bson(),
        }
    }

    /// The `$avg` result over `n` inputs (`AccumulatorAvg::getValue`; null when `n` is 0).
    pub fn avg(&self, n: i64) -> Bson {
        if n == 0 {
            return Bson::Null;
        }
        match self.ty {
            Ty::Decimal => self.dec.add(&self.nd.get_decimal()).div(n).to_bson(),
            _ => Bson::Double(self.nd.get_double() / n as f64),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sum(vals: &[Bson]) -> Bson {
        let mut s = DSum::new();
        for v in vals {
            s.add(v);
        }
        s.value()
    }

    /// The shape of the generator's check 3 (REPORT-2.md section 1b): order totals whose exact sum (and Kahan's) is
    /// 2760.2599999999998 while mongod's double-double `_sum` is 2760.26. Found with a simulation of both algorithms;
    /// mongod 7.0.34 gives 2760.26 for it (ctl-tests/test_85_sum_like_mongod.py compares many such sums with mongod).
    #[test]
    fn high_part_not_the_rounded_total() {
        let vals = [94.82, 942.87, 702.04, 1020.53];
        let mut dd = DoubleDouble::default();
        let (mut hi, mut lo) = (0.0f64, 0.0f64);
        for v in vals {
            dd.add_double(v);
            let y = v - lo;
            let t = hi + y;
            lo = (t - hi) - y;
            hi = t;
        }
        assert_eq!(hi, 2760.2599999999998, "Kahan's sum, the base's answer");
        assert_eq!(dd.get_double(), 2760.26);
        assert_ne!(dd.addend, 0.0);
        assert_eq!(sum(&vals.map(Bson::Double)), Bson::Double(2760.26));
    }

    #[test]
    fn integer_promotion() {
        assert_eq!(sum(&[Bson::Int32(1), Bson::Int32(2)]), Bson::Int32(3));
        assert_eq!(sum(&[Bson::Int32(i32::MAX), Bson::Int32(1)]), Bson::Int64(i32::MAX as i64 + 1));
        assert_eq!(sum(&[Bson::Int32(1), Bson::Int64(2)]), Bson::Int64(3));
        assert_eq!(sum(&[Bson::Int64(i64::MAX), Bson::Int64(-1)]), Bson::Int64(i64::MAX - 1));
        assert_eq!(sum(&[Bson::Int64(i64::MAX), Bson::Int64(1)]), Bson::Double(9223372036854775808.0));
        assert_eq!(sum(&[Bson::Int32(1), Bson::Double(0.5)]), Bson::Double(1.5));
        assert_eq!(sum(&[]), Bson::Int32(0));
        // a long sum stays exact past 2^53
        assert_eq!(sum(&[Bson::Int64(1 << 60), Bson::Int64(1), Bson::Int64(-(1 << 60))]), Bson::Int64(1));
    }

    #[test]
    fn decimals() {
        let d = |s: &str| Bson::Decimal128(s.parse().unwrap());
        assert_eq!(sum(&[d("1.10"), d("2.20")]), d("3.30"));
        assert_eq!(sum(&[d("1"), Bson::Int32(2)]), d("3"));
        let mut s = DSum::new();
        s.add(&d("1"));
        s.add(&d("2"));
        assert_eq!(s.avg(2), d("1.5"));
    }

    #[test]
    fn infinities_and_nan() {
        assert_eq!(sum(&[Bson::Double(f64::INFINITY), Bson::Double(1.0)]), Bson::Double(f64::INFINITY));
        let Bson::Double(x) = sum(&[Bson::Double(f64::INFINITY), Bson::Double(f64::NEG_INFINITY)]) else { panic!() };
        assert!(x.is_nan());
    }
}
