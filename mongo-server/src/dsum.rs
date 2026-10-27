//! `$sum` / `$avg` accumulation as MongoDB does it: integers stay exact (int -> long -> double on overflow) and doubles are summed
//! with Kahan's compensated summation, so the total does not depend on the rounding of every partial sum.

use bson::Bson;

#[derive(Clone, Debug)]
pub struct DSum {
    /// 0: only int32 so far, 1: an int64 was seen, 2: a double was seen or an integer sum overflowed
    kind: u8,
    int: i64,
    hi: f64,
    lo: f64,
}

impl DSum {
    pub fn new() -> Self {
        DSum { kind: 0, int: 0, hi: 0.0, lo: 0.0 }
    }

    fn add_double(&mut self, x: f64) {
        // Kahan summation, as mongod's $sum / $avg do (measured against mongod 7.0.45)
        if !x.is_finite() || !self.hi.is_finite() {
            self.hi += x;
            return;
        }
        let y = x - self.lo;
        let t = self.hi + y;
        self.lo = (t - self.hi) - y;
        self.hi = t;
    }

    fn to_double(&mut self) {
        if self.kind < 2 {
            self.hi = self.int as f64;
            self.lo = 0.0;
            self.kind = 2;
        }
    }

    pub fn add(&mut self, v: &Bson) {
        match v {
            Bson::Int32(i) => self.add_int(*i as i64, false),
            Bson::Int64(i) => self.add_int(*i, true),
            other => {
                let x = crate::query::as_f64(other);
                self.to_double();
                self.add_double(x);
            }
        }
    }

    fn add_int(&mut self, i: i64, long: bool) {
        if self.kind == 2 {
            self.add_double(i as f64);
            return;
        }
        match self.int.checked_add(i) {
            Some(s) => {
                self.int = s;
                if long {
                    self.kind = self.kind.max(1);
                } else if self.kind == 0 && i32::try_from(s).is_err() {
                    self.kind = 1; // an int32 sum that no longer fits is a long
                }
            }
            None => {
                self.to_double();
                self.add_double(i as f64);
            }
        }
    }

    /// The double total (for `$avg`).
    pub fn total(&self) -> f64 {
        match self.kind {
            2 => self.hi,
            _ => self.int as f64,
        }
    }

    pub fn value(&self) -> Bson {
        match self.kind {
            0 => Bson::Int32(self.int as i32),
            1 => Bson::Int64(self.int),
            _ => Bson::Double(self.hi),
        }
    }
}
