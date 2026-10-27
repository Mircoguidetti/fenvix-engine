//! Trigonometric, bitwise and standard-deviation expression operators: `$sin` .. `$tanh`, `$degreesToRadians`,
//! `$radiansToDegrees`, `$bitAnd` / `$bitOr` / `$bitXor` / `$bitNot`, `$stdDevPop` / `$stdDevSamp` as expressions.
//! (Decimal128 operands are computed in double precision; mongod computes them with 34 digits.)

use super::*;

type Out = R<Option<Option<Bson>>>;

fn some(v: Bson) -> Out {
    Ok(Some(Some(v)))
}

fn null() -> Out {
    some(Bson::Null)
}

fn as_f(v: &Bson) -> Option<f64> {
    match v {
        Bson::Int32(i) => Some(*i as f64),
        Bson::Int64(i) => Some(*i as f64),
        Bson::Double(f) => Some(*f),
        Bson::Decimal128(_) => Some(crate::query::as_f64(v)),
        _ => None,
    }
}

fn fmt_num(x: f64) -> String {
    if x.is_infinite() {
        return if x > 0.0 { "inf".into() } else { "-inf".into() };
    }
    if x.fract() == 0.0 && x.abs() < 1e15 {
        format!("{}", x as i64)
    } else {
        format!("{x}")
    }
}

pub(super) fn operator(name: &str, arg: &Bson, sc: &Scope) -> Out {
    match name {
        "$sin" | "$cos" | "$tan" | "$asin" | "$acos" | "$atan" | "$sinh" | "$cosh" | "$tanh" | "$asinh" | "$acosh" | "$atanh" | "$degreesToRadians" | "$radiansToDegrees" => unary(name, arg, sc),
        "$atan2" => {
            let a = exact_args(name, arg, 2)?;
            let (y, x) = (ev(a[0], sc)?, ev(a[1], sc)?);
            if is_nullish(&y) || is_nullish(&x) {
                return null();
            }
            let f = |v: &Option<Bson>| v.as_ref().and_then(as_f).ok_or_else(|| err(51044, format!("{name} only supports numeric types, not {}", v.as_ref().map(type_name).unwrap_or("missing"))));
            some(Bson::Double(f(&y)?.atan2(f(&x)?)))
        }
        "$bitAnd" | "$bitOr" | "$bitXor" => bit_binary(name, arg, sc),
        "$bitNot" => {
            let a = exact_args(name, arg, 1)?;
            match ev(a[0], sc)? {
                v if is_nullish(&v) => null(),
                Some(Bson::Int32(i)) => some(Bson::Int32(!i)),
                Some(Bson::Int64(i)) => some(Bson::Int64(!i)),
                Some(o @ (Bson::Double(_) | Bson::Decimal128(_))) => Err(MongoError::new(14, "TypeMismatch", format!("$bitNot only supports int and long, not: {}.", type_name(&o)))),
                Some(o) => Err(err(28765, format!("$bitNot only supports numeric types, not {}", type_name(&o)))),
                None => null(),
            }
        }
        "$stdDevPop" | "$stdDevSamp" => {
            let v = ev(arg, sc)?;
            let items: Vec<f64> = match &v {
                Some(Bson::Array(a)) => a.iter().filter_map(as_f).collect(),
                Some(x) => as_f(x).into_iter().collect(),
                None => vec![],
            };
            if items.is_empty() || (name == "$stdDevSamp" && items.len() < 2) {
                return null();
            }
            let (mut n, mut mean, mut m2) = (0.0f64, 0.0f64, 0.0f64);
            for x in &items {
                n += 1.0;
                let delta = x - mean;
                mean += delta / n;
                m2 += delta * (x - mean);
            }
            some(Bson::Double(if name == "$stdDevPop" { (m2 / n).sqrt() } else { (m2 / (n - 1.0)).sqrt() }))
        }
        _ => Ok(None),
    }
}

fn unary(name: &str, arg: &Bson, sc: &Scope) -> Out {
    let a = exact_args(name, arg, 1)?;
    let v = ev(a[0], sc)?;
    if is_nullish(&v) {
        return null();
    }
    let v = v.unwrap();
    let Some(x) = as_f(&v) else {
        return Err(err(28765, format!("{name} only supports numeric types, not {}", type_name(&v))));
    };
    let bad = |range: &str| Err(err(50989, format!("cannot apply {name} to {}, value must be in {range}", fmt_num(x))));
    let r = match name {
        "$sin" | "$cos" | "$tan" if x.is_infinite() => return bad("(-inf,inf)"),
        "$sin" => x.sin(),
        "$cos" => x.cos(),
        "$tan" => x.tan(),
        "$asin" | "$acos" | "$atanh" if !x.is_nan() && x.abs() > 1.0 => return bad("[-1,1]"),
        "$asin" => x.asin(),
        "$acos" => x.acos(),
        "$atanh" => x.atanh(),
        "$acosh" if !x.is_nan() && x < 1.0 => return bad("[1,inf]"),
        "$acosh" => x.acosh(),
        "$atan" => x.atan(),
        "$sinh" => x.sinh(),
        "$cosh" => x.cosh(),
        "$tanh" => x.tanh(),
        "$asinh" => x.asinh(),
        "$degreesToRadians" => x * (std::f64::consts::PI / 180.0),
        _ => x * (180.0 / std::f64::consts::PI),
    };
    if matches!(v, Bson::Decimal128(_)) && r.is_finite() {
        if let Ok(d) = format!("{r:e}").parse::<bson::Decimal128>() {
            return some(Bson::Decimal128(d));
        }
    }
    some(Bson::Double(r))
}

fn bit_binary(name: &str, arg: &Bson, sc: &Scope) -> Out {
    let a = args(arg);
    let mut acc: i64 = match name {
        "$bitAnd" => -1,
        _ => 0,
    };
    let mut long = false;
    let mut is_null = false;
    for x in a {
        match ev(x, sc)? {
            v if is_nullish(&v) => is_null = true,
            Some(Bson::Int32(i)) => acc = apply(name, acc, i as i64),
            Some(Bson::Int64(i)) => {
                long = true;
                acc = apply(name, acc, i);
            }
            Some(_) => return Err(MongoError::new(14, "TypeMismatch", format!("{name} only supports int and long operands."))),
            None => is_null = true,
        }
    }
    if is_null {
        return null();
    }
    some(if long { Bson::Int64(acc) } else { Bson::Int32(acc as i32) })
}

fn apply(name: &str, a: i64, b: i64) -> i64 {
    match name {
        "$bitAnd" => a & b,
        "$bitOr" => a | b,
        _ => a ^ b,
    }
}
