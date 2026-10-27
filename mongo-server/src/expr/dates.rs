//! Date expression operators on chrono / chrono-tz (Olson time zone names and fixed offsets).

use super::*;
use chrono::{Datelike, Duration as CDuration, NaiveDate, NaiveDateTime, Offset, TimeZone, Timelike, Weekday};

type Out = R<Option<Option<Bson>>>;

fn some(v: Bson) -> Out {
    Ok(Some(Some(v)))
}

fn null() -> Out {
    some(Bson::Null)
}

#[derive(Clone, Copy)]
pub(super) enum Tz {
    Utc,
    Fixed(i32),
    Olson(chrono_tz::Tz),
}

pub(super) fn parse_tz(s: &str) -> Option<Tz> {
    if matches!(s, "UTC" | "GMT" | "Etc/UTC" | "Etc/GMT" | "Zulu" | "Etc/Zulu") {
        return Some(Tz::Utc);
    }
    if let Some(m) = tz_offset(s) {
        return Some(Tz::Fixed((m * 60) as i32));
    }
    s.parse::<chrono_tz::Tz>().ok().map(Tz::Olson)
}

fn offset_at(tz: Tz, ms: i64) -> i32 {
    match tz {
        Tz::Utc => 0,
        Tz::Fixed(s) => s,
        Tz::Olson(z) => {
            let secs = ms.div_euclid(1000);
            match z.timestamp_opt(secs, 0).single() {
                Some(dt) => dt.offset().fix().local_minus_utc(),
                None => 0,
            }
        }
    }
}

fn naive(ms: i64) -> Option<NaiveDateTime> {
    chrono::DateTime::from_timestamp_millis(ms).map(|d| d.naive_utc())
}

fn local(ms: i64, tz: Tz) -> R<NaiveDateTime> {
    naive(ms + offset_at(tz, ms) as i64 * 1000).ok_or_else(|| err(9, "date out of range"))
}

fn from_local(l: NaiveDateTime, tz: Tz) -> i64 {
    let as_utc = l.and_utc().timestamp_millis();
    match tz {
        Tz::Utc => as_utc,
        Tz::Fixed(s) => as_utc - s as i64 * 1000,
        Tz::Olson(z) => match z.from_local_datetime(&l).earliest() {
            Some(dt) => dt.timestamp_millis() + l.and_utc().timestamp_subsec_millis() as i64 * 0,
            None => {
                // a local time that does not exist (spring forward): use the offset before the gap
                let off = offset_at(tz, as_utc - 86_400_000);
                as_utc - off as i64 * 1000
            }
        },
    }
}

pub(super) fn iso(ms: i64) -> String {
    match naive(ms) {
        Some(d) => format!("{}.{:03}Z", d.format("%Y-%m-%dT%H:%M:%S"), ms.rem_euclid(1000)),
        None => String::new(),
    }
}

/// Evaluate the optional `timezone` expression. `None`: the date is null (a null timezone nulls the result).
fn tz_of(d: &Document, sc: &Scope, who: &str) -> R<Option<Tz>> {
    match d.get("timezone") {
        None => Ok(Some(Tz::Utc)),
        Some(e) => match ev(e, sc)? {
            None | Some(Bson::Null) => Ok(None),
            Some(Bson::String(s)) => parse_tz(&s).map(Some).ok_or_else(|| err(40485, format!("unrecognized time zone identifier: \"{s}\""))),
            Some(o) => Err(err(40517, format!("timezone must evaluate to a string, found {}", type_name(&o)))),
        },
    }
    .map_err(|e| {
        let _ = who;
        e
    })
}

fn date_value(v: Option<Bson>) -> R<Option<i64>> {
    match v {
        None | Some(Bson::Null) | Some(Bson::Undefined) => Ok(None),
        Some(Bson::DateTime(x)) => Ok(Some(x.timestamp_millis())),
        Some(Bson::Timestamp(_)) => Err(err(241, "can't convert from BSON type timestamp to Date").with_conversion_name()),
        Some(Bson::ObjectId(o)) => Ok(Some(o.timestamp().timestamp_millis())),
        Some(o) => Err(err(16006, format!("can't convert from BSON type {} to Date", type_name(&o)))),
    }
}

/// `date` and `timezone` of an operator given as `{date, timezone}` or as the date expression itself.
fn date_and_tz(name: &str, arg: &Bson, sc: &Scope) -> R<Option<(i64, Tz)>> {
    let (date_e, tz) = match arg {
        Bson::Document(d) if d.contains_key("date") => {
            for k in d.keys() {
                if !matches!(k.as_str(), "date" | "timezone") {
                    return Err(err(40535, format!("unrecognized option to {name}: \"{k}\". Expected arguments are date and timezone.")));
                }
            }
            let Some(tz) = tz_of(d, sc, name)? else { return Ok(None) };
            (d.get("date").unwrap(), tz)
        }
        Bson::Document(d) if !d.keys().next().map(|k| k.starts_with('$')).unwrap_or(false) && !d.is_empty() => {
            return Err(err(40539, format!("missing 'date' argument to {name}, provided: {}", crate::update::fmt_doc(d))));
        }
        Bson::Array(a) if a.len() == 1 => (&a[0], Tz::Utc),
        Bson::Array(a) => return Err(err(16020, format!("Expression {name} takes exactly 1 arguments. {} were passed in.", a.len()))),
        other => (other, Tz::Utc),
    };
    Ok(date_value(ev(date_e, sc)?)?.map(|ms| (ms, tz)))
}

pub(super) fn date_operator(name: &str, arg: &Bson, sc: &Scope) -> Out {
    match name {
        "$year" | "$month" | "$dayOfMonth" | "$hour" | "$minute" | "$second" | "$millisecond" | "$dayOfWeek" | "$dayOfYear" | "$week" | "$isoWeek" | "$isoWeekYear" | "$isoDayOfWeek" => {
            let Some((ms, tz)) = date_and_tz(name, arg, sc)? else { return null() };
            let l = local(ms, tz)?;
            let wd_sun = l.weekday().num_days_from_sunday() as i32;
            some(Bson::Int32(match name {
                "$year" => l.year(),
                "$month" => l.month() as i32,
                "$dayOfMonth" => l.day() as i32,
                "$hour" => l.hour() as i32,
                "$minute" => l.minute() as i32,
                "$second" => l.second() as i32,
                "$millisecond" => ms.rem_euclid(1000) as i32,
                "$dayOfWeek" => wd_sun + 1,
                "$dayOfYear" => l.ordinal() as i32,
                "$week" => ((l.ordinal0() as i32 + 7 - wd_sun) / 7),
                "$isoWeek" => l.iso_week().week() as i32,
                "$isoWeekYear" => return some(Bson::Int64(l.iso_week().year() as i64)),
                _ => l.weekday().number_from_monday() as i32,
            }))
        }
        "$dateToString" => date_to_string(arg, sc),
        "$dateToParts" => date_to_parts(arg, sc),
        "$dateFromParts" => date_from_parts(arg, sc),
        "$dateFromString" => date_from_string(arg, sc),
        "$dateAdd" | "$dateSubtract" => date_add(name, arg, sc),
        "$dateDiff" => date_diff(arg, sc),
        "$dateTrunc" => date_trunc(arg, sc),
        _ => Ok(None),
    }
}

trait ConvName {
    fn with_conversion_name(self) -> Self;
}
impl ConvName for MongoError {
    fn with_conversion_name(mut self) -> Self {
        self.name = "ConversionFailure";
        self
    }
}

fn doc_arg<'a>(name: &str, arg: &'a Bson) -> R<&'a Document> {
    arg.as_document().ok_or_else(|| err(40536, format!("{name} only supports an object as its argument")))
}

pub(super) fn format_date(ms: i64, fmt: &str, tz: Tz) -> R<String> {
    let l = local(ms, tz)?;
    let off = offset_at(tz, ms) / 60;
    let mut out = String::new();
    let mut it = fmt.chars();
    while let Some(c) = it.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        match it.next() {
            Some('Y') => out.push_str(&format!("{:04}", l.year())),
            Some('G') => out.push_str(&format!("{:04}", l.iso_week().year())),
            Some('m') => out.push_str(&format!("{:02}", l.month())),
            Some('d') => out.push_str(&format!("{:02}", l.day())),
            Some('H') => out.push_str(&format!("{:02}", l.hour())),
            Some('M') => out.push_str(&format!("{:02}", l.minute())),
            Some('S') => out.push_str(&format!("{:02}", l.second())),
            Some('L') => out.push_str(&format!("{:03}", ms.rem_euclid(1000))),
            Some('j') => out.push_str(&format!("{:03}", l.ordinal())),
            Some('w') => out.push_str(&(l.weekday().num_days_from_sunday() + 1).to_string()),
            Some('u') => out.push_str(&l.weekday().number_from_monday().to_string()),
            Some('U') => out.push_str(&format!("{:02}", (l.ordinal0() + 7 - l.weekday().num_days_from_sunday()) / 7)),
            Some('V') => out.push_str(&format!("{:02}", l.iso_week().week())),
            Some('Z') => out.push_str(&off.to_string()),
            Some('z') => out.push_str(&format!("{}{:02}{:02}", if off < 0 { '-' } else { '+' }, off.abs() / 60, off.abs() % 60)),
            Some('%') => out.push('%'),
            Some(other) => return Err(err(18536, format!("Invalid format character '%{other}' in format string"))),
            None => return Err(err(18535, "Unmatched '%' at end of format string")),
        }
    }
    Ok(out)
}

fn date_to_string(arg: &Bson, sc: &Scope) -> Out {
    let d = arg.as_document().ok_or_else(|| err(18629, "$dateToString only supports an object as its argument"))?;
    for (k, _) in d {
        if !matches!(k.as_str(), "date" | "format" | "timezone" | "onNull") {
            return Err(err(18534, format!("Unrecognized parameter to $dateToString: {k}")));
        }
    }
    let date = d.get("date").ok_or_else(|| err(18628, "Missing 'date' parameter to $dateToString"))?;
    let fmt = match d.get("format") {
        None => "%Y-%m-%dT%H:%M:%S.%LZ".to_string(),
        Some(f) => match ev(f, sc)? {
            Some(Bson::String(s)) => s,
            None | Some(Bson::Null) => return null(),
            Some(other) => return Err(err(18533, format!("$dateToString requires that 'format' be a string, found: {}", type_name(&other)))),
        },
    };
    let Some(tz) = tz_of(d, sc, "$dateToString")? else { return null() };
    match date_value(ev(date, sc)?)? {
        None => match d.get("onNull") {
            Some(x) => Ok(Some(ev(x, sc)?)),
            None => null(),
        },
        Some(ms) => some(Bson::String(format_date(ms, &fmt, tz)?)),
    }
}

fn int_part(d: &Document, key: &str, sc: &Scope, who: &str, lo: i64, hi: i64) -> R<Option<Option<i64>>> {
    match d.get(key) {
        None => Ok(Some(None)),
        Some(e) => match ev(e, sc)? {
            None | Some(Bson::Null) | Some(Bson::Undefined) => Ok(None),
            Some(v) => match as_num(&v) {
                Some(n) if to_f(n).fract() == 0.0 => {
                    let x = to_f(n) as i64;
                    if x < lo || x > hi {
                        Err(err(40523, format!("'{key}' must evaluate to an integer in the range {lo} to {hi}, found {x}")))
                    } else {
                        Ok(Some(Some(x)))
                    }
                }
                Some(_) => Err(err(40515, format!("'{key}' must evaluate to an integer, found {}", crate::update::fmt_bson(&v)))),
                None => Err(err(40515, format!("{who} requires '{key}' to evaluate to an integer, found {} with value {}", type_name(&v), crate::update::fmt_bson(&v)))),
            },
        },
    }
}

fn date_to_parts(arg: &Bson, sc: &Scope) -> Out {
    let d = doc_arg("$dateToParts", arg)?;
    for k in d.keys() {
        if !matches!(k.as_str(), "date" | "timezone" | "iso8601") {
            return Err(err(40520, format!("Unrecognized argument to $dateToParts: {k}. Expected arguments are date, timezone, and iso8601")));
        }
    }
    let date = d.get("date").ok_or_else(|| err(40522, "Missing 'date' parameter to $dateToParts"))?;
    let Some(tz) = tz_of(d, sc, "$dateToParts")? else { return null() };
    let iso8601 = match d.get("iso8601") {
        None => false,
        Some(e) => match ev(e, sc)? {
            Some(Bson::Boolean(b)) => b,
            None | Some(Bson::Null) => return null(),
            Some(o) => return Err(err(40521, format!("iso8601 must evaluate to a bool, found {}", type_name(&o)))),
        },
    };
    let Some(ms) = date_value(ev(date, sc)?)? else { return null() };
    let l = local(ms, tz)?;
    let mut out = Document::new();
    if iso8601 {
        out.insert("isoWeekYear", l.iso_week().year());
        out.insert("isoWeek", l.iso_week().week() as i32);
        out.insert("isoDayOfWeek", l.weekday().number_from_monday() as i32);
    } else {
        out.insert("year", l.year());
        out.insert("month", l.month() as i32);
        out.insert("day", l.day() as i32);
    }
    out.insert("hour", l.hour() as i32);
    out.insert("minute", l.minute() as i32);
    out.insert("second", l.second() as i32);
    out.insert("millisecond", ms.rem_euclid(1000) as i32);
    some(Bson::Document(out))
}

fn date_from_parts(arg: &Bson, sc: &Scope) -> Out {
    let d = doc_arg("$dateFromParts", arg)?;
    for k in d.keys() {
        if !matches!(k.as_str(), "year" | "month" | "day" | "hour" | "minute" | "second" | "millisecond" | "isoWeekYear" | "isoWeek" | "isoDayOfWeek" | "timezone") {
            return Err(err(40518, format!("Unrecognized argument to $dateFromParts: {k}")));
        }
    }
    let iso = d.contains_key("isoWeekYear") || d.contains_key("isoWeek") || d.contains_key("isoDayOfWeek");
    if iso && (d.contains_key("year") || d.contains_key("month") || d.contains_key("day")) {
        return Err(err(40489, "$dateFromParts does not allow mixing natural dates with ISO dates"));
    }
    let Some(tz) = tz_of(d, sc, "$dateFromParts")? else { return null() };
    macro_rules! part {
        ($k:expr, $lo:expr, $hi:expr) => {
            match int_part(d, $k, sc, "$dateFromParts", $lo, $hi)? {
                None => return null(),
                Some(v) => v,
            }
        };
    }
    let hour = part!("hour", -10_000_000, 10_000_000).unwrap_or(0);
    let minute = part!("minute", -10_000_000, 10_000_000).unwrap_or(0);
    let second = part!("second", -1_000_000_000, 1_000_000_000).unwrap_or(0);
    let milli = part!("millisecond", -1_000_000_000_000, 1_000_000_000_000).unwrap_or(0);
    let base: NaiveDate = if iso {
        let Some(y) = part!("isoWeekYear", 1, 9999) else { return Err(err(40516, "Missing 'isoWeekYear' parameter to $dateFromParts")) };
        let w = part!("isoWeek", -10_000, 10_000).unwrap_or(1);
        let dow = part!("isoDayOfWeek", -10_000, 10_000).unwrap_or(1);
        let jan4 = NaiveDate::from_ymd_opt(y as i32, 1, 4).ok_or_else(|| err(40523, "year out of range"))?;
        let week1_monday = jan4 - CDuration::days(jan4.weekday().num_days_from_monday() as i64);
        week1_monday + CDuration::days((w - 1) * 7 + (dow - 1))
    } else {
        let Some(y) = part!("year", 1, 9999) else { return Err(err(40516, "Missing 'year' parameter to $dateFromParts")) };
        let m = part!("month", -1_000_000, 1_000_000).unwrap_or(1);
        let day = part!("day", -1_000_000, 1_000_000).unwrap_or(1);
        let m0 = y * 12 + (m - 1);
        let (yy, mm) = (m0.div_euclid(12), m0.rem_euclid(12) + 1);
        NaiveDate::from_ymd_opt(yy as i32, mm as u32, 1).ok_or_else(|| err(9, "date out of range"))? + CDuration::days(day - 1)
    };
    let midnight = base.and_hms_opt(0, 0, 0).unwrap();
    let local_dt = midnight + CDuration::hours(hour) + CDuration::minutes(minute) + CDuration::seconds(second) + CDuration::milliseconds(milli);
    some(Bson::DateTime(DateTime::from_millis(from_local(local_dt, tz))))
}

/// Parse a date string: ISO 8601 forms, with an optional `format` and time zone.
pub(super) fn parse_date_string(s: &str, format: Option<&str>, tz: Option<Tz>) -> Result<i64, String> {
    let tz_default = tz.unwrap_or(Tz::Utc);
    if let Some(f) = format {
        return parse_with_format(s, f, tz_default);
    }
    let t = s.trim();
    // trailing zone designator
    let (body, zone): (&str, Option<Tz>) = if let Some(b) = t.strip_suffix('Z') {
        (b, Some(Tz::Utc))
    } else if t.len() > 6 && (t.as_bytes()[t.len() - 6] == b'+' || t.as_bytes()[t.len() - 6] == b'-') && t.as_bytes()[t.len() - 3] == b':' && t.contains('T') {
        (&t[..t.len() - 6], parse_tz(&t[t.len() - 6..]))
    } else if t.len() > 5 && (t.as_bytes()[t.len() - 5] == b'+' || t.as_bytes()[t.len() - 5] == b'-') && t.contains('T') {
        (&t[..t.len() - 5], parse_tz(&t[t.len() - 5..]))
    } else {
        (t, None)
    };
    let zone = zone.unwrap_or(tz_default);
    let mut tried = Vec::new();
    for pat in ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%d %H:%M:%S", "%Y%m%dT%H%M%S%.f", "%Y%m%dT%H%M%S"] {
        if let Ok(dt) = NaiveDateTime::parse_from_str(body, pat) {
            return Ok(from_local(dt, zone));
        }
        tried.push(pat);
    }
    for pat in ["%Y-%m-%d", "%Y%m%d", "%Y/%m/%d", "%m/%d/%Y", "%B %d, %Y", "%b %d, %Y"] {
        if let Ok(d) = NaiveDate::parse_from_str(body, pat) {
            return Ok(from_local(d.and_hms_opt(0, 0, 0).unwrap(), zone));
        }
    }
    Err(format!("Error parsing date string '{s}'"))
}

fn parse_with_format(s: &str, fmt: &str, tz: Tz) -> Result<i64, String> {
    let mut chrono_fmt = String::new();
    let mut it = fmt.chars();
    let mut zone: Option<Tz> = None;
    let mut has_z = false;
    while let Some(c) = it.next() {
        if c != '%' {
            chrono_fmt.push(c);
            continue;
        }
        match it.next() {
            Some('Y') => chrono_fmt.push_str("%Y"),
            Some('m') => chrono_fmt.push_str("%m"),
            Some('d') => chrono_fmt.push_str("%d"),
            Some('H') => chrono_fmt.push_str("%H"),
            Some('M') => chrono_fmt.push_str("%M"),
            Some('S') => chrono_fmt.push_str("%S"),
            Some('L') => chrono_fmt.push_str("%3f"),
            Some('j') => chrono_fmt.push_str("%j"),
            Some('z') => {
                chrono_fmt.push_str("%z");
                has_z = true;
            }
            Some('Z') => {
                chrono_fmt.push_str("%:z");
                has_z = true;
            }
            Some('%') => chrono_fmt.push_str("%%"),
            Some(o) => return Err(format!("Invalid format character '%{o}'")),
            None => return Err("Unmatched '%'".into()),
        }
    }
    let _ = &mut zone;
    if fmt.contains("%j") {
        // MongoDB resolves a day of the year as January 1st plus that many days (one day later than the calendar day).
        let f2 = fmt.replace("%j", "%j");
        if let Ok(dt) = NaiveDate::parse_from_str(s, &chrono_fmt) {
            let y = dt.year();
            let doy = dt.ordinal() as i64;
            return Ok(from_local(NaiveDate::from_yo_opt(y, 1).unwrap().and_hms_opt(0, 0, 0).unwrap() + CDuration::days(doy), tz)).map(|v| { let _ = &f2; v });
        }
    }
    if has_z {
        if let Ok(dt) = chrono::DateTime::parse_from_str(s, &chrono_fmt) {
            return Ok(dt.timestamp_millis());
        }
        return Err(format!("Error parsing date string '{s}'"));
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, &chrono_fmt) {
        return Ok(from_local(dt, tz));
    }
    if let Ok(d) = NaiveDate::parse_from_str(s, &chrono_fmt) {
        return Ok(from_local(d.and_hms_opt(0, 0, 0).unwrap(), tz));
    }
    Err(format!("Error parsing date string '{s}'"))
}

fn date_from_string(arg: &Bson, sc: &Scope) -> Out {
    let d = doc_arg("$dateFromString", arg)?;
    for k in d.keys() {
        if !matches!(k.as_str(), "dateString" | "format" | "timezone" | "onError" | "onNull") {
            return Err(err(40541, format!("Unrecognized argument to $dateFromString: {k}")));
        }
    }
    let ds = d.get("dateString").ok_or_else(|| err(40542, "Missing 'dateString' parameter to $dateFromString"))?;
    let fmt = match d.get("format") {
        None => None,
        Some(f) => match ev(f, sc)? {
            Some(Bson::String(s)) => Some(s),
            None | Some(Bson::Null) => return null(),
            Some(o) => return Err(err(40684, format!("$dateFromString requires that 'format' be a string, found: {}", type_name(&o)))),
        },
    };
    let tz = tz_of(d, sc, "$dateFromString")?;
    let v = ev(ds, sc)?;
    if is_nullish(&v) {
        return match d.get("onNull") {
            Some(x) => Ok(Some(ev(x, sc)?)),
            None => null(),
        };
    }
    let Some(tz) = tz else { return null() };
    let s = match v.unwrap() {
        Bson::String(s) => s,
        o => {
            let e = err(241, format!("$dateFromString requires that 'dateString' be a string, found: {} with value {}", type_name(&o), crate::update::fmt_bson(&o)));
            return match d.get("onError") {
                Some(x) => Ok(Some(ev(x, sc)?)),
                None => Err(e),
            };
        }
    };
    match parse_date_string(&s, fmt.as_deref(), Some(tz)) {
        Ok(ms) => some(Bson::DateTime(DateTime::from_millis(ms))),
        Err(m) => match d.get("onError") {
            Some(x) => Ok(Some(ev(x, sc)?)),
            None => {
                let mut e = err(241, m);
                e.name = "ConversionFailure";
                Err(e)
            }
        },
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Unit {
    Year,
    Quarter,
    Month,
    Week,
    Day,
    Hour,
    Minute,
    Second,
    Milli,
}

fn unit_of(d: &Document, key: &str, sc: &Scope, who: &str) -> R<Option<Unit>> {
    let e = d.get(key).ok_or_else(|| err(5439015, format!("Missing '{key}' parameter to {who}")))?;
    match ev(e, sc)? {
        None | Some(Bson::Null) => Ok(None),
        Some(Bson::String(s)) => Ok(Some(match s.as_str() {
            "year" => Unit::Year,
            "quarter" => Unit::Quarter,
            "month" => Unit::Month,
            "week" => Unit::Week,
            "day" => Unit::Day,
            "hour" => Unit::Hour,
            "minute" => Unit::Minute,
            "second" => Unit::Second,
            "millisecond" => Unit::Milli,
            other => return Err(err(9, format!("unknown time unit value: {other}"))),
        })),
        Some(o) => Err(err(5439016, format!("{who} requires '{key}' to be a string, but got {}", type_name(&o)))),
    }
}

fn add_months(l: NaiveDateTime, months: i64) -> R<NaiveDateTime> {
    let m0 = l.year() as i64 * 12 + (l.month0() as i64) + months;
    let (y, m) = (m0.div_euclid(12), m0.rem_euclid(12) as u32 + 1);
    let last = NaiveDate::from_ymd_opt(y as i32, m, 1).and_then(|d| (1..=31).rev().find_map(|dd| d.with_day(dd))).ok_or_else(|| err(5166406, "date out of range"))?;
    let day = l.day().min(last.day());
    Ok(NaiveDate::from_ymd_opt(y as i32, m, day).ok_or_else(|| err(5166406, "date out of range"))?.and_time(l.time()))
}

fn date_add(name: &str, arg: &Bson, sc: &Scope) -> Out {
    let d = doc_arg(name, arg)?;
    for k in d.keys() {
        if !matches!(k.as_str(), "startDate" | "unit" | "amount" | "timezone") {
            return Err(err(5166301, format!("Unrecognized argument to {name}: {k}")));
        }
    }
    let start = d.get("startDate").ok_or_else(|| err(5166402, format!("Missing 'startDate' parameter to {name}")))?;
    let amount_e = d.get("amount").ok_or_else(|| err(5166403, format!("Missing 'amount' parameter to {name}")))?;
    let Some(unit) = unit_of(d, "unit", sc, name)? else { return null() };
    let Some(tz) = tz_of(d, sc, name)? else { return null() };
    let amount = match ev(amount_e, sc)? {
        None | Some(Bson::Null) => return null(),
        Some(v) => match as_num(&v) {
            Some(n) if to_f(n).fract() == 0.0 => to_f(n) as i64,
            _ => return Err(err(5166405, format!("Invalid amount: {}", crate::update::fmt_bson(&v)))),
        },
    };
    let amount = if name == "$dateSubtract" { -amount } else { amount };
    let Some(ms) = date_value(ev(start, sc)?)? else { return null() };
    let l = local(ms, tz)?;
    let out = match unit {
        Unit::Year => from_local(add_months(l, amount * 12)?, tz),
        Unit::Quarter => from_local(add_months(l, amount * 3)?, tz),
        Unit::Month => from_local(add_months(l, amount)?, tz),
        Unit::Week => from_local(l + CDuration::days(amount * 7), tz),
        Unit::Day => from_local(l + CDuration::days(amount), tz),
        Unit::Hour => ms + amount * 3_600_000,
        Unit::Minute => ms + amount * 60_000,
        Unit::Second => ms + amount * 1000,
        Unit::Milli => ms + amount,
    };
    some(Bson::DateTime(DateTime::from_millis(out)))
}

fn weekday_of(d: &Document, sc: &Scope) -> R<Weekday> {
    match d.get("startOfWeek") {
        None => Ok(Weekday::Sun),
        Some(e) => match ev(e, sc)? {
            Some(Bson::String(s)) => match s.to_ascii_lowercase().as_str() {
                "monday" | "mon" => Ok(Weekday::Mon),
                "tuesday" | "tue" => Ok(Weekday::Tue),
                "wednesday" | "wed" => Ok(Weekday::Wed),
                "thursday" | "thu" => Ok(Weekday::Thu),
                "friday" | "fri" => Ok(Weekday::Fri),
                "saturday" | "sat" => Ok(Weekday::Sat),
                "sunday" | "sun" => Ok(Weekday::Sun),
                other => Err(err(9, format!("unknown day of week value: {other}"))),
            },
            _ => Err(err(5439015, "'startOfWeek' must be a string")),
        },
    }
}

fn week_start(l: NaiveDateTime, sow: Weekday) -> NaiveDate {
    let diff = (l.weekday().num_days_from_sunday() as i64 - sow.num_days_from_sunday() as i64).rem_euclid(7);
    l.date() - CDuration::days(diff)
}

fn date_diff(arg: &Bson, sc: &Scope) -> Out {
    let d = doc_arg("$dateDiff", arg)?;
    for k in d.keys() {
        if !matches!(k.as_str(), "startDate" | "endDate" | "unit" | "timezone" | "startOfWeek") {
            return Err(err(5166301, format!("Unrecognized argument to $dateDiff: {k}")));
        }
    }
    let s_e = d.get("startDate").ok_or_else(|| err(5166302, "Missing 'startDate' parameter to $dateDiff"))?;
    let e_e = d.get("endDate").ok_or_else(|| err(5166303, "Missing 'endDate' parameter to $dateDiff"))?;
    let Some(unit) = unit_of(d, "unit", sc, "$dateDiff")? else { return null() };
    let Some(tz) = tz_of(d, sc, "$dateDiff")? else { return null() };
    let (Some(a), Some(b)) = (date_value(ev(s_e, sc)?)?, date_value(ev(e_e, sc)?)?) else { return null() };
    let sow = weekday_of(d, sc)?;
    let (la, lb) = (local(a, tz)?, local(b, tz)?);
    let diff: i64 = match unit {
        Unit::Year => lb.year() as i64 - la.year() as i64,
        Unit::Quarter => (lb.year() as i64 * 4 + lb.month0() as i64 / 3) - (la.year() as i64 * 4 + la.month0() as i64 / 3),
        Unit::Month => (lb.year() as i64 * 12 + lb.month0() as i64) - (la.year() as i64 * 12 + la.month0() as i64),
        Unit::Week => (week_start(lb, sow) - week_start(la, sow)).num_days() / 7,
        Unit::Day => (lb.date() - la.date()).num_days(),
        Unit::Hour => (b + offset_at(tz, b) as i64 * 1000).div_euclid(3_600_000) - (a + offset_at(tz, a) as i64 * 1000).div_euclid(3_600_000),
        Unit::Minute => (b + offset_at(tz, b) as i64 * 1000).div_euclid(60_000) - (a + offset_at(tz, a) as i64 * 1000).div_euclid(60_000),
        Unit::Second => b.div_euclid(1000) - a.div_euclid(1000),
        Unit::Milli => b - a,
    };
    some(Bson::Int64(diff))
}

fn date_trunc(arg: &Bson, sc: &Scope) -> Out {
    let d = doc_arg("$dateTrunc", arg)?;
    for k in d.keys() {
        if !matches!(k.as_str(), "date" | "unit" | "binSize" | "timezone" | "startOfWeek") {
            return Err(err(5439100, format!("Unrecognized argument to $dateTrunc: {k}")));
        }
    }
    let date = d.get("date").ok_or_else(|| err(5439101, "Missing 'date' parameter to $dateTrunc"))?;
    let Some(unit) = unit_of(d, "unit", sc, "$dateTrunc")? else { return null() };
    let Some(tz) = tz_of(d, sc, "$dateTrunc")? else { return null() };
    let bin = match d.get("binSize") {
        None => 1,
        Some(e) => match ev(e, sc)? {
            None | Some(Bson::Null) => return null(),
            Some(v) => match as_num(&v) {
                Some(n) if to_f(n).fract() == 0.0 && to_f(n) > 0.0 => to_f(n) as i64,
                _ => return Err(err(5439017, "'binSize' must be a positive integer")),
            },
        },
    };
    let Some(ms) = date_value(ev(date, sc)?)? else { return null() };
    let sow = weekday_of(d, sc)?;
    let l = local(ms, tz)?;
    let ref_date = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
    let ref_monday = NaiveDate::from_ymd_opt(2000, 1, 3).unwrap();
    let out_local: NaiveDateTime = match unit {
        Unit::Year => {
            let y = (l.year() as i64 - 2000).div_euclid(bin) * bin + 2000;
            NaiveDate::from_ymd_opt(y as i32, 1, 1).ok_or_else(|| err(9, "date out of range"))?.and_hms_opt(0, 0, 0).unwrap()
        }
        Unit::Quarter | Unit::Month => {
            let per = if unit == Unit::Quarter { 3 } else { 1 };
            let m = (l.year() as i64 - 2000) * 12 + l.month0() as i64;
            let b = m.div_euclid(per * bin) * per * bin;
            NaiveDate::from_ymd_opt((2000 + b.div_euclid(12)) as i32, b.rem_euclid(12) as u32 + 1, 1).ok_or_else(|| err(9, "date out of range"))?.and_hms_opt(0, 0, 0).unwrap()
        }
        Unit::Week => {
            let ws = week_start(l, sow);
            let anchor = ref_monday - CDuration::days((ref_monday.weekday().num_days_from_sunday() as i64 - sow.num_days_from_sunday() as i64).rem_euclid(7));
            let weeks = (ws - anchor).num_days().div_euclid(7);
            (anchor + CDuration::days(weeks.div_euclid(bin) * bin * 7)).and_hms_opt(0, 0, 0).unwrap()
        }
        Unit::Day => {
            let days = (l.date() - ref_date).num_days();
            (ref_date + CDuration::days(days.div_euclid(bin) * bin)).and_hms_opt(0, 0, 0).unwrap()
        }
        Unit::Hour | Unit::Minute | Unit::Second | Unit::Milli => {
            let unit_ms = match unit {
                Unit::Hour => 3_600_000,
                Unit::Minute => 60_000,
                Unit::Second => 1000,
                _ => 1,
            };
            let ref_ms = from_local(ref_date.and_hms_opt(0, 0, 0).unwrap(), tz);
            let t = (ms - ref_ms).div_euclid(unit_ms * bin) * unit_ms * bin + ref_ms;
            return some(Bson::DateTime(DateTime::from_millis(t)));
        }
    };
    some(Bson::DateTime(DateTime::from_millis(from_local(out_local, tz))))
}
