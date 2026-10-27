//! Collation (locale-aware string comparison). The collation document is validated the way mongod 7.0 does
//! (field types, enumerations, the list of ICU 57 locales with their per-locale defaults in `collation_locales.json`),
//! expanded to the full document mongod reports (`listCollections`, `listIndexes`, `explain`), and applied with the ICU4X
//! collator: `compare` of strings, grouping keys and index keys. The collation in force for a command is carried in a
//! task-local ([`scope`]) so the comparison functions need no extra argument.
//!
//! Differences from mongod's ICU 57 that are known and not hidden: ICU4X carries newer CLDR data, so a handful of
//! tailorings differ (`lv`, `se`, `smn`, `kl`, `haw`, `*@collation=search` of cs/da/de/sk/smn/se/kl); `backwards` can be
//! set only through the locale (fr_CA is `fr` with backwards) and a different value is refused; `normalization` is always on.

use crate::error::MongoError;
use bson::{doc, Bson, Document};
use icu_collator::options::{AlternateHandling, CaseLevel, CollatorOptions, MaxVariable, Strength};
use icu_collator::preferences::{CollationCaseFirst, CollationNumericOrdering};
use icu_collator::{Collator, CollatorPreferences};
use icu_locale::Locale;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex, OnceLock};

type R<T> = Result<T, MongoError>;

const TABLE: &str = include_str!("collation_locales.json");

struct Table {
    defaults: Document,
    valid: HashMap<String, Document>,
    hints: HashMap<String, String>,
}

fn table() -> &'static Table {
    static T: OnceLock<Table> = OnceLock::new();
    T.get_or_init(|| {
        let v: serde_json::Value = serde_json::from_str(TABLE).expect("collation_locales.json");
        // (serde_json sorts object keys; the fields of a collation document have their own order)
        let d = |x: &serde_json::Value| -> Document {
            let mut out = Document::new();
            if let Some(m) = x.as_object() {
                for k in ORDER {
                    match m.get(k) {
                        Some(serde_json::Value::Bool(b)) => out.insert(k, *b),
                        Some(serde_json::Value::Number(n)) => out.insert(k, n.as_i64().unwrap_or(0) as i32),
                        Some(serde_json::Value::String(s)) => out.insert(k, s.clone()),
                        _ => None,
                    };
                }
            }
            out
        };
        let valid = v["valid"].as_object().map(|m| m.iter().map(|(k, x)| (k.clone(), d(x))).collect()).unwrap_or_default();
        let hints = v["hints"].as_object().map(|m| m.iter().map(|(k, x)| (k.clone(), x.as_str().unwrap_or("").to_string())).collect()).unwrap_or_default();
        Table { defaults: d(&v["defaults"]), valid, hints }
    })
}

pub struct Collation {
    id: u64,
    /// The expanded document mongod reports.
    pub spec: Document,
    /// The locale handed to ICU4X (`fr_CA` stands in for `fr` with `backwards: true`).
    use_locale: String,
}

thread_local! {
    /// ICU4X collators are not `Send`: every thread builds its own from the `Collation`.
    static LOCAL: std::cell::RefCell<(u64, Option<Rc<Collator>>, HashMap<u64, Rc<Collator>>)> = std::cell::RefCell::new((0, None, HashMap::new()));
}

impl Collation {
    fn with<T>(&self, f: impl FnOnce(&Collator) -> T) -> T {
        LOCAL.with(|l| {
            let mut l = l.borrow_mut();
            if l.0 != self.id || l.1.is_none() {
                let c = match l.2.get(&self.id) {
                    Some(c) => Rc::clone(c),
                    None => {
                        let c = Rc::new(make_collator(&self.use_locale, &self.spec).expect("collator validated at parse"));
                        l.2.insert(self.id, Rc::clone(&c));
                        c
                    }
                };
                l.0 = self.id;
                l.1 = Some(c);
            }
            let c = Rc::clone(l.1.as_ref().unwrap());
            drop(l);
            f(&c)
        })
    }

    pub fn cmp(&self, a: &str, b: &str) -> Ordering {
        if a == b {
            return Ordering::Equal;
        }
        self.with(|c| c.as_borrowed().compare(a, b))
    }

    /// Bytes whose order is this collation's order (index keys, grouping keys).
    pub fn sort_key(&self, s: &str, out: &mut Vec<u8>) {
        self.with(|c| {
            let Ok(()) = c.as_borrowed().write_sort_key_to(s, out);
        })
    }

    pub fn to_doc(&self) -> Document {
        self.spec.clone()
    }
}

impl std::fmt::Debug for Collation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Collation({})", self.spec)
    }
}

struct Ctx {
    /// The command (or statement) named a collation, `simple` included: the collection's default does not apply.
    explicit: bool,
    /// The default collation of the command's collection was taken.
    defaulted: bool,
    c: Option<Arc<Collation>>,
}

tokio::task_local! {
    static CUR: std::cell::RefCell<Ctx>;
}

/// Run `f` with the collation named by the command (`Some`: `Some(None)` is `simple`, `None`: none named, the default of
/// the collection the command works on applies once [`note_default`] sees it).
pub async fn scope<F: std::future::Future>(explicit: Option<Option<Arc<Collation>>>, f: F) -> F::Output {
    let ctx = match explicit {
        Some(c) => Ctx { explicit: true, defaulted: true, c },
        None => Ctx { explicit: false, defaulted: false, c: None },
    };
    CUR.scope(std::cell::RefCell::new(ctx), f).await
}

/// Synchronous code under a given collation (an index's own).
pub fn sync_scope<T>(c: Option<Arc<Collation>>, f: impl FnOnce() -> T) -> T {
    CUR.sync_scope(std::cell::RefCell::new(Ctx { explicit: true, defaulted: true, c }), f)
}

/// The default collation of the collection the command works on, taken unless the command named a collation.
pub fn note_default(c: Option<Arc<Collation>>) {
    let _ = CUR.try_with(|x| {
        let mut x = x.borrow_mut();
        if !x.explicit && !x.defaulted {
            x.defaulted = true;
            x.c = c;
        }
    });
}

/// The collation in force, if any.
pub fn current() -> Option<Arc<Collation>> {
    CUR.try_with(|c| c.borrow().c.clone()).ok().flatten()
}

/// The sort key of `s` under the collation in force (none: `None`).
pub fn sort_key_current(s: &str) -> Option<Vec<u8>> {
    CUR.try_with(|c| {
        c.borrow().c.as_ref().map(|c| {
            let mut k = Vec::new();
            c.sort_key(s, &mut k);
            k
        })
    })
    .ok()
    .flatten()
}

/// Compare two strings under the collation in force (`None`: none is in force).
#[inline]
pub fn cmp_current(a: &str, b: &str) -> Option<Ordering> {
    CUR.try_with(|c| c.borrow().c.as_ref().map(|c| c.cmp(a, b))).ok().flatten()
}

/// The same collation (by its expanded document)? `None` is the simple collation.
pub fn same(a: &Option<Arc<Collation>>, b: &Option<Arc<Collation>>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => Arc::ptr_eq(x, y) || x.spec == y.spec,
        _ => false,
    }
}

fn type_of(v: &Bson) -> &'static str {
    crate::update::type_name(v)
}

fn canonical(l: &str) -> Option<(String, String)> {
    let (base, kw) = match l.split_once('@') {
        Some((b, k)) => (b, k.to_string()),
        None => (l, String::new()),
    };
    let mut parts = base.split(['_', '-']);
    let first = parts.next().filter(|s| !s.is_empty())?;
    let mut out = vec![first.to_lowercase()];
    for p in parts {
        let alpha = p.chars().all(|c| c.is_ascii_alphabetic());
        if p.len() == 4 && alpha {
            let mut c = p.chars();
            out.push(c.next().map(|f| f.to_uppercase().collect::<String>() + &c.as_str().to_lowercase()).unwrap_or_default());
        } else {
            out.push(p.to_uppercase());
        }
    }
    Some((out.join("_"), kw))
}

/// The locale mongod would suggest for an invalid `locale` string.
fn hint_for(l: &str) -> Option<String> {
    let t = table();
    if let Some(h) = t.hints.get(l) {
        return Some(h.clone());
    }
    let (canon, kw) = canonical(l)?;
    let mut parts: Vec<&str> = canon.split('_').collect();
    let coll = kw.strip_prefix("collation=").map(str::to_string);
    while !parts.is_empty() {
        let cand = parts.join("_");
        if let Some(c) = &coll {
            let with = format!("{cand}@collation={c}");
            if t.valid.contains_key(&with) {
                return Some(with);
            }
        }
        if t.valid.contains_key(&cand) {
            return Some(cand);
        }
        parts.pop();
    }
    None
}

fn bad_locale(l: &str, spec: &Document) -> MongoError {
    let mut msg = format!("Field 'locale' is invalid in: {}", crate::update::fmt_doc(spec));
    if let Some(h) = hint_for(l) {
        msg.push_str(&format!(". Did you mean '{h}'?"));
    }
    MongoError::bad_value(msg)
}

fn icu_locale(l: &str) -> Option<Locale> {
    if l == "en_US_POSIX" {
        return "en-US-u-va-posix".parse().ok();
    }
    let (base, kw) = match l.split_once('@') {
        Some((b, k)) => (b, Some(k)),
        None => (l, None),
    };
    let mut tag = base.replace('_', "-");
    if let Some(k) = kw {
        let ty = k.strip_prefix("collation=")?;
        let ty = match ty {
            "phonebook" => "phonebk",
            "traditional" => "trad",
            "dictionary" => "dict",
            "gb2312han" => "gb2312",
            other => other,
        };
        tag.push_str("-u-co-");
        tag.push_str(ty);
    }
    tag.parse().ok()
}

fn cache() -> &'static Mutex<HashMap<String, Arc<Collation>>> {
    static C: OnceLock<Mutex<HashMap<String, Arc<Collation>>>> = OnceLock::new();
    C.get_or_init(|| Mutex::new(HashMap::new()))
}

fn int_of(v: &Bson, field: &str) -> R<i64> {
    match v {
        Bson::Int32(i) => Ok(*i as i64),
        Bson::Int64(i) => Ok(*i),
        Bson::Double(d) => Ok(*d as i64),
        Bson::Decimal128(d) => Ok(d.to_string().parse::<f64>().map(|f| f as i64).unwrap_or(0)),
        other => Err(MongoError::type_mismatch(format!("BSON field 'collation.{field}' is the wrong type '{}', expected types '[long, int, decimal, double']", type_of(other)))),
    }
}

fn bool_of(v: &Bson, field: &str) -> R<bool> {
    match v {
        Bson::Boolean(b) => Ok(*b),
        other => Err(MongoError::type_mismatch(format!("BSON field 'collation.{field}' is the wrong type '{}', expected type 'bool'", type_of(other)))),
    }
}

fn str_enum<'a>(v: &'a Bson, field: &str, allowed: &[&str]) -> R<&'a str> {
    match v {
        Bson::String(s) => {
            if allowed.contains(&s.as_str()) {
                Ok(s)
            } else {
                Err(MongoError::bad_value(format!("Enumeration value '{s}' for field 'collation.{field}' is not a valid value.")))
            }
        }
        other => Err(MongoError::type_mismatch(format!("BSON field 'collation.{field}' is the wrong type '{}', expected type 'string'", type_of(other)))),
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    /// A query's collation: the fields it leaves out take the locale's defaults.
    Query,
    /// The collation of a new collection: every field is stored explicitly (the global defaults for those left out).
    Create,
    /// The collation of a new index: checked like a query's, stored like a collection's.
    Index,
}

struct Parsed {
    collation: Option<Arc<Collation>>,
    /// Create / Index: the document to store (all fields explicit).
    stored: Document,
}

const ORDER: [&str; 9] = ["caseLevel", "caseFirst", "strength", "numericOrdering", "alternate", "maxVariable", "normalization", "backwards", "version"];

/// Validate a `collation` document and build the collator. `Ok(None)` is the simple (binary) collation.
pub fn parse(spec: &Bson) -> R<Option<Arc<Collation>>> {
    parse_with(spec, Mode::Query).map(|p| p.collation)
}

/// A collation to store with a new collection (`create`): the expanded document and the collation.
pub fn for_collection(spec: &Bson) -> R<Option<Arc<Collation>>> {
    parse_with(spec, Mode::Create).map(|p| p.collation)
}

/// A collation to store with a new index: the expanded document and the collation.
pub fn for_index(spec: &Bson) -> R<Option<Arc<Collation>>> {
    parse_with(spec, Mode::Index).map(|p| p.collation)
}

fn parse_with(spec: &Bson, mode: Mode) -> R<Parsed> {
    let simple = || Parsed { collation: None, stored: Document::new() };
    let Bson::Document(spec) = spec else {
        return Err(MongoError::type_mismatch("Expected field collationto be of type object".into()));
    };
    if spec.is_empty() {
        return Ok(simple());
    }
    let t = table();
    let mut locale: Option<String> = None;
    let mut over = Document::new();
    for (k, v) in spec {
        match k.as_str() {
            "locale" => match v {
                Bson::String(s) => locale = Some(s.clone()),
                other => return Err(MongoError::type_mismatch(format!("BSON field 'collation.locale' is the wrong type '{}', expected type 'string'", type_of(other)))),
            },
            "caseLevel" | "numericOrdering" | "backwards" | "normalization" => {
                if k == "backwards" && !matches!(v, Bson::Boolean(_)) {
                    return Err(MongoError::type_mismatch(format!("Field 'backwards' should be a boolean value, but found: {}", type_of(v))));
                }
                over.insert(k.clone(), bool_of(v, k)?);
            }
            "strength" => {
                let n = int_of(v, k)?;
                if n > 5 {
                    return Err(MongoError::new(51024, "Location51024", format!("BSON field 'strength' value must be <= 5, actual value '{n}'")));
                }
                if n < 0 {
                    return Err(MongoError::new(51024, "Location51024", format!("BSON field 'strength' value must be >= 0, actual value '{n}'")));
                }
                if n < 1 {
                    return Err(MongoError::bad_value(format!("Enumeration value '{n}' for field 'collation.strength' is not a valid value.")));
                }
                over.insert("strength", n as i32);
            }
            "caseFirst" => {
                over.insert(k.clone(), str_enum(v, k, &["lower", "upper", "off"])?);
            }
            "alternate" => {
                over.insert(k.clone(), str_enum(v, k, &["non-ignorable", "shifted"])?);
            }
            "maxVariable" => {
                over.insert(k.clone(), str_enum(v, k, &["punct", "space"])?);
            }
            "version" => {
                over.insert(k.clone(), v.clone());
            }
            other => return Err(MongoError::new(40415, "IDLUnknownField", format!("BSON field 'collation.{other}' is an unknown field."))),
        }
    }
    let Some(locale) = locale else {
        return Err(MongoError::new(40414, "IDLFailedToParse", "BSON field 'collation.locale' is missing but a required field".into()));
    };
    // what a message shows of the specification: as given, or (create) expanded with the global defaults
    let shown = || -> String {
        if mode == Mode::Create {
            let mut exp = doc! { "locale": locale.clone() };
            for k in ORDER.iter().filter(|k| **k != "backwards" && **k != "version") {
                exp.insert(*k, over.get(k).cloned().unwrap_or_else(|| t.defaults.get(k).cloned().unwrap()));
            }
            crate::update::fmt_doc(&exp)
        } else {
            crate::update::fmt_doc(spec)
        }
    };
    if locale.is_empty() {
        return Err(MongoError::bad_value(format!("Field 'locale' cannot be the empty string in: {}", shown())));
    }
    if locale == "simple" {
        return Ok(simple());
    }
    let Some(locale_defaults) = t.valid.get(&locale) else {
        let mut msg = format!("Field 'locale' is invalid in: {}", shown());
        if let Some(h) = hint_for(&locale) {
            msg.push_str(&format!(". Did you mean '{h}'?"));
        }
        return Err(MongoError::bad_value(msg));
    };
    let expand = |from_locale: bool| -> Document {
        let mut exp = doc! { "locale": locale.clone() };
        for k in ORDER {
            // (`backwards` is part of the locale, as in fr_CA, whether the collation is a query's or a collection's)
            let from_loc = from_locale || k == "backwards";
            let v = over.get(k).cloned().or_else(|| if from_loc { locale_defaults.get(k).cloned() } else { None }).unwrap_or_else(|| t.defaults.get(k).cloned().unwrap());
            exp.insert(k, v);
        }
        exp
    };
    // the options in force: a query's and an index's take the locale's defaults for what they leave out; a collection's do not
    // (except `backwards`, which a locale such as fr_CA has on)
    let mut eff = expand(mode != Mode::Create);
    if mode == Mode::Create && !over.contains_key("backwards") {
        eff.insert("backwards", locale_defaults.get_bool("backwards").unwrap_or(false));
    }
    let strength = eff.get_i32("strength").unwrap_or(3);
    if eff.get_str("caseFirst").unwrap_or("off") != "off" && !eff.get_bool("caseLevel").unwrap_or(false) && strength <= 2 {
        return Err(MongoError::bad_value(format!("'caseFirst' is invalid unless 'caseLevel' is on or 'strength' is greater than 2 in: {}", shown())));
    }
    if eff.get_bool("backwards").unwrap_or(false) && strength == 1 {
        return Err(MongoError::bad_value(format!("'backwards' is invalid with 'strength' of 1 in: {}", shown())));
    }
    if mode != Mode::Query {
        // stored explicitly; the collation is built from what is stored
        let stored = expand(false);
        let c = parse_with(&Bson::Document(stored.clone()), Mode::Query)?.collation;
        return Ok(Parsed { collation: c, stored });
    }
    let key = eff.to_string();
    if let Some(c) = cache().lock().unwrap().get(&key) {
        return Ok(Parsed { collation: Some(Arc::clone(c)), stored: Document::new() });
    }
    let c = Arc::new(build(&locale, eff)?);
    cache().lock().unwrap().insert(key, Arc::clone(&c));
    Ok(Parsed { collation: Some(c), stored: Document::new() })
}

fn build(locale: &str, exp: Document) -> R<Collation> {
    static IDS: AtomicU64 = AtomicU64::new(1);
    let t = table();
    // `backwards` comes with the locale data (fr_CA); the other values are refused rather than ignored.
    let want_back = exp.get_bool("backwards").unwrap_or(false);
    let natural_back = t.valid.get(locale).and_then(|d| d.get_bool("backwards").ok()).unwrap_or(false);
    let mut use_locale = locale.to_string();
    if want_back != natural_back {
        use_locale = match (locale, want_back) {
            ("fr", true) => "fr_CA".into(),
            ("fr_CA", false) => "fr".into(),
            _ => return Err(MongoError::bad_value(format!("Collation option 'backwards: {want_back}' is not supported for locale '{locale}' by this server"))),
        };
    }
    make_collator(&use_locale, &exp)?;
    Ok(Collation { id: IDS.fetch_add(1, Relaxed), spec: exp, use_locale })
}

fn make_collator(locale: &str, exp: &Document) -> R<Collator> {
    let loc = icu_locale(locale).ok_or_else(|| bad_locale(locale, &doc! { "locale": locale }))?;
    let mut prefs = CollatorPreferences::from(&loc);
    prefs.case_first = Some(match exp.get_str("caseFirst").unwrap_or("off") {
        "upper" => CollationCaseFirst::Upper,
        "lower" => CollationCaseFirst::Lower,
        _ => CollationCaseFirst::False,
    });
    prefs.numeric_ordering = Some(if exp.get_bool("numericOrdering").unwrap_or(false) { CollationNumericOrdering::True } else { CollationNumericOrdering::False });
    let mut o = CollatorOptions::default();
    o.strength = Some(match exp.get_i32("strength").unwrap_or(3) {
        1 => Strength::Primary,
        2 => Strength::Secondary,
        3 => Strength::Tertiary,
        4 => Strength::Quaternary,
        _ => Strength::Identical,
    });
    o.case_level = Some(if exp.get_bool("caseLevel").unwrap_or(false) { CaseLevel::On } else { CaseLevel::Off });
    o.alternate_handling = Some(if exp.get_str("alternate").unwrap_or("non-ignorable") == "shifted" { AlternateHandling::Shifted } else { AlternateHandling::NonIgnorable });
    o.max_variable = Some(if exp.get_str("maxVariable").unwrap_or("punct") == "space" { MaxVariable::Space } else { MaxVariable::Punctuation });
    let c = Collator::try_new(prefs, o).map_err(|e| MongoError::bad_value(format!("Failed to create collator for locale '{locale}': {e:?}")))?;
    Ok(c.static_to_owned())
}

/// The collation named by the `collation` field of command `name` (which parses the field its own way): `None` when the
/// command names none, `Some(None)` for `simple`.
pub fn of_command(name: &str, cmd: &Document) -> R<Option<Option<Arc<Collation>>>> {
    let Some(v) = cmd.get("collation") else { return Ok(None) };
    if !matches!(v, Bson::Document(_)) && name != "find" {
        return Err(MongoError::type_mismatch(format!("BSON field '{}.collation' is the wrong type '{}', expected type 'object'", name, type_of(v))));
    }
    match parse(v) {
        Err(e) if name == "distinct" && e.code == 2 && e.msg.starts_with("Field 'locale'") => Err(MongoError::bad_value(format!("unable to parse collation :: caused by :: {}", e.msg))),
        r => r.map(Some),
    }
}

/// The collation of an update/delete statement (`updates.collation`, `deletes.collation`). A value of the wrong type fails the
/// command; an invalid collation fails only its statement (the inner error is returned for the caller to report).
pub fn of_statement(name: &str, stmt: &Document) -> R<R<Option<Option<Arc<Collation>>>>> {
    let Some(v) = stmt.get("collation") else { return Ok(Ok(None)) };
    if !matches!(v, Bson::Document(_)) {
        let (cmd, arr) = if name == "update" { ("update", "updates") } else { ("delete", "deletes") };
        return Err(MongoError::type_mismatch(format!("BSON field '{cmd}.{arr}.collation' is the wrong type '{}', expected type 'object'", type_of(v))));
    }
    Ok(parse(v).map(Some))
}

/// `collation` of `create`: an empty document is an error here (unlike in queries); a message spells the options out the way
/// mongod expands them, and the field name in a type or unknown-field error is `create.collation`.
pub fn of_create(v: &Bson) -> R<Option<Arc<Collation>>> {
    let Bson::Document(spec) = v else {
        return Err(MongoError::type_mismatch(format!("BSON field 'create.collation' is the wrong type '{}', expected type 'object'", type_of(v))));
    };
    if spec.is_empty() {
        return Err(MongoError::new(40414, "IDLFailedToParse", "BSON field 'create.collation.locale' is missing but a required field".into()));
    }
    for_collection(v).map_err(|mut e| {
        e.msg = e.msg.replace("'collation.", "'create.collation.");
        e
    })
}
