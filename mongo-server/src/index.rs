//! Secondary indexes: definitions, entry generation, and the query planner.
//!
//! Entry key: `idx/{collection}/{index name}/` + one [`keyenc::component`] per key-pattern field +
//! `{_id encoding}{u16 length of the _id encoding}`. The value is empty. Because the components are
//! prefix-free, all entries with the same indexed values share an exact byte prefix.
//!
//! An index is `Ready` (queries use it, unique is enforced on writes), `Building` (writes maintain it,
//! a backfill adds the entries of existing documents, queries ignore it) or `Dropping` (hidden, its
//! entries are being deleted).

use crate::error::MongoError;
use crate::keyenc;
use crate::layout::{escape_collection, encode_sortable};
use crate::query::{matches, path_values, type_rank};
use bson::{doc, Bson, Document};
use std::collections::{BTreeMap, BTreeSet, HashMap};

type R<T> = Result<T, MongoError>;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum State {
    Ready,
    Building,
    Dropping,
}

/// What kind of index this is; `Regular` indexes are ordered B-tree-like key lists.
#[derive(Clone, Debug)]
pub enum Kind {
    Regular,
    Text(crate::text::TextSpec),
    Geo(crate::geo_query::GeoSpec),
    /// Hashed: the position of the hashed field in the key.
    Hashed(usize),
    Wildcard(crate::wild::WildSpec),
}

#[derive(Clone, Debug)]
pub struct IndexDef {
    pub name: String,
    /// Key pattern with the direction (true = ascending).
    pub key: Vec<(String, bool)>,
    pub unique: bool,
    pub sparse: bool,
    pub partial: Option<Document>,
    pub ttl: Option<i64>,
    pub state: State,
    pub multikey: bool,
    /// Build cursor: the document key (inside the collection prefix) up to which existing documents are indexed.
    pub cursor: Option<Vec<u8>>,
    /// 2dsphere / 2d / hashed / wildcard: recorded and listed, no entries (until implemented).
    pub special: bool,
    pub kind: Kind,
    /// Text indexes: the key pattern as the user gave it (`listIndexes` shows `_fts`/`_ftsx`); internal.
    pub text_key: Option<Document>,
    /// The spec as the user gave it (plus `v`), without internal state; this is what `listIndexes` returns.
    pub spec: Document,
}

/// Numeric value of an int / long / double.
pub fn num_of(v: &Bson) -> Option<f64> {
    num(v)
}

fn num(v: &Bson) -> Option<f64> {
    match v {
        Bson::Int32(i) => Some(*i as f64),
        Bson::Int64(i) => Some(*i as f64),
        Bson::Double(f) => Some(*f),
        _ => None,
    }
}

impl IndexDef {
    /// Parse a stored spec (internal fields included).
    pub fn from_stored(stored: &Document) -> Option<IndexDef> {
        let key_doc = stored.get_document("key").ok()?;
        let mut key = Vec::new();
        let mut special = false;
        let is_text = key_doc.contains_key("_fts");
        let is_geo = key_doc.values().any(|v| matches!(v, Bson::String(s) if s == "2dsphere" || s == "2d"));
        let hashed_pos = key_doc.values().position(|v| matches!(v, Bson::String(s) if s == "hashed"));
        let wild = if is_text || is_geo { None } else { crate::wild::WildSpec::from_stored(stored) };
        for (k, v) in key_doc {
            if wild.is_some() || (hashed_pos.is_some() && matches!(v, Bson::String(s) if s == "hashed")) {
                key.push((k.clone(), wild.as_ref().map(|w| w.asc).unwrap_or(true)));
                continue;
            }
            if is_text && (k == "_fts" || k == "_ftsx") {
                continue;
            }
            if is_geo {
                key.push((k.clone(), !matches!(num(v), Some(n) if n < 0.0)));
                continue;
            }
            match num(v) {
                Some(n) => key.push((k.clone(), n >= 0.0)),
                None => {
                    special = true;
                    key.push((k.clone(), true));
                }
            }
        }
        let flag = |n: &str| matches!(stored.get(n), Some(Bson::Boolean(true))) || stored.get(n).and_then(num).map(|x| x != 0.0).unwrap_or(false);
        let state = if flag("dropping") {
            State::Dropping
        } else if flag("building") {
            State::Building
        } else {
            State::Ready
        };
        let mut spec = stored.clone();
        for internal in ["building", "dropping", "buildCursor", "multikey", "textKey"] {
            spec.remove(internal);
        }
        let text_key = stored.get_document("textKey").ok().cloned();
        let kind = if is_text {
            match crate::text::TextSpec::from_spec(stored) {
                Some(t) => Kind::Text(t),
                None => return None,
            }
        } else if is_geo {
            match crate::geo_query::GeoSpec::from_spec(stored) {
                Some(g) => Kind::Geo(g),
                None => return None,
            }
        } else if let Some(w) = wild {
            Kind::Wildcard(w)
        } else if let Some(p) = hashed_pos {
            // a hashed field next to a wildcard / unknown plugin key is only recorded
            if special { Kind::Regular } else { Kind::Hashed(p) }
        } else {
            Kind::Regular
        };
        Some(IndexDef {
            name: stored.get_str("name").ok()?.to_string(),
            key,
            unique: flag("unique"),
            sparse: flag("sparse"),
            partial: stored.get_document("partialFilterExpression").ok().cloned(),
            ttl: if key_doc.len() == 1 { stored.get("expireAfterSeconds").and_then(num).map(|f| f as i64) } else { None },
            state,
            multikey: flag("multikey"),
            cursor: match stored.get("buildCursor") {
                Some(Bson::Binary(b)) => Some(b.bytes.clone()),
                _ => None,
            },
            special,
            kind,
            text_key,
            spec,
        })
    }

    /// The document stored in `meta/indexes/{collection}` (spec plus internal state).
    pub fn to_stored(&self) -> Document {
        let mut d = self.spec.clone();
        if let Some(tk) = &self.text_key {
            d.insert("textKey", tk.clone());
        }
        if self.state == State::Building {
            d.insert("building", true);
            if let Some(c) = &self.cursor {
                d.insert("buildCursor", Bson::Binary(bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: c.clone() }));
            }
        }
        if self.state == State::Dropping {
            d.insert("dropping", true);
        }
        if self.multikey {
            d.insert("multikey", true);
        }
        d
    }

    pub fn key_pattern(&self) -> Document {
        let mut d = Document::new();
        for (f, asc) in &self.key {
            d.insert(f.clone(), if *asc { 1 } else { -1 });
        }
        if self.special || matches!(self.kind, Kind::Text(_) | Kind::Geo(_) | Kind::Hashed(_) | Kind::Wildcard(_)) {
            return self.spec.get_document("key").cloned().unwrap_or(d);
        }
        d
    }

    pub fn prefix(&self, coll: &str) -> Vec<u8> {
        let mut k = b"idx/".to_vec();
        k.extend(escape_collection(coll));
        k.push(b'/');
        k.extend(escape_collection(&self.name));
        k.push(b'/');
        k
    }

    /// Whether this index maintains entries (every state except `special`).
    pub fn has_entries(&self) -> bool {
        !self.special
    }

    /// Distinct entries of `d` as `component bytes -> {field: value}` (empty when the document is not in
    /// the index), and whether any indexed path crossed an array.
    pub fn entries(&self, d: &Document) -> R<(BTreeMap<Vec<u8>, Document>, bool)> {
        if !self.has_entries() {
            return Ok((BTreeMap::new(), false));
        }
        if let Some(p) = &self.partial {
            if !matches(d, p)? {
                return Ok((BTreeMap::new(), false));
            }
        }
        match &self.kind {
            Kind::Text(t) => return text_entries(t, d),
            Kind::Geo(g) => return geo_entries(g, d),
            Kind::Hashed(pos) => return crate::wild::hashed_entries(&self.key, *pos, self.sparse, d),
            Kind::Wildcard(w) => return Ok((w.entries(d), false)),
            Kind::Regular => {}
        }
        let mut fields: Vec<(Vec<Bson>, bool)> = Vec::new();
        let mut any_present = false;
        let mut multikey = false;
        let mut array_fields = 0;
        for (path, _) in &self.key {
            let vals = path_values(d, path);
            let mut through_array = false;
            let parts: Vec<&str> = path.split('.').collect();
            for n in 1..parts.len() {
                if path_values(d, &parts[..n].join(".")).iter().any(|v| matches!(v, Bson::Array(_))) {
                    through_array = true;
                }
            }
            if vals.is_empty() {
                fields.push((vec![Bson::Null], through_array));
                if through_array {
                    array_fields += 1;
                    multikey = true;
                }
                continue;
            }
            any_present = true;
            let mut out = Vec::new();
            let mut is_arr = through_array;
            for v in vals {
                match v {
                    Bson::Array(a) => {
                        is_arr = true;
                        if a.is_empty() {
                            out.push(Bson::Undefined);
                        } else {
                            out.extend(a.iter().cloned());
                        }
                    }
                    other => out.push(other.clone()),
                }
            }
            if is_arr {
                array_fields += 1;
                multikey = true;
            }
            fields.push((out, is_arr));
        }
        if self.sparse && !any_present {
            return Ok((BTreeMap::new(), false));
        }
        if array_fields > 1 {
            let mut names: Vec<String> = self.key.iter().zip(&fields).filter(|(_, (_, a))| *a).map(|((f, _), _)| format!("[{f}]")).collect();
            names.reverse();
            return Err(MongoError::new(171, "CannotIndexParallelArrays", format!("cannot index parallel arrays {}", names.join(" "))));
        }
        let mut acc: Vec<(Vec<u8>, Document)> = vec![(Vec::new(), Document::new())];
        for ((path, asc), (vals, _)) in self.key.iter().zip(&fields) {
            let mut comps: BTreeMap<Vec<u8>, &Bson> = BTreeMap::new();
            for v in vals {
                comps.entry(keyenc::component(v, *asc)).or_insert(v);
            }
            let mut next = Vec::with_capacity(acc.len() * comps.len());
            for (p, pd) in &acc {
                for (c, v) in &comps {
                    let mut k = p.clone();
                    k.extend(c);
                    let mut nd = pd.clone();
                    nd.insert(path.clone(), match v { Bson::Undefined => Bson::Undefined, o => (*o).clone() });
                    next.push((k, nd));
                }
            }
            acc = next;
        }
        Ok((acc.into_iter().collect(), multikey))
    }

    /// `{field: value}` pairs of the first entry of `d`, for E11000 messages.
    pub fn key_value(&self, d: &Document) -> Document {
        let mut out = Document::new();
        for (f, _) in &self.key {
            let v = path_values(d, f).first().map(|v| match v {
                Bson::Array(a) if !a.is_empty() => a[0].clone(),
                other => (*other).clone(),
            });
            out.insert(f.clone(), v.unwrap_or(Bson::Null));
        }
        out
    }
}

/// Values a key field contributes to the entries of a document (`null` when missing, the elements of an array).
fn field_values(d: &Document, path: &str) -> Vec<Bson> {
    let vals = path_values(d, path);
    if vals.is_empty() {
        return vec![Bson::Null];
    }
    let mut out = Vec::new();
    for v in vals {
        match v {
            Bson::Array(a) if a.is_empty() => out.push(Bson::Undefined),
            Bson::Array(a) => out.extend(a.iter().cloned()),
            other => out.push(other.clone()),
        }
    }
    out
}

/// Entries of a text index: prefix fields, the term, suffix fields.
fn text_entries(t: &crate::text::TextSpec, d: &Document) -> R<(BTreeMap<Vec<u8>, Document>, bool)> {
    let scores = crate::text::doc_scores(t, d)?;
    let mut acc: Vec<Vec<u8>> = vec![Vec::new()];
    let cross = |acc: Vec<Vec<u8>>, comps: Vec<Vec<u8>>| -> Vec<Vec<u8>> {
        let mut next = Vec::with_capacity(acc.len() * comps.len());
        for p in &acc {
            for c in &comps {
                let mut k = p.clone();
                k.extend(c);
                next.push(k);
            }
        }
        next
    };
    for (path, asc) in t.prefix.iter().chain(t.suffix.iter()) {
        if path_values(d, path).iter().any(|v| matches!(v, Bson::Array(_))) {
            return Err(MongoError::new(201, "CannotBuildIndexKeys", format!("Field '{path}' of text index contains an array in document: {{ _id: {} }}", d.get("_id").map(crate::update::fmt_bson).unwrap_or_default())));
        }
    }
    for (path, asc) in &t.prefix {
        let mut comps: Vec<Vec<u8>> = field_values(d, path).iter().map(|v| keyenc::component(v, *asc)).collect();
        comps.sort();
        comps.dedup();
        acc = cross(acc, comps);
    }
    let term_comps: Vec<Vec<u8>> = scores.keys().map(|term| keyenc::component(&Bson::String(term.clone()), true)).collect();
    acc = cross(acc, term_comps);
    for (path, asc) in &t.suffix {
        let mut comps: Vec<Vec<u8>> = field_values(d, path).iter().map(|v| keyenc::component(v, *asc)).collect();
        comps.sort();
        comps.dedup();
        acc = cross(acc, comps);
    }
    if scores.is_empty() {
        return Ok((BTreeMap::new(), false));
    }
    Ok((acc.into_iter().map(|k| (k, Document::new())).collect(), false))
}

/// Entries of a 2dsphere / 2d index: prefix fields, the cell token, suffix fields. The flag is set when a shape larger than
/// a point was indexed (queries then also look for coarser cells).
fn geo_entries(g: &crate::geo_query::GeoSpec, d: &Document) -> R<(BTreeMap<Vec<u8>, Document>, bool)> {
    let vals = path_values(d, &g.field);
    let (tokens, coarse): (Vec<u64>, bool) = if g.sphere {
        crate::geo_query::sphere_keys(g, d, &vals)?
    } else {
        (crate::geo_query::plane_keys(g, d, &vals)?, false)
    };
    if tokens.is_empty() {
        return Ok((BTreeMap::new(), false));
    }
    let comp = |v: &Bson, asc: bool| keyenc::component(v, asc);
    let mut acc: Vec<Vec<u8>> = vec![Vec::new()];
    let cross = |acc: Vec<Vec<u8>>, comps: Vec<Vec<u8>>| -> Vec<Vec<u8>> {
        let mut next = Vec::with_capacity(acc.len() * comps.len());
        for p in &acc {
            for c in &comps {
                let mut k = p.clone();
                k.extend(c);
                next.push(k);
            }
        }
        next
    };
    for (path, asc) in &g.prefix {
        let mut comps: Vec<Vec<u8>> = field_values(d, path).iter().map(|v| comp(v, *asc)).collect();
        comps.sort();
        comps.dedup();
        acc = cross(acc, comps);
    }
    acc = cross(acc, tokens.iter().map(|t| comp(&Bson::String(crate::geo_query::token(*t)), true)).collect());
    for (path, asc) in &g.suffix {
        let mut comps: Vec<Vec<u8>> = field_values(d, path).iter().map(|v| comp(v, *asc)).collect();
        comps.sort();
        comps.dedup();
        acc = cross(acc, comps);
    }
    Ok((acc.into_iter().map(|k| (k, Document::new())).collect(), coarse))
}

/// Key of one entry.
pub fn entry_key(prefix: &[u8], comps: &[u8], id_enc: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(prefix.len() + comps.len() + id_enc.len() + 2);
    k.extend(prefix);
    k.extend(comps);
    k.extend(id_enc);
    k.extend((id_enc.len() as u16).to_be_bytes());
    k
}

/// The `_id` encoding at the tail of an entry key.
pub fn entry_id(key: &[u8]) -> Option<&[u8]> {
    if key.len() < 2 {
        return None;
    }
    let n = u16::from_be_bytes([key[key.len() - 2], key[key.len() - 1]]) as usize;
    if key.len() < 2 + n {
        return None;
    }
    Some(&key[key.len() - 2 - n..key.len() - 2])
}

/// The key-component bytes of an entry key (between `prefix` and the `_id` tail).
pub fn entry_comps<'a>(key: &'a [u8], prefix: &[u8]) -> Option<&'a [u8]> {
    let id = entry_id(key)?;
    let end = key.len() - 2 - id.len();
    if end < prefix.len() {
        return None;
    }
    Some(&key[prefix.len()..end])
}

// ---------------------------------------------------------------------------
// Planner
// ---------------------------------------------------------------------------

#[derive(Default, Clone, Debug)]
pub struct Constraint {
    /// Alternatives from equality / `$in` (all must be indexable scalars or documents).
    pub eq: Option<Vec<Bson>>,
    pub lo: Option<(Bson, bool)>,
    pub hi: Option<(Bson, bool)>,
    pub exists: bool,
    pub exists_false: bool,
    /// `$ne` / `$nin` scalar operands.
    pub ne: Vec<Bson>,
    /// Anchored literal prefix of a `$regex` / regex literal, and the regex value itself.
    pub regex_prefix: Option<(String, Bson)>,
}

fn bracketable(v: &Bson) -> bool {
    match v {
        Bson::Null | Bson::Undefined | Bson::MinKey | Bson::MaxKey | Bson::Array(_) | Bson::RegularExpression(_) => false,
        Bson::Double(f) => !f.is_nan(),
        _ => true,
    }
}

fn eq_ok(v: &Bson) -> bool {
    !matches!(v, Bson::Array(_) | Bson::RegularExpression(_) | Bson::Undefined)
}

fn is_op_doc(d: &Document) -> bool {
    !d.is_empty() && d.keys().all(|k| k.starts_with('$'))
}

fn add_conjunct(out: &mut HashMap<String, Constraint>, path: &str, cond: &Bson) {
    let c = out.entry(path.to_string()).or_default();
    match cond {
        Bson::Document(d) if is_op_doc(d) => {
            for (op, arg) in d {
                match op.as_str() {
                    "$eq" if eq_ok(arg) => c.eq = Some(vec![arg.clone()]),
                    "$in" => {
                        if let Bson::Array(a) = arg {
                            if a.iter().all(eq_ok) && !a.is_empty() {
                                c.eq = Some(a.clone());
                            }
                        }
                    }
                    "$gt" | "$gte" if bracketable(arg) => c.lo = Some((arg.clone(), op == "$gte")),
                    "$lt" | "$lte" if bracketable(arg) => c.hi = Some((arg.clone(), op == "$lte")),
                    "$exists" => {
                        if matches!(arg, Bson::Boolean(true)) || num(arg).map(|n| n != 0.0).unwrap_or(false) {
                            c.exists = true;
                        } else {
                            c.exists_false = true;
                        }
                    }
                    "$ne" if eq_ok(arg) && !matches!(arg, Bson::Document(_)) && !matches!(arg, Bson::Double(f) if f.is_nan()) => c.ne = vec![arg.clone()],
                    "$nin" => {
                        if let Bson::Array(a) = arg {
                            if !a.is_empty() && a.iter().all(|v| eq_ok(v) && !matches!(v, Bson::Document(_)) && !matches!(v, Bson::Double(f) if f.is_nan())) {
                                c.ne = a.clone();
                            }
                        }
                    }
                    "$regex" => {
                        let opts = d.get("$options").and_then(|o| o.as_str()).unwrap_or("");
                        match arg {
                            Bson::String(p) => c.regex_prefix = regex_prefix(p, opts),
                            Bson::RegularExpression(r) => c.regex_prefix = regex_prefix(r.pattern.as_str(), if opts.is_empty() { r.options.as_str() } else { opts }),
                            _ => {}
                        }
                    }
                    _ => {}
                }
            }
        }
        Bson::RegularExpression(r) => c.regex_prefix = regex_prefix(r.pattern.as_str(), r.options.as_str()),
        other if eq_ok(other) => c.eq = Some(vec![other.clone()]),
        _ => {}
    }
}

/// Literal prefix of a `^`-anchored regex without case folding or multiline mode.
fn regex_prefix(pattern: &str, options: &str) -> Option<(String, Bson)> {
    if options.chars().any(|c| matches!(c, 'i' | 'm' | 'x')) || !pattern.starts_with('^') || pattern.contains('|') {
        return None;
    }
    let mut prefix = String::new();
    let rest: Vec<char> = pattern[1..].chars().collect();
    let mut i = 0;
    while i < rest.len() {
        let c = rest[i];
        if "\\.[](){}*+?^$|".contains(c) {
            if c == '\\' && i + 1 < rest.len() && !rest[i + 1].is_alphanumeric() {
                prefix.push(rest[i + 1]);
                i += 2;
                continue;
            }
            break;
        }
        // a quantifier makes the previous literal optional
        if matches!(rest.get(i + 1), Some('*') | Some('?') | Some('{')) {
            break;
        }
        prefix.push(c);
        i += 1;
    }
    if prefix.is_empty() {
        return None;
    }
    let re = Bson::RegularExpression(bson::Regex { pattern: pattern.to_string().try_into().ok()?, options: bson::raw::CString::try_from(options.to_string()).ok()? });
    Some((prefix, re))
}

/// Per-field constraints implied by the AND-level conjuncts of `filter` (top-level fields and `$and`).
pub fn constraints(filter: &Document) -> HashMap<String, Constraint> {
    let mut out = HashMap::new();
    collect(filter, &mut out);
    out.retain(|_, c| c.eq.is_some() || c.lo.is_some() || c.hi.is_some() || c.exists || c.exists_false || !c.ne.is_empty() || c.regex_prefix.is_some());
    out
}

fn collect(filter: &Document, out: &mut HashMap<String, Constraint>) {
    for (k, v) in filter {
        match k.as_str() {
            "$and" => {
                if let Bson::Array(a) = v {
                    for s in a {
                        if let Bson::Document(d) = s {
                            collect(d, out);
                        }
                    }
                }
            }
            k if k.starts_with('$') => {}
            path => add_conjunct(out, path, v),
        }
    }
}

/// Byte intervals relative to the index prefix: `[start, end)`, `end == None` is the end of the index.
pub type Interval = (Vec<u8>, Option<Vec<u8>>);

#[derive(Clone, Debug)]
pub struct IxScan {
    pub idx: usize,
    pub intervals: Vec<Interval>,
    pub reverse: bool,
    /// Index order satisfies the requested sort (and no residual in-memory sort is needed).
    pub sorted: bool,
    /// Number of key fields that carry a bound.
    pub bounded_fields: usize,
    /// Human-readable bounds per key field, as `explain` prints them.
    pub bounds: Vec<(String, Vec<String>)>,
}

#[derive(Clone, Debug)]
pub enum Plan {
    Coll { reverse: bool },
    Id(Bson),
    Ix(IxScan),
    Or(Vec<Plan>),
}

impl Plan {
    pub fn is_indexed(&self) -> bool {
        !matches!(self, Plan::Coll { .. })
    }
}

/// Endpoint of a value-space interval.
enum Ep {
    /// Unbounded (not limited to the type bracket).
    Open,
    /// The edge of the type bracket of `rank`.
    Bracket,
    Val(Vec<u8>, bool),
}

struct VI {
    lo: Ep,
    hi: Ep,
    rank: u8,
    lo_s: String,
    hi_s: String,
    lo_incl: bool,
    hi_incl: bool,
}

impl VI {
    /// Byte range of this interval on a key component of the given direction, and its `explain` label.
    fn map_dir(&self, asc: bool) -> Option<(Vec<u8>, Option<Vec<u8>>, String)> {
        let rank = self.rank;
        if asc {
            let start = match &self.lo {
                Ep::Open => Vec::new(),
                Ep::Bracket => vec![rank],
                Ep::Val(b, incl) => if *incl { b.clone() } else { keyenc::prefix_end(b)? },
            };
            let end = match &self.hi {
                Ep::Open => None,
                Ep::Bracket => keyenc::prefix_end(&[rank]),
                Ep::Val(b, incl) => if *incl { keyenc::prefix_end(b) } else { Some(b.clone()) },
            };
            let label = format!("{}{}, {}{}", if self.lo_incl { "[" } else { "(" }, self.lo_s, self.hi_s, if self.hi_incl { "]" } else { ")" });
            Some((start, end, label))
        } else {
            let start = match &self.hi {
                Ep::Open => Vec::new(),
                Ep::Bracket => vec![!rank],
                Ep::Val(b, incl) => {
                    let mut x = b.clone();
                    keyenc::invert(&mut x);
                    if *incl { x } else { keyenc::prefix_end(&x)? }
                }
            };
            let end = match &self.lo {
                Ep::Open => None,
                Ep::Bracket => keyenc::prefix_end(&[!rank]),
                Ep::Val(b, incl) => {
                    let mut x = b.clone();
                    keyenc::invert(&mut x);
                    if *incl { keyenc::prefix_end(&x) } else { Some(x) }
                }
            };
            let label = format!("{}{}, {}{}", if self.hi_incl { "[" } else { "(" }, self.hi_s, self.lo_s, if self.lo_incl { "]" } else { ")" });
            Some((start, end, label))
        }
    }
}

fn val_ep(v: &Bson) -> (Ep, String) {
    let b = keyenc::encode(v);
    (Ep::Val(b, true), fmt_bound(v))
}

/// Value-space intervals implied by a non-equality constraint (a range, regex prefix, `$ne`/`$nin`, `$exists: false`).
fn value_intervals(c: &Constraint, multikey: bool) -> Vec<VI> {
    let (lo, hi) = (&c.lo, &c.hi);
    if lo.is_some() || hi.is_some() {
        let (lo, hi) = match (lo, hi) {
            (Some((l, _)), Some((h, _))) if type_rank(l) != type_rank(h) => (lo, &None),
            _ => (lo, hi),
        };
        let (lo, hi) = if multikey && lo.is_some() && hi.is_some() { (lo, &None) } else { (lo, hi) };
        let rank = type_rank(&lo.as_ref().or(hi.as_ref()).unwrap().0);
        let (lo_ep, lo_s, lo_i) = match lo {
            Some((v, i)) => (Ep::Val(keyenc::encode(v), *i), fmt_bound(v), *i),
            None => (Ep::Bracket, min_of_bracket(rank), true),
        };
        let (hi_ep, hi_s, hi_i) = match hi {
            Some((v, i)) => (Ep::Val(keyenc::encode(v), *i), fmt_bound(v), *i),
            None => (Ep::Bracket, max_of_bracket(rank), true),
        };
        return vec![VI { lo: lo_ep, hi: hi_ep, rank, lo_s, hi_s, lo_incl: lo_i, hi_incl: hi_i }];
    }
    if let Some((prefix, re)) = &c.regex_prefix {
        let mut e = vec![4u8];
        for &b in prefix.as_bytes() {
            e.push(b);
            if b == 0 {
                e.push(0xFF);
            }
        }
        let mut hi_label = prefix.clone();
        if let Some(last) = hi_label.pop() {
            hi_label.push(char::from_u32(last as u32 + 1).unwrap_or(last));
        }
        let (re_ep, re_s) = val_ep(re);
        let Ep::Val(re_b, _) = &re_ep else { unreachable!() };
        return vec![
            VI { lo: Ep::Val(keyenc::encode(&Bson::String(prefix.clone())), true), hi: match keyenc::prefix_end(&e) { Some(x) => Ep::Val(x, false), None => Ep::Open }, rank: 4, lo_s: format!("\"{prefix}\""), hi_s: format!("\"{hi_label}\""), lo_incl: true, hi_incl: false },
            VI { lo: Ep::Val(re_b.clone(), true), hi: Ep::Val(re_b.clone(), true), rank: 12, lo_s: re_s.clone(), hi_s: re_s, lo_incl: true, hi_incl: true },
        ];
    }
    if !c.ne.is_empty() {
        let mut vals = c.ne.clone();
        vals.sort_by(crate::query::compare);
        vals.dedup_by(|a, b| crate::query::equal(a, b));
        let mut out = Vec::new();
        let n = vals.len();
        for i in 0..=n {
            let (lo, lo_s, lo_i) = if i == 0 { (Ep::Open, "MinKey".to_string(), true) } else { (Ep::Val(keyenc::encode(&vals[i - 1]), false), fmt_bound(&vals[i - 1]), false) };
            let (hi, hi_s, hi_i) = if i == n { (Ep::Open, "MaxKey".to_string(), true) } else { (Ep::Val(keyenc::encode(&vals[i]), false), fmt_bound(&vals[i]), false) };
            out.push(VI { lo, hi, rank: 1, lo_s, hi_s, lo_incl: lo_i, hi_incl: hi_i });
        }
        return out;
    }
    if c.exists_false {
        let (ep, s) = val_ep(&Bson::Null);
        let Ep::Val(b, _) = &ep else { unreachable!() };
        return vec![VI { lo: Ep::Val(b.clone(), true), hi: Ep::Val(b.clone(), true), rank: 2, lo_s: s.clone(), hi_s: s, lo_incl: true, hi_incl: true }];
    }
    Vec::new()
}

fn min_of_bracket(rank: u8) -> String {
    match rank {
        3 => "-inf.0".into(),
        4 => "\"\"".into(),
        _ => "MinKey".into(),
    }
}

fn max_of_bracket(rank: u8) -> String {
    match rank {
        3 => "inf.0".into(),
        4 => "{}".into(),
        _ => "MaxKey".into(),
    }
}

pub fn fmt_bound(v: &Bson) -> String {
    match v {
        Bson::Int32(i) => i.to_string(),
        Bson::Int64(i) => i.to_string(),
        Bson::Double(f) => {
            if f.is_infinite() { if *f > 0.0 { "inf.0".into() } else { "-inf.0".into() } } else { f.to_string() }
        }
        Bson::String(s) => format!("\"{s}\""),
        Bson::Null => "null".into(),
        Bson::Undefined => "undefined".into(),
        Bson::RegularExpression(r) => format!("/{}/{}", r.pattern.as_str(), r.options.as_str()),
        Bson::MinKey => "MinKey".into(),
        Bson::MaxKey => "MaxKey".into(),
        Bson::Boolean(b) => b.to_string(),
        Bson::ObjectId(o) => format!("ObjectId('{}')", o.to_hex()),
        Bson::DateTime(d) => {
            let s = d.try_to_rfc3339_string().unwrap_or_default();
            format!("new Date({})", if s.is_empty() { d.timestamp_millis().to_string() } else { d.timestamp_millis().to_string() })
        }
        other => crate::update::fmt_bson(other),
    }
}

fn eq_label(v: &Bson) -> String {
    format!("[{0}, {0}]", fmt_bound(v))
}

fn merge_intervals(mut v: Vec<Interval>) -> Vec<Interval> {
    v.sort();
    let mut out: Vec<Interval> = Vec::new();
    for (s, e) in v {
        if let Some(last) = out.last_mut() {
            let overlaps = match &last.1 {
                None => true,
                Some(le) => s <= *le,
            };
            if overlaps {
                last.1 = match (&last.1, &e) {
                    (None, _) | (_, None) => None,
                    (Some(a), Some(b)) => Some(if a >= b { a.clone() } else { b.clone() }),
                };
                continue;
            }
        }
        out.push((s, e));
    }
    out
}

/// Bounds of `idx` for the constraints; `None` when no key field is constrained.
fn bounds_for(idx: &IndexDef, cs: &HashMap<String, Constraint>) -> Option<(Vec<Interval>, usize, Vec<(String, Vec<String>)>)> {
    let mut prefixes: Vec<Vec<u8>> = vec![Vec::new()];
    let mut bounded = 0;
    let mut labels: Vec<(String, Vec<String>)> = Vec::new();
    let mut ranged: Option<Vec<Interval>> = None;
    for (field, asc) in &idx.key {
        let c = cs.get(field);
        if let Some(eq) = c.and_then(|c| c.eq.as_ref()) {
            let undefined = Bson::Undefined;
            let mut comps: Vec<(Vec<u8>, &Bson)> = Vec::new();
            for v in eq {
                comps.push((keyenc::component(v, *asc), v));
                if matches!(v, Bson::Null) {
                    comps.push((keyenc::component(&undefined, *asc), &undefined));
                }
            }
            comps.sort_by(|a, b| a.0.cmp(&b.0));
            comps.dedup_by(|a, b| a.0 == b.0);
            let mut next = Vec::new();
            for p in &prefixes {
                for (c, _) in &comps {
                    let mut k = p.clone();
                    k.extend(c);
                    next.push(k);
                }
            }
            prefixes = next;
            bounded += 1;
            labels.push((field.clone(), comps.iter().map(|(_, v)| eq_label(v)).collect()));
            continue;
        }
        let mut trip: Vec<(Vec<u8>, Option<Vec<u8>>, String)> = c.map(|c| value_intervals(c, idx.multikey)).unwrap_or_default().iter().filter_map(|vi| vi.map_dir(*asc)).collect();
        if !trip.is_empty() {
            trip.sort();
            let mut ivs = Vec::new();
            for p in &prefixes {
                for (s, e, _) in &trip {
                    let mut st = p.clone();
                    st.extend(s);
                    let en = match e {
                        Some(e) => {
                            let mut x = p.clone();
                            x.extend(e);
                            Some(x)
                        }
                        None => keyenc::prefix_end(p),
                    };
                    ivs.push((st, en));
                }
            }
            ranged = Some(ivs);
            bounded += 1;
            labels.push((field.clone(), trip.into_iter().map(|t| t.2).collect()));
        }
        break;
    }
    if bounded == 0 {
        return None;
    }
    let ivs = match ranged {
        Some(i) => i,
        None => prefixes.iter().map(|p| (p.clone(), keyenc::prefix_end(p))).collect(),
    };
    // Unconstrained trailing fields.
    for (field, asc) in idx.key.iter().skip(labels.len()) {
        labels.push((field.clone(), vec![if *asc { "[MinKey, MaxKey]".into() } else { "[MaxKey, MinKey]".into() }]));
    }
    Some((merge_intervals(ivs), bounded, labels))
}

/// Does the query guarantee that every matching document is in the (sparse/partial) index?
fn index_covers_query(idx: &IndexDef, cs: &HashMap<String, Constraint>) -> bool {
    if idx.sparse {
        let implies_exists = idx.key.iter().any(|(f, _)| {
            cs.get(f).map(|c| c.exists || c.lo.is_some() || c.hi.is_some() || c.eq.as_ref().map(|e| e.iter().all(|v| !matches!(v, Bson::Null))).unwrap_or(false)).unwrap_or(false)
        });
        if !implies_exists {
            return false;
        }
    }
    if let Some(p) = &idx.partial {
        let pcs = constraints(p);
        // every conjunct of the partial filter must be implied by the query
        let simple = p.keys().all(|k| !k.starts_with('$') || k == "$and");
        if !simple {
            return false;
        }
        for (f, pc) in &pcs {
            let Some(qc) = cs.get(f) else { return false };
            if pc.exists && !(qc.exists || qc.eq.is_some() || qc.lo.is_some() || qc.hi.is_some()) {
                return false;
            }
            if let Some(pe) = &pc.eq {
                let Some(qe) = &qc.eq else { return false };
                if !qe.iter().all(|v| pe.iter().any(|p| crate::query::equal(p, v))) {
                    return false;
                }
            }
            if let Some((pl, pincl)) = &pc.lo {
                let ok = qc.lo.as_ref().map(|(ql, qincl)| match crate::query::compare(ql, pl) {
                    std::cmp::Ordering::Greater => true,
                    std::cmp::Ordering::Equal => *pincl || !*qincl,
                    _ => false,
                }).unwrap_or(false)
                    || qc.eq.as_ref().map(|e| e.iter().all(|v| type_rank(v) == type_rank(pl) && match crate::query::compare(v, pl) { std::cmp::Ordering::Greater => true, std::cmp::Ordering::Equal => *pincl, _ => false })).unwrap_or(false);
                if !ok {
                    return false;
                }
            }
            if let Some((ph, pincl)) = &pc.hi {
                let ok = qc.hi.as_ref().map(|(qh, qincl)| match crate::query::compare(qh, ph) {
                    std::cmp::Ordering::Less => true,
                    std::cmp::Ordering::Equal => *pincl || !*qincl,
                    _ => false,
                }).unwrap_or(false)
                    || qc.eq.as_ref().map(|e| e.iter().all(|v| type_rank(v) == type_rank(ph) && match crate::query::compare(v, ph) { std::cmp::Ordering::Less => true, std::cmp::Ordering::Equal => *pincl, _ => false })).unwrap_or(false);
                if !ok {
                    return false;
                }
            }
        }
        // anything in the partial filter we did not model (e.g. $type) makes the answer "no"
        let modelled: usize = pcs.len();
        if modelled == 0 {
            return false;
        }
        if p.iter().any(|(k, v)| k != "$and" && matches!(v, Bson::Document(d) if d.keys().any(|o| !matches!(o.as_str(), "$eq" | "$gt" | "$gte" | "$lt" | "$lte" | "$exists" | "$in")))) {
            return false;
        }
    }
    true
}

/// Does the index order produce `sort`? Equality-constrained single-valued prefix fields are skipped.
fn sort_satisfied(idx: &IndexDef, cs: &HashMap<String, Constraint>, sort: &[(String, bool)]) -> Option<bool> {
    if sort.is_empty() || idx.multikey {
        return None;
    }
    let mut i = 0;
    let mut forward: Option<bool> = None;
    for (field, asc) in &idx.key {
        if i == sort.len() {
            break;
        }
        let single_eq = cs.get(field).and_then(|c| c.eq.as_ref()).map(|e| e.len() == 1).unwrap_or(false);
        if sort[i].0 == *field {
            let same = sort[i].1 == *asc;
            match forward {
                None => forward = Some(same),
                Some(f) if f != same => return None,
                _ => {}
            }
            i += 1;
        } else if single_eq && i == 0 {
            continue;
        } else if single_eq && forward.is_some() {
            return None;
        } else {
            return None;
        }
    }
    if i == sort.len() {
        Some(forward.unwrap_or(true))
    } else {
        None
    }
}

pub struct PlanInput<'a> {
    pub indexes: &'a [IndexDef],
    pub filter: &'a Document,
    pub sort: &'a [(String, bool)],
    pub has_limit: bool,
    pub hint: Option<usize>,
    /// Hint was `{$natural: ±1}`.
    pub natural: Option<bool>,
}

/// Choose how to find candidate documents. The caller re-applies the full filter to every candidate,
/// so a plan only has to return a superset of the matching documents.
pub fn plan(inp: &PlanInput) -> Plan {
    if let Some(asc) = inp.natural {
        return Plan::Coll { reverse: !asc };
    }
    let cs = constraints(inp.filter);
    if inp.hint.is_none() {
        if let Some(id) = crate::query::id_equality(inp.filter) {
            return Plan::Id(id.clone());
        }
    }
    if let Some(Bson::Document(op)) = crate::text::find_operand(inp.filter) {
        if let Some(scan) = text_scan(inp.indexes, op, &cs, inp.hint) {
            return Plan::Ix(scan);
        }
    }
    if let Some(scan) = geo_scan(inp.indexes, inp.filter, &cs, inp.hint) {
        return Plan::Ix(scan);
    }
    let usable = |i: usize, idx: &IndexDef| idx.state == State::Ready && !idx.special && matches!(idx.kind, Kind::Regular | Kind::Hashed(_)) && inp.hint.map(|h| h == i).unwrap_or(true);
    let mut best: Option<(i64, IxScan)> = None;
    for (i, idx) in inp.indexes.iter().enumerate() {
        if !usable(i, idx) {
            continue;
        }
        let covers = inp.hint.is_some() || index_covers_query(idx, &cs);
        if !covers {
            continue;
        }
        let hashed_cs;
        let (b, sorted_dir) = match &idx.kind {
            Kind::Hashed(pos) => {
                hashed_cs = crate::wild::hashed_constraints(&cs, &idx.key[*pos].0);
                (bounds_for(idx, &hashed_cs), None)
            }
            _ => (bounds_for(idx, &cs), sort_satisfied(idx, &cs, inp.sort)),
        };
        if b.is_none() && sorted_dir.is_none() && inp.hint.is_none() {
            continue;
        }
        if b.is_none() && sorted_dir.is_some() && !inp.has_limit && inp.hint.is_none() {
            continue; // reading the whole index to avoid a sort is only worth it with a limit
        }
        let (intervals, bounded, bounds) = match b {
            Some(x) => x,
            None => (vec![(Vec::new(), None)], 0, idx.key.iter().map(|(f, asc)| (f.clone(), vec![if *asc { "[MinKey, MaxKey]".to_string() } else { "[MaxKey, MinKey]".to_string() }])).collect()),
        };
        let reverse = sorted_dir.map(|f| !f).unwrap_or(false);
        let mut score = (bounded as i64) * 100 + if sorted_dir.is_some() { 30 } else { 0 };
        if idx.unique && bounded == idx.key.len() {
            score += 50;
        }
        score -= idx.key.len() as i64;
        let scan = IxScan { idx: i, intervals, reverse, sorted: sorted_dir.is_some(), bounded_fields: bounded, bounds };
        if best.as_ref().map(|(s, _)| score > *s).unwrap_or(true) {
            best = Some((score, scan));
        }
    }
    if let Some((_, s)) = best {
        return Plan::Ix(s);
    }
    if let Some(scan) = wild_scan(inp.indexes, inp.filter, &cs, inp.hint) {
        return Plan::Ix(scan);
    }
    // $or: every clause must be answered by an index.
    if inp.hint.is_none() {
        if let Some(Bson::Array(clauses)) = inp.filter.get("$or") {
            let mut subs = Vec::new();
            for c in clauses {
                let Bson::Document(cd) = c else { return Plan::Coll { reverse: false } };
                let p = plan(&PlanInput { indexes: inp.indexes, filter: cd, sort: &[], has_limit: false, hint: None, natural: None });
                if !p.is_indexed() {
                    subs.clear();
                    break;
                }
                subs.push(p);
            }
            if !subs.is_empty() {
                return Plan::Or(subs);
            }
        }
    }
    Plan::Coll { reverse: false }
}

/// The first field of `filter` (top level or inside `$and`) that carries geo conditions.
fn find_geo_field(filter: &Document) -> Option<(String, Vec<crate::geo_query::Cond>)> {
    for (k, v) in filter {
        match (k.as_str(), v) {
            ("$and", Bson::Array(a)) => {
                for s in a {
                    if let Bson::Document(sd) = s {
                        if let Some(x) = find_geo_field(sd) {
                            return Some(x);
                        }
                    }
                }
            }
            (k2, Bson::Document(d)) if !k2.starts_with('$') && crate::geo_query::has_geo_cond(d) => {
                if let Ok(conds) = crate::geo_query::parse_field_conds(d) {
                    if !conds.is_empty() {
                        return Some((k.clone(), conds));
                    }
                }
            }
            _ => {}
        }
    }
    None
}

/// The index scan that answers geo conditions with a 2dsphere or 2d index on the queried field.
pub fn geo_scan(indexes: &[IndexDef], filter: &Document, cs: &HashMap<String, Constraint>, hint: Option<usize>) -> Option<IxScan> {
    use crate::geo_query::{cover, plane_query_bbox, plane_ranges, sphere_query_shape, sphere_ranges, token, Cond};
    let (path, conds) = find_geo_field(filter)?;
    let (i, g, idx) = indexes.iter().enumerate().find_map(|(i, x)| match &x.kind {
        Kind::Geo(g) if x.state == State::Ready && g.field == path && hint.map(|h| h == i).unwrap_or(true) => Some((i, g, x)),
        _ => None,
    })?;
    let mut prefixes: Vec<Vec<u8>> = vec![Vec::new()];
    for (p, asc) in &g.prefix {
        let vals = cs.get(p).and_then(|c| c.eq.clone())?;
        let mut next = Vec::new();
        for pre in &prefixes {
            for v in &vals {
                let mut k = pre.clone();
                k.extend(keyenc::component(v, *asc));
                next.push(k);
            }
        }
        prefixes = next;
    }
    // the condition that bounds the scan: a near query, else a region
    let cond = conds.iter().find(|c| matches!(c, Cond::Near(_))).or_else(|| conds.first())?;
    let ranges: Option<Vec<(u64, u64)>> = if g.sphere {
        match sphere_query_shape(cond) {
            Some(shape) => Some(sphere_ranges(&cover(&shape, 20, 0, 24), g.coarsest, idx.multikey)),
            None => None,
        }
    } else {
        match (cond, plane_query_bbox(cond)) {
            (Cond::Intersects(_), _) => None,
            (_, Some((lo, hi))) => Some(plane_ranges(g, lo, hi)),
            _ => None,
        }
    };
    let mut intervals: Vec<Interval> = Vec::new();
    for p in &prefixes {
        match &ranges {
            Some(rs) => {
                for (lo, hi) in rs {
                    let mut start = p.clone();
                    start.extend(keyenc::component(&Bson::String(token(*lo)), true));
                    let mut hi_key = p.clone();
                    hi_key.extend(keyenc::component(&Bson::String(token(*hi)), true));
                    intervals.push((start, keyenc::prefix_end(&hi_key)));
                }
            }
            None => intervals.push((p.clone(), keyenc::prefix_end(p))),
        }
    }
    intervals.sort();
    Some(IxScan { idx: i, intervals, reverse: false, sorted: false, bounded_fields: g.prefix.len() + 1, bounds: vec![(path, vec!["[geo]".to_string()])] })
}

/// The index scan that answers a `$text` condition: one interval per (prefix value, term). `None` when there is no
/// text index or a prefix field has no equality (the caller reports the error).
pub fn text_scan(indexes: &[IndexDef], operand: &Document, cs: &HashMap<String, Constraint>, hint: Option<usize>) -> Option<IxScan> {
    let (i, idx, t) = indexes.iter().enumerate().find_map(|(i, x)| match &x.kind {
        Kind::Text(t) if x.state == State::Ready && hint.map(|h| h == i).unwrap_or(true) => Some((i, x, t)),
        _ => None,
    })?;
    let _ = idx;
    let mut prefixes: Vec<Vec<u8>> = vec![Vec::new()];
    for (path, asc) in &t.prefix {
        let vals = cs.get(path).and_then(|c| c.eq.clone()).filter(|v| v.len() == 1)?;
        let mut next = Vec::new();
        for p in &prefixes {
            for v in &vals {
                let mut k = p.clone();
                k.extend(keyenc::component(v, *asc));
                next.push(k);
            }
        }
        prefixes = next;
    }
    let terms = crate::text::lookup_terms(&Bson::Document(operand.clone()), t).ok()?;
    let mut intervals: Vec<Interval> = Vec::new();
    for p in &prefixes {
        for term in &terms {
            let mut start = p.clone();
            start.extend(keyenc::component(&Bson::String(term.clone()), true));
            let end = keyenc::prefix_end(&start);
            intervals.push((start, end));
        }
    }
    intervals.sort();
    Some(IxScan { idx: i, intervals, reverse: false, sorted: false, bounded_fields: t.prefix.len() + 1, bounds: vec![("_fts".to_string(), terms.iter().map(|x| format!("[\"{x}\", \"{x}\"]")).collect())] })
}

/// Collection-level helper: the doc key of an `_id` encoding.
pub fn id_enc(id: &Bson) -> Vec<u8> {
    encode_sortable(id)
}

/// A single-field TTL index: documents whose `field` date plus `secs` has passed are expired.
#[derive(Clone, Debug)]
pub struct TtlSpec {
    pub field: String,
    pub secs: i64,
    /// Only documents matching a partial TTL index's filter expire.
    pub partial: Option<Document>,
}

pub fn ttl_specs(indexes: &[IndexDef]) -> Vec<TtlSpec> {
    indexes
        .iter()
        .filter(|i| i.state == State::Ready && i.ttl.is_some() && i.key.len() == 1)
        .map(|i| TtlSpec { field: i.key[0].0.clone(), secs: i.ttl.unwrap(), partial: i.partial.clone() })
        .collect()
}

/// Is the document past its expiry for any TTL index? Dates in arrays use the earliest date.
pub fn is_expired(d: &Document, specs: &[TtlSpec], now_ms: i64) -> bool {
    for sp in specs {
        let mut earliest: Option<i64> = None;
        for v in path_values(d, &sp.field) {
            let mut take = |x: &Bson| {
                if let Bson::DateTime(t) = x {
                    let ms = t.timestamp_millis();
                    earliest = Some(earliest.map(|e| e.min(ms)).unwrap_or(ms));
                }
            };
            match v {
                Bson::Array(a) => a.iter().for_each(&mut take),
                other => take(other),
            }
        }
        if let Some(ms) = earliest {
            if ms.saturating_add(sp.secs.saturating_mul(1000)) <= now_ms {
                if let Some(p) = &sp.partial {
                    if !matches(d, p).unwrap_or(false) {
                        continue;
                    }
                }
                return true;
            }
        }
    }
    false
}

pub fn index_summary(idx: &IndexDef) -> Document {
    doc! { "name": idx.name.clone() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    fn def(key: Document, extra: Document) -> IndexDef {
        let mut s = doc! { "v": 2, "key": key, "name": "i" };
        for (k, v) in extra {
            s.insert(k, v);
        }
        IndexDef::from_stored(&s).unwrap()
    }

    #[test]
    fn multikey_entries_dedupe_and_flag() {
        let i = def(doc! { "a": 1 }, doc! {});
        let (e, mk) = i.entries(&doc! { "_id": 1, "a": [1, 1, 2] }).unwrap();
        assert_eq!(e.len(), 2);
        assert!(mk);
    }

    #[test]
    fn parallel_arrays_rejected() {
        let i = def(doc! { "a": 1, "b": 1 }, doc! {});
        assert!(i.entries(&doc! { "a": [1], "b": [2] }).is_err());
        assert_eq!(i.entries(&doc! { "a": [1, 2], "b": 3 }).unwrap().0.len(), 2);
    }

    #[test]
    fn sparse_and_partial_skip_documents() {
        let s = def(doc! { "a": 1 }, doc! { "sparse": true });
        assert!(s.entries(&doc! { "b": 1 }).unwrap().0.is_empty());
        assert_eq!(s.entries(&doc! { "a": Bson::Null }).unwrap().0.len(), 1);
        let p = def(doc! { "a": 1 }, doc! { "partialFilterExpression": { "b": { "$gt": 5 } } });
        assert!(p.entries(&doc! { "a": 1, "b": 1 }).unwrap().0.is_empty());
        assert_eq!(p.entries(&doc! { "a": 1, "b": 9 }).unwrap().0.len(), 1);
    }

    #[test]
    fn equality_prefix_then_range() {
        let i = def(doc! { "a": 1, "b": -1 }, doc! {});
        let cs = constraints(&doc! { "a": 5, "b": { "$gt": 1, "$lt": 9 } });
        let (iv, bounded, _) = bounds_for(&i, &cs).unwrap();
        assert_eq!(bounded, 2);
        assert_eq!(iv.len(), 1);
        let (e, _) = i.entries(&doc! { "a": 5, "b": 4 }).unwrap();
        let k = e.into_iter().next().unwrap().0;
        assert!(iv.iter().any(|(s, en)| k >= *s && en.as_ref().map(|en| k < *en).unwrap_or(true)));
        let (e, _) = i.entries(&doc! { "a": 5, "b": 12 }).unwrap();
        let k = e.into_iter().next().unwrap().0;
        assert!(!iv.iter().any(|(s, en)| k >= *s && en.as_ref().map(|en| k < *en).unwrap_or(true)));
    }

    #[test]
    fn entry_id_roundtrip() {
        let k = entry_key(b"p/", b"cc", &[7, 8, 9]);
        assert_eq!(entry_id(&k).unwrap(), &[7, 8, 9]);
        assert_eq!(entry_comps(&k, b"p/").unwrap(), b"cc");
    }
}

/// A scan of a wildcard index for `filter`: the entries of one path within the bounds of its constraint.
pub fn wild_scan(indexes: &[IndexDef], filter: &Document, cs: &HashMap<String, Constraint>, hint: Option<usize>) -> Option<IxScan> {
    use crate::wild::WildBound;
    for (i, idx) in indexes.iter().enumerate() {
        let Kind::Wildcard(w) = &idx.kind else { continue };
        if idx.state != State::Ready || !hint.map(|h| h == i).unwrap_or(true) || !index_covers_query(idx, cs) {
            continue;
        }
        let Some(bound) = crate::wild::choose_bound(w, filter, cs) else { continue };
        let intervals = match bound {
            WildBound::Whole => vec![(Vec::new(), None)],
            WildBound::Path(path) => {
                // a pseudo two-field key: the path component, then the value
                let mut pseudo = idx.clone();
                pseudo.key = vec![("\u{0}path".to_string(), true), (path.clone(), w.asc)];
                pseudo.multikey = true;
                let mut c2 = cs.clone();
                c2.insert("\u{0}path".to_string(), Constraint { eq: Some(vec![Bson::String(path.clone())]), ..Default::default() });
                match bounds_for(&pseudo, &c2) {
                    Some((ivs, _, _)) => ivs,
                    None => continue,
                }
            }
        };
        return Some(IxScan { idx: i, intervals, reverse: false, sorted: false, bounded_fields: 1, bounds: vec![("$_path".to_string(), vec!["[MinKey, MaxKey]".to_string()])] });
    }
    None
}

/// Can the wildcard index `idx` answer `filter` when it is forced with a hint? MongoDB refuses to scan a whole wildcard index.
pub fn wild_hint_ok(indexes: &[IndexDef], idx: usize, filter: &Document) -> bool {
    let cs = constraints(filter);
    wild_scan(indexes, filter, &cs, Some(idx)).is_some()
}
