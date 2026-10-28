//! Projection of `find` / `findAndModify` as MongoDB 7.0 runs it: inclusion and exclusion trees over (arrays of) documents,
//! expression values, `$slice`, `$elemMatch`, the positional `arr.$`, and `$meta`.
//!
//! The rules were read off mongod 7.0.45 with differential tests (mongo-tests/gaps/g04_projection.py):
//! * the first field decides the mode: inclusion (`1`, an expression, `.$`) or exclusion (`0`); mixing is an error, `_id` excepted;
//! * `$slice` alone keeps every field; next to inclusion it includes the sliced field; `$elemMatch` alone is an inclusion;
//! * expressions are evaluated against the whole input document and appended after the included fields;
//! * arrays are projected element by element (documents and nested arrays; scalars drop out of an inclusion);
//! * `arr.$` keeps the element at the index recorded while the query matched.

use crate::error::MongoError;
use crate::query::{self, matches};
use bson::{Bson, Document};

type R<T> = Result<T, MongoError>;

#[derive(Clone, Debug)]
enum PN {
    Include,
    Exclude,
    Computed(Bson),
    Slice(i64, Option<i64>),
    ElemMatch(Document),
    /// `path.$`: the field is included and the first array along the path is cut down to the matched element.
    Positional,
    Sub(Vec<(String, PN)>),
}

pub struct FindProjection {
    tree: Vec<(String, PN)>,
    inclusion: bool,
    id_excluded: bool,
    /// Path of the positional operator (without the `$`) and the query it reads the matched index from.
    positional: Option<Vec<String>>,
    filter: Document,
    pub metas: Vec<(String, &'static str)>,
}

fn err(code: i32, name: &'static str, msg: String) -> MongoError {
    MongoError::new(code, name, msg)
}

fn loc(code: i32, msg: &str) -> MongoError {
    static NAMES: std::sync::Mutex<Vec<(i32, &'static str)>> = std::sync::Mutex::new(Vec::new());
    let mut n = NAMES.lock().unwrap();
    let name = match n.iter().find(|(c, _)| *c == code) {
        Some((_, nm)) => *nm,
        None => {
            let nm: &'static str = Box::leak(format!("Location{code}").into_boxed_str());
            n.push((code, nm));
            nm
        }
    };
    MongoError::new(code, name, msg.to_string())
}

fn number(v: &Bson) -> Option<f64> {
    if query::is_number(v) {
        Some(query::as_f64(v))
    } else {
        None
    }
}

fn parse_slice(arg: &Bson) -> R<(i64, Option<i64>)> {
    let num = |b: &Bson| number(b).map(|f| f as i64).ok_or_else(|| loc(28667, "$slice only supports numbers and [skip, limit] arrays"));
    match arg {
        Bson::Array(a) if a.len() == 2 => {
            let (skip, limit) = (num(&a[0])?, num(&a[1])?);
            if limit <= 0 {
                return Err(loc(28724, "$slice limit must be positive"));
            }
            Ok((skip, Some(limit)))
        }
        Bson::Array(a) if a.len() > 2 => Err(loc(28724, "$slice array argument should be of the form [skip, limit]")),
        Bson::Array(_) => Err(loc(28667, "$slice only supports numbers and [skip, limit] arrays")),
        other => Ok((num(other)?, None)),
    }
}

fn check_path(path: &str) -> R<()> {
    if path.is_empty() {
        return Err(loc(40352, "FieldPath cannot be constructed with empty string"));
    }
    if path.ends_with('.') {
        return Err(loc(40353, "FieldPath must not end with a '.'."));
    }
    for part in path.split('.') {
        if part.is_empty() {
            return Err(loc(15998, "FieldPath field names may not be empty strings."));
        }
        if part.starts_with('$') {
            return Err(loc(16410, &format!("FieldPath field names may not start with '$'. Consider using $getField or $setField. Found: {part}")));
        }
    }
    Ok(())
}

/// Insert `leaf` at `parts`, reporting MongoDB's path-collision errors.
fn insert(nodes: &mut Vec<(String, PN)>, parts: &[&str], leaf: PN, full: &str) -> R<()> {
    let (first, rest) = (parts[0], &parts[1..]);
    let pos = nodes.iter().position(|(k, _)| k == first);
    if rest.is_empty() {
        return match (pos, leaf) {
            (None, leaf) => {
                nodes.push((first.to_string(), leaf));
                Ok(())
            }
            (Some(i), PN::Sub(ch)) => match &mut nodes[i].1 {
                PN::Sub(existing) => {
                    for (k, n) in ch {
                        insert(existing, &[k.as_str()], n, full)?;
                    }
                    Ok(())
                }
                _ => Err(loc(31249, &format!("Path collision at {full}"))),
            },
            (Some(i), _) => Err(match nodes[i].1 {
                PN::Sub(_) => loc(31250, &format!("Path collision at {full}")),
                _ => loc(31250, &format!("Path collision at {full}")),
            }),
        };
    }
    match pos {
        None => {
            let mut sub = Vec::new();
            insert(&mut sub, rest, leaf, full)?;
            nodes.push((first.to_string(), PN::Sub(sub)));
            Ok(())
        }
        Some(i) => match &mut nodes[i].1 {
            PN::Sub(ch) => insert(ch, rest, leaf, full),
            _ => Err(loc(31249, &format!("Path collision at {full} remaining portion {}", rest.join(".")))),
        },
    }
}

#[derive(Default)]
struct Flags {
    include: bool,
    exclude: bool,
    slice: bool,
    elem: bool,
    positional: Option<Vec<String>>,
    metas: Vec<(String, &'static str)>,
    id_excluded: bool,
    id_included: bool,
    /// Some(true): an inclusion field was seen first, Some(false): an exclusion field.
    mode: Option<bool>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Leaf {
    Inc,
    CompDoc,
    CompOther,
    Exc,
}

/// Record one leaf: the mode analysis happens right after the field is inserted (so a path collision is reported first).
fn note(fl: &mut Flags, kind: Leaf, name: &str) -> R<()> {
    let inc = !matches!(kind, Leaf::Exc);
    if inc {
        fl.include = true;
    } else {
        fl.exclude = true;
    }
    match fl.mode {
        None => fl.mode = Some(inc),
        Some(true) if !inc => return Err(loc(31254, &format!("Cannot do exclusion on field {name} in inclusion projection"))),
        Some(false) if inc => {
            return Err(match kind {
                Leaf::CompDoc => loc(31252, &format!("Cannot use an expression in an exclusion projection (field {name})")),
                Leaf::CompOther => loc(31310, &format!("Cannot use an expression in an exclusion projection (field {name})")),
                _ => loc(31253, &format!("Cannot do inclusion on field {name} in exclusion projection")),
            })
        }
        _ => {}
    }
    Ok(())
}

/// Parse `spec` (whose fields live under `prefix`); every leaf is inserted into `root` by its full path.
fn parse_into(spec: &Document, prefix: &[String], root: &mut Vec<(String, PN)>, fl: &mut Flags) -> R<()> {
    for (k, v) in spec {
        let prefix_s = prefix.join(".");
        let full = if prefix.is_empty() { k.clone() } else { format!("{prefix_s}.{k}") };
        let comps: Vec<&str> = k.split('.').collect();
        if comps.len() > 1 && comps[..comps.len() - 1].iter().any(|c| *c == "$") {
            return Err(loc(31394, "Positional projection '.$' may only appear at the end of the path"));
        }
        let positional = comps.last() == Some(&"$") && comps.len() > 1;
        if k == "$" {
            return Err(loc(16410, "FieldPath field names may not start with '$'. Consider using $getField or $setField. Found: $"));
        }
        let key: String = if positional { comps[..comps.len() - 1].join(".") } else { k.clone() };
        check_path(&key)?;
        let full_path: Vec<String> = prefix.iter().cloned().chain(key.split('.').map(str::to_string)).collect();
        let parts: Vec<&str> = full_path.iter().map(String::as_str).collect();
        let full = if positional { full_path.join(".") } else { full };
        let top_id = parts.len() == 1 && parts[0] == "_id";
        if positional {
            let t = match v {
                Bson::Document(d) if d.keys().next().map(|k| k == "$slice").unwrap_or(false) => return Err(loc(31271, "positional projection cannot be used with $slice")),
                Bson::Document(_) => return Err(loc(31395, "positional projection must be a boolean or a number")),
                other => number(other).map(|f| f != 0.0).or(match other {
                    Bson::Boolean(b) => Some(*b),
                    _ => None,
                }),
            };
            if t != Some(true) {
                return Err(loc(31395, "Cannot exclude array elements with the positional operator"));
            }
            if fl.positional.is_some() {
                return Err(loc(31276, "Cannot specify more than one positional proj. per query."));
            }
            if fl.elem {
                return Err(loc(31255, "Cannot specify positional operator and $elemMatch."));
            }
            insert(root, &parts, PN::Positional, &full)?;
            note(fl, Leaf::Inc, &full)?;
            fl.positional = Some(parts.iter().map(|s| s.to_string()).collect());
            continue;
        }
        match v {
            Bson::Document(d) if d.len() == 1 && d.contains_key("$slice") => {
                let (s, l) = parse_slice(&d["$slice"])?;
                fl.slice = true;
                insert(root, &parts, PN::Slice(s, l), &full)?;
            }
            Bson::Document(d) if d.len() == 1 && d.contains_key("$elemMatch") => {
                let q = match &d["$elemMatch"] {
                    Bson::Document(q) => q.clone(),
                    _ => return Err(loc(31274, "elemMatch: Invalid argument, object required.")),
                };
                if parts.len() > 1 {
                    return Err(loc(31275, "Cannot use $elemMatch projection on a nested field."));
                }
                if fl.positional.is_some() {
                    return Err(loc(31255, "Cannot specify positional operator and $elemMatch."));
                }
                if q.contains_key("$where") {
                    return Err(err(2, "BadValue", "$where cannot be used in a $elemMatch projection".into()));
                }
                let (probe_doc, probe_q) = elem_probe(&q);
                matches(&probe_doc, &probe_q)?; // report a bad query now
                fl.elem = true;
                insert(root, &parts, PN::ElemMatch(q), &full)?;
            }
            Bson::Document(d) if d.len() == 1 && d.contains_key("$meta") => {
                let key_name: &'static str = match d.get_str("$meta") {
                    Ok("textScore") => crate::text::SCORE_KEY,
                    Ok("searchHighlights") => crate::search_more::HIGHLIGHT_KEY,
                    Ok("geoNearDistance") => crate::geo::DIST_KEY,
                    Ok("geoNearPoint") => crate::geo::POINT_KEY,
                    Ok(other) => return Err(loc(17308, &format!("Unsupported argument to $meta: {other}"))),
                    Err(_) => return Err(err(2, "BadValue", "$meta projection requires a string argument".into())),
                };
                fl.metas.push((full.clone(), key_name));
            }
            Bson::Document(d) if d.is_empty() => {
                return Err(loc(51270, &format!("An empty sub-projection is not a valid value. Found empty object at path {full}")));
            }
            Bson::Document(d) if !d.keys().next().map(|k| k.starts_with('$')).unwrap_or(false) || d.len() > 1 => {
                // a nested specification: {c: {d: 1}} is {"c.d": 1}
                parse_into(d, &full_path, root, fl)?;
            }
            other => {
                let t = match other {
                    Bson::Boolean(b) => Some(*b),
                    n if query::is_number(n) => Some(query::as_f64(n) != 0.0),
                    _ => None,
                };
                match t {
                    Some(true) => {
                        insert(root, &parts, PN::Include, &full)?;
                        if top_id {
                            fl.id_included = true;
                        } else {
                            note(fl, Leaf::Inc, &full)?;
                        }
                    }
                    Some(false) => {
                        if top_id {
                            fl.id_excluded = true;
                            continue;
                        }
                        insert(root, &parts, PN::Exclude, &full)?;
                        note(fl, Leaf::Exc, &full)?;
                    }
                    None => {
                        if let Bson::Document(e) = other {
                            crate::expr::check_single_operator(other, 16410, "FieldPath field names may not start with '$'. Consider using $getField or $setField. Found:")?;
                            if let Some((op, _)) = e.iter().next() {
                                if !crate::expr::is_known_operator(op) {
                                    return Err(loc(31325, &format!("Unknown expression {op}")));
                                }
                            }
                        }
                        crate::expr::validate(other)?;
                        if matches!(other, Bson::Document(_)) {
                            // an operator expression in an exclusion projection is refused while it is parsed, before the path is inserted
                            note(fl, Leaf::CompDoc, &full)?;
                            insert(root, &parts, PN::Computed(other.clone()), &full)?;
                        } else {
                            insert(root, &parts, PN::Computed(other.clone()), &full)?;
                            note(fl, Leaf::CompOther, &full)?;
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

fn has_computed(nodes: &[(String, PN)]) -> bool {
    nodes.iter().any(|(_, n)| match n {
        PN::Computed(_) => true,
        PN::Sub(c) => has_computed(c),
        _ => false,
    })
}

pub fn parse(spec: &Document, filter: &Document) -> R<Option<FindProjection>> {
    if spec.is_empty() {
        return Ok(None);
    }
    let mut tree = Vec::new();
    let mut fl = Flags::default();
    parse_into(spec, &[], &mut tree, &mut fl)?;
    // The mode: an inclusion or an exclusion field decides; otherwise `$elemMatch` alone is an inclusion, `$slice` and
    // `$meta` keep every field, and `{_id: 1}` alone keeps only the _id.
    let inclusion = if fl.include {
        true
    } else if fl.exclude {
        false
    } else if fl.id_included {
        true // {_id: 1, ...}: the _id (and a sliced or elemMatch field) alone
    } else {
        fl.elem && !fl.slice // $elemMatch alone is an inclusion; $slice and $meta alone keep every field
    };
    Ok(Some(FindProjection { tree, inclusion, id_excluded: fl.id_excluded, positional: fl.positional, filter: filter.clone(), metas: fl.metas }))
}

impl FindProjection {
    pub fn has_positional(&self) -> bool {
        self.positional.is_some()
    }

    pub fn needs_geo_near(&self) -> bool {
        self.metas.iter().any(|(_, k)| *k == crate::geo::DIST_KEY || *k == crate::geo::POINT_KEY)
    }

    pub fn needs_text_score(&self) -> bool {
        self.metas.iter().any(|(_, k)| *k == crate::text::SCORE_KEY)
    }

    pub fn apply(&self, doc: &Document) -> R<Document> {
        let mut out = if self.inclusion {
            let mut o = include_doc(doc, &self.tree, doc)?;
            if !self.id_excluded {
                if let Some(id) = doc.get("_id") {
                    if !self.tree.iter().any(|(k, _)| k == "_id") {
                        let mut with_id = Document::new();
                        with_id.insert("_id", id.clone());
                        for (k, v) in o {
                            with_id.insert(k, v);
                        }
                        o = with_id;
                    }
                }
            }
            o
        } else {
            let mut o = exclude_doc(doc, &self.tree)?;
            if self.id_excluded {
                o.remove("_id");
            }
            o
        };
        if let Some(path) = &self.positional {
            if first_array_exists(&out, path) {
                let Some(idx) = elem_match_index(doc, &self.filter)? else {
                    return Err(loc(51246, "positional operator '.$' couldn't find a matching element in the array"));
                };
                if !cut_first_array(&mut out, path, idx) {
                    return Err(loc(51247, "positional operator '.$' element mismatch"));
                }
            }
        }
        crate::text::strip_hidden(&mut out);
        for (name, key) in &self.metas {
            if let Some(v) = doc.get(*key) {
                out.insert(name.clone(), v.clone());
            }
        }
        Ok(out)
    }
}

fn slice_value(v: &Bson, skip: i64, limit: Option<i64>) -> Bson {
    let Bson::Array(a) = v else { return v.clone() };
    let len = a.len() as i64;
    let (start, take) = match limit {
        None if skip >= 0 => (0, skip),
        None => ((len + skip).max(0), len),
        Some(l) if skip >= 0 => (skip.min(len), l),
        Some(l) => ((len + skip).max(0), l),
    };
    let end = start.saturating_add(take).min(len);
    Bson::Array(a[start as usize..end as usize].to_vec())
}

fn elem_probe(q: &Document) -> (Document, Document) {
    let mut w = Document::new();
    w.insert("v", Bson::Array(vec![Bson::Int32(0)]));
    let mut f = Document::new();
    f.insert("v", Bson::Document(bson::doc! { "$elemMatch": q.clone() }));
    (w, f)
}

fn elem_value(v: &Bson, q: &Document) -> R<Option<Bson>> {
    let Bson::Array(a) = v else { return Ok(None) };
    if q.is_empty() {
        // an empty object query matches the first element that is a document or an array
        return Ok(a.iter().find(|e| matches!(e, Bson::Document(_) | Bson::Array(_))).map(|e| Bson::Array(vec![e.clone()])));
    }
    let mut f = Document::new();
    f.insert("v", Bson::Document(bson::doc! { "$elemMatch": q.clone() }));
    for e in a {
        let mut w = Document::new();
        w.insert("v", Bson::Array(vec![e.clone()]));
        if matches(&w, &f)? {
            return Ok(Some(Bson::Array(vec![e.clone()])));
        }
    }
    Ok(None)
}

fn include_doc(src: &Document, nodes: &[(String, PN)], root: &Document) -> R<Document> {
    let mut out = Document::new();
    for (k, v) in src {
        let Some((_, n)) = nodes.iter().find(|(nk, _)| nk == k) else { continue };
        match n {
            PN::Include | PN::Positional => {
                out.insert(k.clone(), v.clone());
            }
            PN::Slice(s, l) => {
                out.insert(k.clone(), slice_value(v, *s, *l));
            }
            PN::ElemMatch(_) => {} // applied after the other fields, see below
            PN::Sub(ch) => match v {
                Bson::Document(d) => {
                    out.insert(k.clone(), Bson::Document(include_doc(d, ch, root)?));
                }
                Bson::Array(a) => {
                    out.insert(k.clone(), Bson::Array(include_array(a, ch, root)?));
                }
                _ => {
                    if has_computed(ch) {
                        out.insert(k.clone(), Bson::Document(computed_only(ch, root)?));
                    }
                }
            },
            PN::Exclude | PN::Computed(_) => {}
        }
    }
    // expressions and `$elemMatch` results, in specification order, after the included fields
    for (k, n) in nodes {
        match n {
            PN::Computed(e) => {
                if let Some(x) = crate::expr::eval(e, &crate::expr::Scope::new(root))? {
                    out.insert(k.clone(), x);
                }
            }
            PN::ElemMatch(q) => {
                if let Some(v) = src.get(k) {
                    if let Some(x) = elem_value(v, q)? {
                        out.insert(k.clone(), x);
                    }
                }
            }
            PN::Sub(ch) if has_computed(ch) && !src.contains_key(k) => {
                out.insert(k.clone(), Bson::Document(computed_only(ch, root)?));
            }
            _ => {}
        }
    }
    Ok(out)
}

fn computed_only(nodes: &[(String, PN)], root: &Document) -> R<Document> {
    include_doc(&Document::new(), nodes, root)
}

fn include_array(a: &[Bson], ch: &[(String, PN)], root: &Document) -> R<Vec<Bson>> {
    let comp = has_computed(ch);
    let mut out = Vec::new();
    for e in a {
        match e {
            Bson::Document(d) => out.push(Bson::Document(include_doc(d, ch, root)?)),
            Bson::Array(inner) => out.push(Bson::Array(include_array(inner, ch, root)?)),
            // a scalar element is replaced by a document holding the computed fields
            _ if comp => out.push(Bson::Document(computed_only(ch, root)?)),
            _ => {}
        }
    }
    Ok(out)
}

fn exclude_doc(src: &Document, nodes: &[(String, PN)]) -> R<Document> {
    let mut out = Document::new();
    for (k, v) in src {
        match nodes.iter().find(|(nk, _)| nk == k).map(|(_, n)| n) {
            None | Some(PN::Include) | Some(PN::Positional) | Some(PN::Computed(_)) => {
                out.insert(k.clone(), v.clone());
            }
            Some(PN::Exclude) => {}
            Some(PN::Slice(s, l)) => {
                out.insert(k.clone(), slice_value(v, *s, *l));
            }
            Some(PN::ElemMatch(q)) => {
                if let Some(x) = elem_value(v, q)? {
                    out.insert(k.clone(), x);
                }
            }
            Some(PN::Sub(ch)) => match v {
                Bson::Document(d) => {
                    out.insert(k.clone(), Bson::Document(exclude_doc(d, ch)?));
                }
                Bson::Array(a) => {
                    out.insert(k.clone(), Bson::Array(exclude_array(a, ch)?));
                }
                other => {
                    out.insert(k.clone(), other.clone());
                }
            },
        }
    }
    Ok(out)
}

fn exclude_array(a: &[Bson], ch: &[(String, PN)]) -> R<Vec<Bson>> {
    let mut out = Vec::with_capacity(a.len());
    for e in a {
        out.push(match e {
            Bson::Document(d) => Bson::Document(exclude_doc(d, ch)?),
            Bson::Array(inner) => Bson::Array(exclude_array(inner, ch)?),
            other => other.clone(),
        });
    }
    Ok(out)
}

fn first_array_exists(doc: &Document, path: &[String]) -> bool {
    match doc.get(&path[0]) {
        Some(Bson::Array(_)) => true,
        Some(Bson::Document(d)) if path.len() > 1 => first_array_exists(d, &path[1..]),
        _ => false,
    }
}

/// Replace the first array met along `path` by its element `idx`.
fn cut_first_array(doc: &mut Document, path: &[String], idx: usize) -> bool {
    match doc.get_mut(&path[0]) {
        Some(Bson::Array(a)) => match a.get(idx).cloned() {
            Some(keep) => {
                *a = vec![keep];
                true
            }
            None => false,
        },
        Some(Bson::Document(d)) if path.len() > 1 => cut_first_array(d, &path[1..], idx),
        _ => true,
    }
}

// ---------------------------------------------------------------------------------------------
// The array index a query matched (MatchDetails::elemMatchKey), for the positional operator
// ---------------------------------------------------------------------------------------------

/// Evaluation rank of an expression type, as in mongod's MatchType order (children of an AND are sorted by it, then by path).
fn rank(op: &str) -> u8 {
    match op {
        "$size" => 4,
        "$eq" => 5,
        "$lte" => 6,
        "$lt" => 7,
        "$gt" => 8,
        "$gte" => 9,
        "$regex" => 10,
        "$mod" => 11,
        "$exists" => 12,
        "$in" => 13,
        "$bitsAllSet" => 14,
        "$bitsAllClear" => 15,
        "$bitsAnySet" => 16,
        "$bitsAnyClear" => 17,
        "$type" => 22,
        "$elemMatch" => 30,
        _ => 40,
    }
}

struct Pred {
    path: String,
    cond: Bson, // an operator document {op: arg} or a plain value (equality)
    op: String,
}

fn flatten(filter: &Document, out: &mut Vec<Pred>) {
    for (k, v) in filter {
        match k.as_str() {
            "$and" => {
                if let Bson::Array(a) = v {
                    for s in a.iter().filter_map(|s| s.as_document()) {
                        flatten(s, out);
                    }
                }
            }
            "$or" | "$nor" | "$expr" | "$where" | "$text" | "$comment" | "$jsonSchema" => {}
            path => match v {
                Bson::Document(d) if d.keys().next().map(|k| k.starts_with('$')).unwrap_or(false) => {
                    for (op, arg) in d {
                        if matches!(op.as_str(), "$options" | "$regex") && op == "$options" {
                            continue;
                        }
                        if op == "$all" {
                            // $all is an AND of equalities (or $elemMatch) in list order
                            if let Bson::Array(items) = arg {
                                for it in items {
                                    let (cond, opn) = match it {
                                        Bson::Document(x) if x.contains_key("$elemMatch") => (Bson::Document(x.clone()), "$elemMatch".to_string()),
                                        other => (Bson::Document(bson::doc! { "$eq": other.clone() }), "$eq".to_string()),
                                    };
                                    out.push(Pred { path: path.to_string(), cond, op: opn });
                                }
                            }
                            continue;
                        }
                        let mut cond = Document::new();
                        cond.insert(op.clone(), arg.clone());
                        if op == "$regex" {
                            if let Some(o) = d.get("$options") {
                                cond.insert("$options", o.clone());
                            }
                        }
                        out.push(Pred { path: path.to_string(), cond: Bson::Document(cond), op: op.clone() });
                    }
                }
                other => out.push(Pred { path: path.to_string(), cond: other.clone(), op: "$eq".to_string() }),
            },
        }
    }
}

/// The index, in the first array along `path`, of the first element for which the predicate holds.
fn pred_index(doc: &Document, p: &Pred) -> R<Option<usize>> {
    // operators that never record an index
    if matches!(p.op.as_str(), "$size" | "$ne" | "$nin" | "$not") {
        return Ok(None);
    }
    if let Bson::Document(c) = &p.cond {
        if c.contains_key("$not") || c.contains_key("$ne") || c.contains_key("$nin") {
            return Ok(None);
        }
    }
    let parts: Vec<&str> = p.path.split('.').collect();
    // walk to the first array
    let mut cur: &Document = doc;
    for (depth, part) in parts.iter().enumerate() {
        let Some(v) = cur.get(*part) else { return Ok(None) };
        match v {
            Bson::Array(a) => {
                if p.op == "$exists" {
                    return Ok(if a.is_empty() { None } else { Some(0) });
                }
                // equality with the whole array records no index
                if depth + 1 == parts.len() {
                    if let (Bson::Array(_), "$eq") = (&p.cond, p.op.as_str()) {
                        return Ok(None);
                    }
                }
                for (i, e) in a.iter().enumerate() {
                    let mut probe = doc.clone();
                    // `$type` looks at the element itself, every other operator at a one-element array
                    let wrapped = if p.op == "$type" { e.clone() } else { Bson::Array(vec![e.clone()]) };
                    if !replace_at(&mut probe, &parts[..=depth], wrapped) {
                        continue;
                    }
                    let mut f = Document::new();
                    f.insert(p.path.clone(), p.cond.clone());
                    if matches(&probe, &f)? {
                        return Ok(Some(i));
                    }
                }
                return Ok(None);
            }
            Bson::Document(d) => cur = d,
            _ => return Ok(None),
        }
    }
    Ok(None)
}

fn replace_at(doc: &mut Document, parts: &[&str], v: Bson) -> bool {
    if parts.len() == 1 {
        doc.insert(parts[0].to_string(), v);
        return true;
    }
    match doc.get_mut(parts[0]) {
        Some(Bson::Document(d)) => replace_at(d, &parts[1..], v),
        _ => false,
    }
}

fn elem_match_index(doc: &Document, filter: &Document) -> R<Option<usize>> {
    let mut preds = Vec::new();
    flatten(filter, &mut preds);
    preds.sort_by(|a, b| rank(&a.op).cmp(&rank(&b.op)).then_with(|| a.path.cmp(&b.path)));
    let mut idx = None;
    for p in &preds {
        if let Some(i) = pred_index(doc, p)? {
            idx = Some(i);
        }
    }
    Ok(idx)
}
