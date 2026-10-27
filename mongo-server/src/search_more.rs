//! The remaining Atlas Search operators and collectors: `near`, `geoWithin`, `geoShape`, `moreLikeThis`,
//! `queryString`, `embeddedDocument`, `facet` and `highlight`.
//!
//! There is no reference server for these, so they follow the documented Atlas semantics:
//!
//! * `near` scores `pivot / (pivot + distance)` for numbers, dates (milliseconds) and GeoJSON points (meters, haversine
//!   on Lucene's mean earth radius) and matches every document that has a value of the origin's type.
//! * `geoWithin` (`geometry`, `circle`, `box`) and `geoShape` (`intersects`, `disjoint`, `within`, `contains`) use the
//!   spherical predicates of the geo query engine; Atlas (Lucene) joins polygon vertices with straight lines in
//!   longitude / latitude, so the two only differ for shapes whose edges span many degrees.
//! * `moreLikeThis` extracts the (at most 25) highest tf-idf terms of the `like` documents and matches documents that
//!   contain any of them; the source document itself matches when it is in the collection.
//! * `queryString` is Lucene's classic query syntax (`+ - AND OR NOT`, groups, `field:`, phrases with `~slop`, `?` and
//!   `*` wildcards, `~n` fuzziness, `^boost`, `[a TO b]` ranges) over the standard analyzer.
//! * `embeddedDocument` runs its operator against each element of an array of documents; the score is the sum
//!   (`score.embedded.aggregate`: `sum`, `maximum`, `minimum`, `mean`).
//! * `facet` (string, number and date facets, `numBuckets`, `boundaries`, `default`) answers `$searchMeta` and fills
//!   `$$SEARCH_META` of `$search`.
//! * `highlight` returns `{$meta: "searchHighlights"}`: per passage `{score, path, texts: [{value, type}]}`.
//!   Passage scores are not Lucene's.

use crate::error::MongoError;
use crate::geo::{self, Shape, V};
use crate::query::path_values;
use crate::search::{analyze, analyze_spans, bad, eval, int_of, num_f64, parse_paths, phrase_count, queries, strings_at, values_at, wildcard_to_regex, Ctx, Fuzzy, Sel};
use bson::{doc, Bson, Document};
use std::collections::BTreeMap;

type R<T> = Result<T, MongoError>;

/// Lucene's mean earth radius for haversine distances (`SloppyMath.TO_METERS`).
const EARTH_M: f64 = 6_371_008.7714;

/// Hidden fields carried by the documents of a `$search` stage.
pub const HIGHLIGHT_KEY: &str = "\u{1}searchHighlights";
pub const META_KEY: &str = "\u{1}searchMeta";

fn point_of(b: &Bson, what: &str) -> R<V> {
    match b {
        Bson::Document(d) => match geo::parse_geojson(d) {
            Ok(Shape::Point(p)) => Ok(p),
            _ => Err(bad(format!("{what} must be a GeoJSON Point"))),
        },
        _ => Err(bad(format!("{what} must be a GeoJSON Point"))),
    }
}

fn exact_paths(spec: &Document) -> R<Vec<String>> {
    Ok(parse_paths(spec.get("path"))?.into_iter().filter_map(|s| if let Sel::Exact(p) = s { Some(p) } else { None }).collect())
}

fn geo_elements(d: &Document, path: &str) -> Vec<geo::Elem> {
    path_values(d, path).into_iter().flat_map(geo::elements).collect()
}

// ---------------------------------------------------------------------------------------------
// near, geoWithin, geoShape
// ---------------------------------------------------------------------------------------------

pub fn near(spec: &Document, d: &Document) -> R<Option<f64>> {
    let paths = exact_paths(spec)?;
    let origin = spec.get("origin").ok_or_else(|| bad("near requires an 'origin'"))?;
    let pivot = spec.get("pivot").and_then(num_f64).filter(|p| *p > 0.0).ok_or_else(|| bad("near requires a 'pivot' greater than 0"))?;
    let mut best: Option<f64> = None;
    let mut take = |dist: f64| best = Some(best.map_or(dist, |b: f64| b.min(dist)));
    match origin {
        Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) => {
            let o = num_f64(origin).unwrap();
            for p in &paths {
                for v in values_at(d, p) {
                    if matches!(v, Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_)) {
                        take((num_f64(v).unwrap() - o).abs());
                    }
                }
            }
        }
        Bson::DateTime(t) => {
            for p in &paths {
                for v in values_at(d, p) {
                    if let Bson::DateTime(x) = v {
                        take((x.timestamp_millis() - t.timestamp_millis()).abs() as f64);
                    }
                }
            }
        }
        Bson::Document(_) => {
            let c = point_of(origin, "near.origin")?;
            for p in &paths {
                for e in geo_elements(d, p) {
                    if let Some(s) = geo::elem_shape(&e) {
                        take(s.dist_point(&c) * EARTH_M);
                    }
                }
            }
        }
        _ => return Err(bad("near.origin must be a number, a date or a GeoJSON Point")),
    }
    Ok(best.map(|dist| pivot / (pivot + dist)))
}

pub fn geo_within(spec: &Document, d: &Document) -> R<Option<f64>> {
    let paths = exact_paths(spec)?;
    let given = ["geometry", "circle", "box"].iter().filter(|k| spec.contains_key(**k)).count();
    if given != 1 {
        return Err(bad("geoWithin requires exactly one of geometry, circle, box"));
    }
    let inside: Box<dyn Fn(&V) -> bool> = if let Some(Bson::Document(g)) = spec.get("geometry") {
        let polys = match geo::parse_geojson(g).map_err(bad)? {
            Shape::Polygon(p) => vec![p],
            Shape::MultiPolygon(ps) => ps,
            _ => return Err(bad("geoWithin.geometry must be a GeoJSON Polygon or MultiPolygon")),
        };
        Box::new(move |p| geo::within(&Shape::Point(*p), &polys))
    } else if let Some(Bson::Document(c)) = spec.get("circle") {
        let center = point_of(c.get("center").unwrap_or(&Bson::Null), "geoWithin.circle.center")?;
        let radius = c.get("radius").and_then(num_f64).filter(|r| *r >= 0.0).ok_or_else(|| bad("geoWithin.circle.radius must be a non-negative number of meters"))?;
        Box::new(move |p| geo::angle(p, &center) * EARTH_M <= radius)
    } else if let Some(Bson::Document(b)) = spec.get("box") {
        let bl = geo::to_lnglat(&point_of(b.get("bottomLeft").unwrap_or(&Bson::Null), "geoWithin.box.bottomLeft")?);
        let tr = geo::to_lnglat(&point_of(b.get("topRight").unwrap_or(&Bson::Null), "geoWithin.box.topRight")?);
        Box::new(move |p| {
            let (lng, lat) = geo::to_lnglat(p);
            let in_lng = if bl.0 <= tr.0 { lng >= bl.0 && lng <= tr.0 } else { lng >= bl.0 || lng <= tr.0 };
            in_lng && lat >= bl.1 && lat <= tr.1
        })
    } else {
        return Err(bad("geoWithin requires a geometry, circle or box object"));
    };
    for p in &paths {
        for e in geo_elements(d, p) {
            if let Some(pt) = geo::elem_point(&e) {
                if inside(&pt) {
                    return Ok(Some(1.0));
                }
            }
        }
    }
    Ok(None)
}

fn polys_of(s: &Shape) -> Option<Vec<geo::Poly>> {
    match s {
        Shape::Polygon(p) => Some(vec![p.clone()]),
        Shape::MultiPolygon(ps) => Some(ps.clone()),
        _ => None,
    }
}

pub fn geo_shape(spec: &Document, d: &Document) -> R<Option<f64>> {
    let paths = exact_paths(spec)?;
    let relation = spec.get_str("relation").map_err(|_| bad("geoShape requires a 'relation'"))?;
    if !matches!(relation, "contains" | "disjoint" | "intersects" | "within") {
        return Err(bad("geoShape.relation must be one of contains, disjoint, intersects, within"));
    }
    let Some(Bson::Document(g)) = spec.get("geometry") else { return Err(bad("geoShape requires a 'geometry'")) };
    let query = geo::parse_geojson(g).map_err(bad)?;
    let query_polys = polys_of(&query);
    if relation == "within" && query_polys.is_none() {
        return Err(bad("geoShape relation 'within' does not support Point, MultiPoint or LineString geometries"));
    }
    for p in &paths {
        for e in geo_elements(d, p) {
            let Some(shape) = geo::elem_shape(&e) else { continue };
            let hit = match relation {
                "intersects" => geo::intersects(&shape, &query),
                "disjoint" => !geo::intersects(&shape, &query),
                "within" => geo::within(&shape, query_polys.as_ref().unwrap()),
                _ => match polys_of(&shape) {
                    Some(polys) => geo::within(&query, &polys),
                    None => matches!(query, Shape::Point(_)) && geo::intersects(&shape, &query),
                },
            };
            if hit {
                return Ok(Some(1.0));
            }
        }
    }
    Ok(None)
}

// ---------------------------------------------------------------------------------------------
// moreLikeThis
// ---------------------------------------------------------------------------------------------

fn like_terms(v: &Bson, path: &str, tf: &mut BTreeMap<(String, String), usize>) {
    match v {
        Bson::String(s) => {
            for t in analyze(s) {
                *tf.entry((path.to_string(), t)).or_default() += 1;
            }
        }
        Bson::Array(a) => a.iter().for_each(|x| like_terms(x, path, tf)),
        Bson::Document(d) => {
            for (k, x) in d {
                let p = if path.is_empty() { k.clone() } else { format!("{path}.{k}") };
                like_terms(x, &p, tf);
            }
        }
        _ => {}
    }
}

pub fn more_like_this(spec: &Document, d: &Document, ctx: &Ctx) -> R<Option<f64>> {
    let likes: Vec<&Document> = match spec.get("like") {
        Some(Bson::Document(x)) => vec![x],
        Some(Bson::Array(a)) if !a.is_empty() && a.iter().all(|x| matches!(x, Bson::Document(_))) => a.iter().filter_map(|x| x.as_document()).collect(),
        _ => return Err(bad("moreLikeThis requires 'like': a document or an array of documents")),
    };
    let key = format!("{:?}", spec.get("like"));
    let cached = ctx.mlt.borrow().get(&key).cloned();
    let terms = match cached {
        Some(t) => t,
        None => {
            let mut tf = BTreeMap::new();
            for l in &likes {
                like_terms(&Bson::Document((*l).clone()), "", &mut tf);
            }
            let n = ctx.docs.len() as f64;
            let mut scored: Vec<(f64, String, String)> = Vec::new();
            for ((path, term), f) in tf {
                let df = ctx.doc_freq(&Sel::Exact(path.clone()), &term, &Fuzzy::default());
                if df == 0 {
                    continue;
                }
                let idf = (1.0 + (n - df as f64 + 0.5) / (df as f64 + 0.5)).ln();
                scored.push((f as f64 * idf, path, term));
            }
            scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal).then_with(|| (&a.1, &a.2).cmp(&(&b.1, &b.2))));
            scored.truncate(25);
            let t: Vec<(String, String)> = scored.into_iter().map(|(_, p, t)| (p, t)).collect();
            ctx.mlt.borrow_mut().insert(key, t.clone());
            t
        }
    };
    let mut total = 0.0;
    let mut hit = false;
    for (path, term) in &terms {
        let sel = Sel::Exact(path.clone());
        let toks: Vec<String> = strings_at(d, &sel).iter().flat_map(|s| analyze(s)).collect();
        let tf = toks.iter().filter(|t| *t == term).count();
        if tf > 0 {
            hit = true;
            total += ctx.bm25(&sel, term, &Fuzzy::default(), tf, toks.len());
        }
    }
    Ok(hit.then_some(total))
}

// ---------------------------------------------------------------------------------------------
// embeddedDocument
// ---------------------------------------------------------------------------------------------

fn nest(path: &str, v: Bson) -> Document {
    let mut cur = v;
    for part in path.split('.').rev() {
        cur = Bson::Document(doc! { part: cur });
    }
    match cur {
        Bson::Document(d) => d,
        _ => Document::new(),
    }
}

pub fn embedded_document(spec: &Document, d: &Document, ctx: &Ctx) -> R<Option<f64>> {
    let path = spec.get_str("path").map_err(|_| bad("embeddedDocument requires a string 'path'"))?;
    let Some(Bson::Document(op)) = spec.get("operator") else { return Err(bad("embeddedDocument requires an 'operator'")) };
    let aggregate = match spec.get("score") {
        Some(Bson::Document(s)) => match s.get("embedded") {
            Some(Bson::Document(e)) => match e.get_str("aggregate") {
                Ok(a @ ("sum" | "maximum" | "minimum" | "mean")) => a,
                Ok(_) => return Err(bad("score.embedded.aggregate must be one of sum, maximum, minimum, mean")),
                Err(_) => "sum",
            },
            _ => "sum",
        },
        _ => "sum",
    };
    let mut scores: Vec<f64> = Vec::new();
    for v in path_values(d, path) {
        let elems: Vec<&Bson> = match v {
            Bson::Array(a) => a.iter().collect(),
            other => vec![other],
        };
        for e in elems {
            if matches!(e, Bson::Document(_)) {
                if let Some(s) = eval(op, &nest(path, e.clone()), ctx)? {
                    scores.push(s);
                }
            }
        }
    }
    if scores.is_empty() {
        return Ok(None);
    }
    Ok(Some(match aggregate {
        "maximum" => scores.iter().cloned().fold(f64::MIN, f64::max),
        "minimum" => scores.iter().cloned().fold(f64::MAX, f64::min),
        "mean" => scores.iter().sum::<f64>() / scores.len() as f64,
        _ => scores.iter().sum(),
    }))
}

// ---------------------------------------------------------------------------------------------
// queryString: Lucene's classic query syntax
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Debug)]
enum Occur {
    Must,
    Should,
    MustNot,
}

#[derive(Clone, Debug)]
enum Q {
    Term { field: Option<String>, text: String, fuzzy: Option<usize>, boost: f64 },
    Phrase { field: Option<String>, text: String, slop: usize, boost: f64 },
    Range { field: Option<String>, lo: Option<String>, hi: Option<String>, incl_lo: bool, incl_hi: bool, boost: f64 },
    Group { clauses: Vec<(Occur, Q)>, boost: f64 },
    All,
}

struct Parser {
    c: Vec<char>,
    i: usize,
}

impl Parser {
    fn peek(&self) -> Option<char> {
        self.c.get(self.i).copied()
    }

    fn ws(&mut self) {
        while self.peek().map_or(false, |c| c.is_whitespace()) {
            self.i += 1;
        }
    }

    fn word_at(&self, w: &str) -> bool {
        let n = w.chars().count();
        self.c.len() >= self.i + n && self.c[self.i..self.i + n].iter().collect::<String>() == w && self.c.get(self.i + n).map_or(true, |c| c.is_whitespace() || *c == '(' || *c == '"' || *c == '+' || *c == '-')
    }

    fn query(&mut self, depth: usize) -> R<Vec<(Occur, Q)>> {
        if depth > 64 {
            return Err(bad("queryString is nested too deeply"));
        }
        let mut clauses: Vec<(Occur, Q)> = Vec::new();
        loop {
            self.ws();
            match self.peek() {
                None => break,
                Some(')') if depth > 0 => break,
                Some(')') => return Err(bad("queryString: unbalanced ')'")),
                _ => {}
            }
            // conjunction
            let mut conj = None;
            if self.word_at("AND") {
                self.i += 3;
                conj = Some(true);
            } else if self.c[self.i..].starts_with(&['&', '&']) {
                self.i += 2;
                conj = Some(true);
            } else if self.word_at("OR") {
                self.i += 2;
                conj = Some(false);
            } else if self.c[self.i..].starts_with(&['|', '|']) {
                self.i += 2;
                conj = Some(false);
            }
            self.ws();
            // modifiers
            let mut modifier = ' ';
            loop {
                match self.peek() {
                    Some(c @ ('+' | '-' | '!')) => {
                        modifier = if c == '+' { '+' } else { '-' };
                        self.i += 1;
                    }
                    _ if self.word_at("NOT") => {
                        modifier = '-';
                        self.i += 3;
                        self.ws();
                    }
                    _ => break,
                }
            }
            self.ws();
            if self.peek().is_none() || self.peek() == Some(')') {
                return Err(bad("queryString: a clause is missing after an operator"));
            }
            let q = self.clause(depth)?;
            if conj == Some(true) {
                if let Some(last) = clauses.last_mut() {
                    if last.0 == Occur::Should {
                        last.0 = Occur::Must;
                    }
                }
            }
            let occur = match modifier {
                '-' => Occur::MustNot,
                '+' => Occur::Must,
                _ if conj == Some(true) => Occur::Must,
                _ => Occur::Should,
            };
            clauses.push((occur, q));
        }
        Ok(clauses)
    }

    fn number(&mut self) -> Option<f64> {
        let st = self.i;
        while self.peek().map_or(false, |c| c.is_ascii_digit() || c == '.') {
            self.i += 1;
        }
        self.c[st..self.i].iter().collect::<String>().parse().ok()
    }

    fn boost(&mut self) -> R<f64> {
        if self.peek() == Some('^') {
            self.i += 1;
            return self.number().ok_or_else(|| bad("queryString: a boost must be a number"));
        }
        Ok(1.0)
    }

    fn clause(&mut self, depth: usize) -> R<Q> {
        // field prefix
        let mut field = None;
        let mut j = self.i;
        while j < self.c.len() && (self.c[j].is_alphanumeric() || matches!(self.c[j], '_' | '.' | '*' | '@')) {
            j += 1;
        }
        if j > self.i && self.c.get(j) == Some(&':') {
            field = Some(self.c[self.i..j].iter().collect::<String>());
            self.i = j + 1;
        }
        match self.peek() {
            Some('(') => {
                self.i += 1;
                let clauses = self.query(depth + 1)?;
                if self.peek() != Some(')') {
                    return Err(bad("queryString: unbalanced '('"));
                }
                self.i += 1;
                let boost = self.boost()?;
                // a field prefix applies to every clause of the group
                let clauses = match field {
                    Some(f) => clauses.into_iter().map(|(o, q)| (o, with_field(q, &f))).collect(),
                    None => clauses,
                };
                Ok(Q::Group { clauses, boost })
            }
            Some('"') => {
                self.i += 1;
                let mut text = String::new();
                loop {
                    match self.peek() {
                        None => return Err(bad("queryString: unterminated phrase")),
                        Some('\\') => {
                            self.i += 1;
                            if let Some(c) = self.peek() {
                                text.push(c);
                                self.i += 1;
                            }
                        }
                        Some('"') => {
                            self.i += 1;
                            break;
                        }
                        Some(c) => {
                            text.push(c);
                            self.i += 1;
                        }
                    }
                }
                let slop = if self.peek() == Some('~') {
                    self.i += 1;
                    self.number().unwrap_or(0.0) as usize
                } else {
                    0
                };
                let boost = self.boost()?;
                Ok(Q::Phrase { field, text, slop, boost })
            }
            Some(open @ ('[' | '{')) => {
                let close = if open == '[' { ']' } else { '}' };
                let end = self.c[self.i..].iter().position(|c| *c == ']' || *c == '}').ok_or_else(|| bad("queryString: unterminated range"))? + self.i;
                let body: String = self.c[self.i + 1..end].iter().collect();
                let end_ch = self.c[end];
                self.i = end + 1;
                let parts: Vec<&str> = body.split_whitespace().collect();
                if parts.len() != 3 || parts[1] != "TO" {
                    return Err(bad("queryString: a range must look like [a TO b]"));
                }
                let bound = |s: &str| if s == "*" { None } else { Some(s.trim_matches('"').to_lowercase()) };
                let _ = close;
                let boost = self.boost()?;
                Ok(Q::Range { field, lo: bound(parts[0]), hi: bound(parts[2]), incl_lo: open == '[', incl_hi: end_ch == ']', boost })
            }
            _ => {
                let mut text = String::new();
                while let Some(c) = self.peek() {
                    if c.is_whitespace() || c == ')' || c == '(' || c == '^' || (c == '~' ) {
                        break;
                    }
                    if c == '\\' {
                        self.i += 1;
                        if let Some(n) = self.peek() {
                            text.push('\u{0}');
                            text.push(n);
                            self.i += 1;
                        }
                        continue;
                    }
                    text.push(c);
                    self.i += 1;
                }
                if text.is_empty() {
                    return Err(bad("queryString: syntax error"));
                }
                let fuzzy = if self.peek() == Some('~') {
                    self.i += 1;
                    Some(self.number().map(|n| n as usize).unwrap_or(2).min(2))
                } else {
                    None
                };
                let boost = self.boost()?;
                if text == "*" && (field.as_deref() == Some("*") || field.is_none()) {
                    return Ok(Q::All);
                }
                Ok(Q::Term { field, text, fuzzy, boost })
            }
        }
    }
}

fn with_field(q: Q, f: &str) -> Q {
    match q {
        Q::Term { field: None, text, fuzzy, boost } => Q::Term { field: Some(f.to_string()), text, fuzzy, boost },
        Q::Phrase { field: None, text, slop, boost } => Q::Phrase { field: Some(f.to_string()), text, slop, boost },
        Q::Range { field: None, lo, hi, incl_lo, incl_hi, boost } => Q::Range { field: Some(f.to_string()), lo, hi, incl_lo, incl_hi, boost },
        Q::Group { clauses, boost } => Q::Group { clauses: clauses.into_iter().map(|(o, q)| (o, with_field(q, f))).collect(), boost },
        other => other,
    }
}

fn parse_query_string(q: &str) -> R<Vec<(Occur, Q)>> {
    let mut p = Parser { c: q.chars().collect(), i: 0 };
    let clauses = p.query(0)?;
    if p.i < p.c.len() {
        return Err(bad("queryString: syntax error"));
    }
    Ok(clauses)
}

fn is_pattern(t: &str) -> bool {
    let mut esc = false;
    for c in t.chars() {
        if c == '\u{0}' {
            esc = true;
            continue;
        }
        if !esc && (c == '*' || c == '?') {
            return true;
        }
        esc = false;
    }
    false
}

fn unescape(t: &str) -> String {
    t.replace('\u{0}', "")
}

fn eval_bool(clauses: &[(Occur, Q)], default: &str, d: &Document, ctx: &Ctx) -> R<Option<f64>> {
    let mut score = 0.0;
    let mut has_must = false;
    let mut should_hit = false;
    for (o, q) in clauses {
        let r = eval_q(q, default, d, ctx)?;
        match o {
            Occur::Must => {
                has_must = true;
                match r {
                    Some(s) => score += s,
                    None => return Ok(None),
                }
            }
            Occur::MustNot => {
                if r.is_some() {
                    return Ok(None);
                }
            }
            Occur::Should => {
                if let Some(s) = r {
                    score += s;
                    should_hit = true;
                }
            }
        }
    }
    if !has_must && !should_hit {
        return Ok(None);
    }
    Ok(Some(score))
}

fn eval_q(q: &Q, default: &str, d: &Document, ctx: &Ctx) -> R<Option<f64>> {
    match q {
        Q::All => Ok(Some(1.0)),
        Q::Group { clauses, boost } => Ok(eval_bool(clauses, default, d, ctx)?.map(|s| s * boost)),
        Q::Term { field, text, fuzzy, boost } => {
            let sel = Sel::Exact(field.clone().unwrap_or_else(|| default.to_string()));
            let toks: Vec<String> = strings_at(d, &sel).iter().flat_map(|s| analyze(s)).collect();
            if is_pattern(text) {
                let pat = format!("^(?:{})$", wildcard_to_regex(&text.replace('\u{0}', "\\").to_lowercase()));
                let mut hit = false;
                for t in &toks {
                    if crate::regex::matches_str(&pat, "s", t)? {
                        hit = true;
                        break;
                    }
                }
                return Ok(hit.then_some(*boost));
            }
            let qtoks = analyze(&unescape(text));
            let fz = Fuzzy { max_edits: fuzzy.unwrap_or(0), prefix: 0 };
            let mut total = 0.0;
            let mut hit = false;
            for t in &qtoks {
                let tf = toks.iter().filter(|x| fz.matches(t, x)).count();
                if tf > 0 {
                    hit = true;
                    total += ctx.bm25(&sel, t, &fz, tf, toks.len());
                }
            }
            Ok(hit.then_some(total * boost))
        }
        Q::Phrase { field, text, slop, boost } => {
            let sel = Sel::Exact(field.clone().unwrap_or_else(|| default.to_string()));
            let ptoks = analyze(text);
            if ptoks.is_empty() {
                return Ok(None);
            }
            let mut best: Option<f64> = None;
            for value in strings_at(d, &sel) {
                let toks = analyze(&value);
                if let Some(count) = phrase_count(&toks, &ptoks, *slop) {
                    let s: f64 = ptoks.iter().map(|t| ctx.bm25(&sel, t, &Fuzzy::default(), count, toks.len())).sum();
                    best = Some(best.unwrap_or(0.0) + s);
                }
            }
            Ok(best.map(|s| s * boost))
        }
        Q::Range { field, lo, hi, incl_lo, incl_hi, boost } => {
            let p = field.clone().unwrap_or_else(|| default.to_string());
            let in_str = |t: &str| {
                lo.as_deref().map_or(true, |l| if *incl_lo { t >= l } else { t > l }) && hi.as_deref().map_or(true, |h| if *incl_hi { t <= h } else { t < h })
            };
            let numeric = |x: f64| {
                let ok_lo = match lo.as_deref() {
                    None => Some(true),
                    Some(l) => l.parse::<f64>().ok().map(|l| if *incl_lo { x >= l } else { x > l }),
                };
                let ok_hi = match hi.as_deref() {
                    None => Some(true),
                    Some(h) => h.parse::<f64>().ok().map(|h| if *incl_hi { x <= h } else { x < h }),
                };
                matches!((ok_lo, ok_hi), (Some(true), Some(true)))
            };
            let mut hit = values_at(d, &p).iter().any(|v| num_f64(v).map_or(false, numeric));
            if !hit {
                hit = strings_at(d, &Sel::Exact(p)).iter().flat_map(|s| analyze(s)).any(|t| in_str(&t));
            }
            Ok(hit.then_some(*boost))
        }
    }
}

pub fn query_string(spec: &Document, d: &Document, ctx: &Ctx) -> R<Option<f64>> {
    let default = spec.get_str("defaultPath").map_err(|_| bad("queryString requires a string 'defaultPath'"))?;
    let q = spec.get_str("query").map_err(|_| bad("queryString requires a string 'query'"))?;
    let key = format!("qs:{q}");
    // parsing is cheap compared with scanning the collection, and keeps the cache out of the way
    let _ = key;
    let clauses = parse_query_string(q)?;
    if clauses.is_empty() {
        return Ok(None);
    }
    eval_bool(&clauses, default, d, ctx)
}

// ---------------------------------------------------------------------------------------------
// facet
// ---------------------------------------------------------------------------------------------

pub fn facets(spec: &Document, hits: &[&Document]) -> R<Document> {
    let Some(Bson::Document(fs)) = spec.get("facets") else { return Err(bad("facet requires 'facets'")) };
    if fs.is_empty() {
        return Err(bad("facet requires at least one facet definition"));
    }
    let mut out = Document::new();
    for (name, def) in fs {
        let Bson::Document(def) = def else { return Err(bad(format!("facet '{name}' must be an object"))) };
        let path = def.get_str("path").map_err(|_| bad(format!("facet '{name}' requires a 'path'")))?;
        let buckets: Vec<Bson> = match def.get_str("type") {
            Ok("string") => {
                let n = match def.get("numBuckets") {
                    None => 10,
                    Some(v) => int_of(v).filter(|n| (1..=1000).contains(n)).ok_or_else(|| bad("facet numBuckets must be between 1 and 1000"))? as usize,
                };
                let mut counts: BTreeMap<String, i64> = BTreeMap::new();
                for d in hits {
                    let mut seen: Vec<&str> = Vec::new();
                    for v in values_at(d, path) {
                        if let Bson::String(s) = v {
                            if !seen.contains(&s.as_str()) {
                                seen.push(s);
                            }
                        }
                    }
                    for s in seen {
                        *counts.entry(s.to_string()).or_default() += 1;
                    }
                }
                let mut v: Vec<(String, i64)> = counts.into_iter().collect();
                v.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
                v.into_iter().take(n).map(|(s, c)| Bson::Document(doc! { "_id": s, "count": c })).collect()
            }
            Ok(t @ ("number" | "date")) => {
                let Some(Bson::Array(bounds)) = def.get("boundaries") else { return Err(bad("facet requires 'boundaries'")) };
                let key = |b: &Bson| -> Option<f64> {
                    if t == "number" {
                        num_f64(b)
                    } else if let Bson::DateTime(x) = b {
                        Some(x.timestamp_millis() as f64)
                    } else {
                        None
                    }
                };
                let ks: Vec<f64> = bounds.iter().map(|b| key(b).ok_or_else(|| bad(format!("facet boundaries must be {t}s")))).collect::<R<_>>()?;
                if ks.len() < 2 || ks.windows(2).any(|w| w[0] >= w[1]) {
                    return Err(bad("facet boundaries must be at least two ascending values"));
                }
                let mut counts = vec![0i64; ks.len() - 1];
                let mut other = 0i64;
                for d in hits {
                    let mut idx: Vec<Option<usize>> = Vec::new();
                    for v in values_at(d, path) {
                        if let Some(x) = key(v) {
                            let i = (0..ks.len() - 1).find(|i| x >= ks[*i] && x < ks[*i + 1]);
                            if !idx.contains(&i) {
                                idx.push(i);
                            }
                        }
                    }
                    for i in idx {
                        match i {
                            Some(i) => counts[i] += 1,
                            None => other += 1,
                        }
                    }
                }
                let mut v: Vec<Bson> = bounds.iter().zip(&counts).map(|(b, c)| Bson::Document(doc! { "_id": b.clone(), "count": *c })).collect();
                if let Ok(name) = def.get_str("default") {
                    v.push(Bson::Document(doc! { "_id": name, "count": other }));
                }
                v
            }
            _ => return Err(bad(format!("facet '{name}' needs a type of string, number or date"))),
        };
        out.insert(name.clone(), doc! { "buckets": buckets });
    }
    Ok(out)
}

// ---------------------------------------------------------------------------------------------
// highlight
// ---------------------------------------------------------------------------------------------

struct HTerm {
    sels: Vec<Sel>,
    terms: Vec<String>,
    fuzzy: Fuzzy,
    prefix: bool,
}

fn collect_terms(op: &Document, out: &mut Vec<HTerm>) {
    let Some((name, Bson::Document(spec))) = op.iter().next() else { return };
    let sels = parse_paths(spec.get("path")).unwrap_or_default();
    match name.as_str() {
        "text" => {
            let terms = queries(spec.get("query")).unwrap_or_default().iter().flat_map(|q| analyze(q)).collect();
            out.push(HTerm { sels, terms, fuzzy: Fuzzy::parse(spec.get("fuzzy")).unwrap_or_default(), prefix: false });
        }
        "phrase" => {
            let terms = queries(spec.get("query")).unwrap_or_default().iter().flat_map(|q| analyze(q)).collect();
            out.push(HTerm { sels, terms, fuzzy: Fuzzy::default(), prefix: false });
        }
        "autocomplete" => {
            let terms = queries(spec.get("query")).unwrap_or_default().iter().flat_map(|q| analyze(q)).collect();
            out.push(HTerm { sels, terms, fuzzy: Fuzzy::default(), prefix: true });
        }
        "queryString" => {
            if let (Ok(def), Ok(q)) = (spec.get_str("defaultPath"), spec.get_str("query")) {
                if let Ok(cl) = parse_query_string(q) {
                    qs_terms(&cl, def, out);
                }
            }
        }
        "compound" => {
            for k in ["must", "should", "filter"] {
                match spec.get(k) {
                    Some(Bson::Array(a)) => a.iter().filter_map(|x| x.as_document()).for_each(|x| collect_terms(x, out)),
                    Some(Bson::Document(x)) => collect_terms(x, out),
                    _ => {}
                }
            }
        }
        "embeddedDocument" => {
            if let Some(Bson::Document(o)) = spec.get("operator") {
                collect_terms(o, out);
            }
        }
        _ => {}
    }
}

fn qs_terms(clauses: &[(Occur, Q)], default: &str, out: &mut Vec<HTerm>) {
    for (o, q) in clauses {
        if *o == Occur::MustNot {
            continue;
        }
        match q {
            Q::Term { field, text, fuzzy, .. } if !is_pattern(text) => out.push(HTerm {
                sels: vec![Sel::Exact(field.clone().unwrap_or_else(|| default.to_string()))],
                terms: analyze(&unescape(text)),
                fuzzy: Fuzzy { max_edits: fuzzy.unwrap_or(0), prefix: 0 },
                prefix: false,
            }),
            Q::Phrase { field, text, .. } => out.push(HTerm { sels: vec![Sel::Exact(field.clone().unwrap_or_else(|| default.to_string()))], terms: analyze(text), fuzzy: Fuzzy::default(), prefix: false }),
            Q::Group { clauses, .. } => qs_terms(clauses, default, out),
            _ => {}
        }
    }
}

fn sel_covers(s: &Sel, label: &str) -> bool {
    match s {
        Sel::Exact(p) => p == label,
        Sel::Wild(prefix) => {
            let p = prefix.trim_end_matches('.');
            p.is_empty() || label == p || label.starts_with(&format!("{p}."))
        }
    }
}

fn string_leaves(v: &Bson, path: &str, out: &mut Vec<(String, String)>) {
    match v {
        Bson::String(s) => out.push((path.to_string(), s.clone())),
        Bson::Array(a) => a.iter().for_each(|x| string_leaves(x, path, out)),
        Bson::Document(d) => {
            for (k, x) in d {
                string_leaves(x, &if path.is_empty() { k.clone() } else { format!("{path}.{k}") }, out);
            }
        }
        _ => {}
    }
}

/// Sentence spans `[start, end)` in characters: a sentence ends after `.`, `!` or `?` followed by white space.
fn sentences(chars: &[char]) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    let mut start = 0;
    let mut i = 0;
    while i < chars.len() {
        if matches!(chars[i], '.' | '!' | '?') {
            let mut j = i + 1;
            while j < chars.len() && matches!(chars[j], '.' | '!' | '?') {
                j += 1;
            }
            if j >= chars.len() || chars[j].is_whitespace() {
                while j < chars.len() && chars[j].is_whitespace() {
                    j += 1;
                }
                out.push((start, j));
                start = j;
                i = j;
                continue;
            }
        }
        i += 1;
    }
    if start < chars.len() {
        out.push((start, chars.len()));
    }
    out
}

pub fn highlights(hl: &Document, opdoc: &Document, d: &Document) -> R<Bson> {
    let sels = parse_paths(hl.get("path"))?;
    let max_chars = match hl.get("maxCharsToExamine") {
        None => 500_000,
        Some(v) => int_of(v).filter(|n| *n > 0).ok_or_else(|| bad("highlight.maxCharsToExamine must be a positive integer"))? as usize,
    };
    let max_passages = match hl.get("maxNumPassages") {
        None => 5,
        Some(v) => int_of(v).filter(|n| *n > 0).ok_or_else(|| bad("highlight.maxNumPassages must be a positive integer"))? as usize,
    };
    let mut terms = Vec::new();
    collect_terms(opdoc, &mut terms);
    let mut leaves: Vec<(String, String)> = Vec::new();
    for s in &sels {
        match s {
            Sel::Exact(p) => {
                for v in path_values(d, p) {
                    string_leaves(v, p, &mut leaves);
                }
            }
            Sel::Wild(prefix) => {
                let p = prefix.trim_end_matches('.');
                if p.is_empty() {
                    string_leaves(&Bson::Document(d.clone()), "", &mut leaves);
                } else {
                    for v in path_values(d, p) {
                        string_leaves(v, p, &mut leaves);
                    }
                }
            }
        }
    }
    let mut passages: Vec<(f64, usize, Document)> = Vec::new();
    let mut order = 0usize;
    for (label, text) in leaves {
        let applicable: Vec<&HTerm> = terms.iter().filter(|t| t.sels.iter().any(|s| sel_covers(s, &label))).collect();
        if applicable.is_empty() {
            continue;
        }
        let chars: Vec<char> = text.chars().take(max_chars).collect();
        for (a, b) in sentences(&chars) {
            let sent: String = chars[a..b].iter().collect();
            let spans = analyze_spans(&sent);
            let sc: Vec<char> = sent.chars().collect();
            let mut hits: Vec<(usize, usize)> = Vec::new();
            for (tok, s, e) in &spans {
                let hit = applicable.iter().any(|t| {
                    t.terms.iter().any(|q| if t.prefix { tok.starts_with(q.as_str()) } else { t.fuzzy.matches(q, tok) })
                });
                if hit {
                    hits.push((*s, *e));
                }
            }
            if hits.is_empty() {
                continue;
            }
            let end = sc.iter().rposition(|c| !c.is_whitespace()).map_or(0, |p| p + 1);
            let mut texts = Vec::new();
            let mut at = 0usize;
            for (s, e) in &hits {
                if *s > at {
                    texts.push(Bson::Document(doc! { "value": sc[at..*s].iter().collect::<String>(), "type": "text" }));
                }
                texts.push(Bson::Document(doc! { "value": sc[*s..*e].iter().collect::<String>(), "type": "hit" }));
                at = *e;
            }
            if end > at {
                texts.push(Bson::Document(doc! { "value": sc[at..end].iter().collect::<String>(), "type": "text" }));
            }
            let score = hits.len() as f64;
            passages.push((score, order, doc! { "score": score, "path": label.clone(), "texts": texts }));
            order += 1;
        }
    }
    passages.sort_by(|x, y| y.0.partial_cmp(&x.0).unwrap_or(std::cmp::Ordering::Equal).then(x.1.cmp(&y.1)));
    passages.truncate(max_passages);
    Ok(Bson::Array(passages.into_iter().map(|(_, _, d)| Bson::Document(d)).collect()))
}
