//! Atlas Search (`$search`, `$searchMeta`) evaluated over the documents of the collection.
//!
//! Atlas runs these stages in a separate search process (`mongot`) against a search index; this server evaluates the
//! operators directly on the documents with the standard analyzer (Unicode word segmentation, lower-casing, no stop
//! words, no stemming), so no search index definition is needed. Supported operators: `text` (with `fuzzy` and
//! `matchCriteria`), `phrase` (with `slop`), `autocomplete` (edge-gram prefixes, `tokenOrder`), `compound` (`must`,
//! `mustNot`, `should`, `filter`, `minimumShouldMatch`), `equals`, `exists`, `range`, `in`, `wildcard` and `regex`.
//! Scores follow BM25 (Lucene's formula without its lossy length encoding), so they are close to, not identical with,
//! Atlas's. Not supported: `near`, `geoShape`, `geoWithin`, `moreLikeThis`, `queryString`, `embeddedDocument`,
//! `facet`, `highlight`, `scoreDetails`, `returnStoredSource` and synonyms.

use crate::error::MongoError;
use crate::query::{compare, path_values, type_rank};
use bson::{doc, Bson, Document};
use std::cell::RefCell;
use std::collections::HashMap;

type R<T> = Result<T, MongoError>;

/// Hidden field carrying the search score through a pipeline (`{$meta: "searchScore"}`).
pub const SCORE_KEY: &str = "\u{1}searchScore";

pub(crate) fn bad(msg: impl Into<String>) -> MongoError {
    MongoError::new(2, "BadValue", msg.into())
}

pub(crate) fn num_f64(b: &Bson) -> Option<f64> {
    crate::index::num_of(b)
}

pub(crate) fn int_of(b: &Bson) -> Option<i64> {
    match b {
        Bson::Int32(i) => Some(*i as i64),
        Bson::Int64(i) => Some(*i),
        Bson::Double(f) if f.fract() == 0.0 => Some(*f as i64),
        _ => None,
    }
}

// ---------------------------------------------------------------------------------------------
// Analysis
// ---------------------------------------------------------------------------------------------

/// Standard-analyzer tokens: lower-cased runs of letters and digits; an apostrophe, dot or colon between letters, and a
/// dot, comma or semicolon between digits, stay inside the token.
pub fn analyze(text: &str) -> Vec<String> {
    analyze_spans(text).into_iter().map(|(t, _, _)| t).collect()
}

/// [`analyze`] with the character offsets `[start, end)` of every token.
pub fn analyze_spans(text: &str) -> Vec<(String, usize, usize)> {
    let chars: Vec<char> = text.chars().collect();
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut start = 0usize;
    let word = |c: char| c.is_alphanumeric() || c == '_';
    for (i, &c) in chars.iter().enumerate() {
        if word(c) {
            if cur.is_empty() {
                start = i;
            }
            cur.extend(c.to_lowercase());
            continue;
        }
        let prev = i.checked_sub(1).map(|j| chars[j]);
        let next = chars.get(i + 1).copied();
        let joins = match (prev, next) {
            (Some(p), Some(n)) if !cur.is_empty() => match c {
                '\'' | '\u{2019}' | ':' => p.is_alphabetic() && n.is_alphabetic(),
                '.' => (p.is_alphabetic() && n.is_alphabetic()) || (p.is_ascii_digit() && n.is_ascii_digit()),
                ',' | ';' => p.is_ascii_digit() && n.is_ascii_digit(),
                _ => false,
            },
            _ => false,
        };
        if joins {
            cur.push(c);
        } else if !cur.is_empty() {
            out.push((std::mem::take(&mut cur), start, i));
        }
    }
    if !cur.is_empty() {
        out.push((cur, start, chars.len()));
    }
    out.into_iter().filter(|(t, _, _)| t.chars().count() <= 255).collect()
}

// ---------------------------------------------------------------------------------------------
// Paths
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) enum Sel {
    Exact(String),
    /// `{wildcard: "a.*"}`: every string under the prefix (`*` alone is the whole document).
    Wild(String),
}

impl Sel {
    fn key(&self) -> String {
        match self {
            Sel::Exact(p) => p.clone(),
            Sel::Wild(p) => format!("{p}*"),
        }
    }
}

pub(crate) fn parse_paths(b: Option<&Bson>) -> R<Vec<Sel>> {
    fn one(b: &Bson) -> R<Sel> {
        const MSG: &str = "path must be a string, an object with 'value' or 'wildcard', or an array of those";
        match b {
            Bson::String(s) => Ok(Sel::Exact(s.clone())),
            Bson::Document(d) => {
                if let Ok(w) = d.get_str("wildcard") {
                    return Ok(Sel::Wild(w.trim_end_matches('*').to_string()));
                }
                if let Ok(v) = d.get_str("value") {
                    return Ok(Sel::Exact(v.to_string()));
                }
                Err(bad(MSG))
            }
            _ => Err(bad(MSG)),
        }
    }
    match b {
        None => Err(bad("operator requires a 'path'")),
        Some(Bson::Array(a)) if !a.is_empty() => a.iter().map(one).collect(),
        Some(x) => Ok(vec![one(x)?]),
    }
}

fn collect_strings(v: &Bson, deep: bool, out: &mut Vec<String>) {
    match v {
        Bson::String(s) => out.push(s.clone()),
        Bson::Array(a) => a.iter().for_each(|x| collect_strings(x, deep, out)),
        Bson::Document(d) if deep => d.values().for_each(|x| collect_strings(x, deep, out)),
        _ => {}
    }
}

/// The string values of a document at a path selector, one entry per array element.
pub(crate) fn strings_at(d: &Document, sel: &Sel) -> Vec<String> {
    let mut out = Vec::new();
    match sel {
        Sel::Exact(p) => path_values(d, p).into_iter().for_each(|v| collect_strings(v, false, &mut out)),
        Sel::Wild(prefix) => {
            let prefix = prefix.trim_end_matches('.');
            if prefix.is_empty() {
                d.values().for_each(|v| collect_strings(v, true, &mut out));
            } else {
                path_values(d, prefix).into_iter().for_each(|v| collect_strings(v, true, &mut out));
            }
        }
    }
    out
}

/// All values (not only strings) at an exact path, arrays flattened.
pub(crate) fn values_at<'a>(d: &'a Document, p: &str) -> Vec<&'a Bson> {
    let mut out = Vec::new();
    for v in path_values(d, p) {
        match v {
            Bson::Array(a) => out.extend(a.iter()),
            other => out.push(other),
        }
    }
    out
}

// ---------------------------------------------------------------------------------------------
// Matching and scoring
// ---------------------------------------------------------------------------------------------

pub(crate) struct Ctx<'a> {
    pub(crate) docs: &'a [Document],
    /// `(selector, term, fuzziness)` -> number of documents containing the term.
    df: RefCell<HashMap<(String, String), usize>>,
    /// selector -> average field length in tokens.
    avg_len: RefCell<HashMap<String, f64>>,
    /// moreLikeThis: the terms chosen for a `like` specification.
    pub(crate) mlt: RefCell<HashMap<String, Vec<(String, String)>>>,
}

impl<'a> Ctx<'a> {
    pub(crate) fn doc_freq(&self, sel: &Sel, term: &str, fuzzy: &Fuzzy) -> usize {
        let key = (sel.key(), format!("{term}\u{0}{}\u{0}{}", fuzzy.max_edits, fuzzy.prefix));
        if let Some(n) = self.df.borrow().get(&key) {
            return *n;
        }
        let n = self.docs.iter().filter(|d| strings_at(d, sel).iter().any(|s| analyze(s).iter().any(|t| fuzzy.matches(term, t)))).count();
        self.df.borrow_mut().insert(key, n);
        n
    }

    fn avg(&self, sel: &Sel) -> f64 {
        if let Some(a) = self.avg_len.borrow().get(&sel.key()) {
            return *a;
        }
        let mut total = 0usize;
        let mut n = 0usize;
        for d in self.docs {
            let len: usize = strings_at(d, sel).iter().map(|s| analyze(s).len()).sum();
            if len > 0 {
                total += len;
                n += 1;
            }
        }
        let a = if n == 0 { 1.0 } else { total as f64 / n as f64 };
        self.avg_len.borrow_mut().insert(sel.key(), a);
        a
    }

    /// BM25 contribution of a term occurring `tf` times in a field of `dl` tokens.
    pub(crate) fn bm25(&self, sel: &Sel, term: &str, fuzzy: &Fuzzy, tf: usize, dl: usize) -> f64 {
        let n = self.docs.len() as f64;
        let df = self.doc_freq(sel, term, fuzzy) as f64;
        let idf = (1.0 + (n - df + 0.5) / (df + 0.5)).ln();
        let (k1, b) = (1.2, 0.75);
        let tf = tf as f64;
        idf * tf * (k1 + 1.0) / (tf + k1 * (1.0 - b + b * dl as f64 / self.avg(sel)))
    }
}

#[derive(Clone, Copy, Default)]
pub(crate) struct Fuzzy {
    pub(crate) max_edits: usize,
    pub(crate) prefix: usize,
}

impl Fuzzy {
    pub(crate) fn parse(b: Option<&Bson>) -> R<Fuzzy> {
        match b {
            None => Ok(Fuzzy::default()),
            Some(Bson::Document(d)) => {
                let me = match d.get("maxEdits") {
                    None => 2,
                    Some(v) => int_of(v).filter(|n| (1..=2).contains(n)).ok_or_else(|| bad("fuzzy.maxEdits must be 1 or 2"))? as usize,
                };
                let pl = match d.get("prefixLength") {
                    None => 0,
                    Some(v) => int_of(v).filter(|n| *n >= 0).ok_or_else(|| bad("fuzzy.prefixLength must be a non-negative integer"))? as usize,
                };
                Ok(Fuzzy { max_edits: me, prefix: pl })
            }
            Some(_) => Err(bad("fuzzy must be an object")),
        }
    }

    pub(crate) fn matches(&self, query: &str, term: &str) -> bool {
        if query == term {
            return true;
        }
        if self.max_edits == 0 {
            return false;
        }
        let (q, t): (Vec<char>, Vec<char>) = (query.chars().collect(), term.chars().collect());
        if q.len() < self.prefix || t.len() < self.prefix || q[..self.prefix] != t[..self.prefix] {
            return false;
        }
        osa_distance(&q, &t) <= self.max_edits
    }
}

/// Damerau-Levenshtein distance (optimal string alignment).
fn osa_distance(a: &[char], b: &[char]) -> usize {
    let (n, m) = (a.len(), b.len());
    let mut d = vec![vec![0usize; m + 1]; n + 1];
    for (i, row) in d.iter_mut().enumerate() {
        row[0] = i;
    }
    for j in 0..=m {
        d[0][j] = j;
    }
    for i in 1..=n {
        for j in 1..=m {
            let cost = usize::from(a[i - 1] != b[j - 1]);
            d[i][j] = (d[i - 1][j] + 1).min(d[i][j - 1] + 1).min(d[i - 1][j - 1] + cost);
            if i > 1 && j > 1 && a[i - 1] == b[j - 2] && a[i - 2] == b[j - 1] {
                d[i][j] = d[i][j].min(d[i - 2][j - 2] + 1);
            }
        }
    }
    d[n][m]
}

pub(crate) fn queries(b: Option<&Bson>) -> R<Vec<String>> {
    match b {
        Some(Bson::String(s)) => Ok(vec![s.clone()]),
        Some(Bson::Array(a)) if !a.is_empty() && a.iter().all(|x| matches!(x, Bson::String(_))) => Ok(a.iter().filter_map(|x| x.as_str().map(str::to_string)).collect()),
        _ => Err(bad("operator requires a 'query' that is a string or an array of strings")),
    }
}

pub(crate) fn apply_score_option(spec: &Document, score: f64) -> R<f64> {
    match spec.get("score") {
        None => Ok(score),
        Some(Bson::Document(s)) => {
            if let Some(Bson::Document(b)) = s.get("boost") {
                let v = b.get("value").and_then(num_f64).ok_or_else(|| bad("score.boost.value must be a number"))?;
                return Ok(score * v);
            }
            if s.contains_key("embedded") {
                return Ok(score);
            }
            if let Some(Bson::Document(c)) = s.get("constant") {
                return c.get("value").and_then(num_f64).ok_or_else(|| bad("score.constant.value must be a number"));
            }
            Err(bad("score supports only 'boost' and 'constant'"))
        }
        Some(_) => Err(bad("score must be an object")),
    }
}

pub(crate) fn eval(op: &Document, d: &Document, ctx: &Ctx) -> R<Option<f64>> {
    if op.len() != 1 {
        return Err(bad("a search operator document must contain exactly one operator"));
    }
    let (name, arg) = op.iter().next().unwrap();
    let Bson::Document(spec) = arg else { return Err(bad(format!("{name} must be an object"))) };
    let r = match name.as_str() {
        "text" => text(spec, d, ctx)?,
        "phrase" => phrase(spec, d, ctx)?,
        "autocomplete" => autocomplete(spec, d, ctx)?,
        "compound" => compound(spec, d, ctx)?,
        "equals" => {
            let sels = parse_paths(spec.get("path"))?;
            let want = spec.get("value").ok_or_else(|| bad("equals requires a 'value'"))?;
            let hit = sels.iter().any(|s| matches!(s, Sel::Exact(p) if values_at(d, p).iter().any(|v| crate::query::equal(v, want))));
            hit.then_some(1.0)
        }
        "in" => {
            let sels = parse_paths(spec.get("path"))?;
            let wants: Vec<Bson> = match spec.get("value") {
                Some(Bson::Array(a)) => a.clone(),
                Some(v) => vec![v.clone()],
                None => return Err(bad("in requires a 'value'")),
            };
            let hit = sels.iter().any(|s| matches!(s, Sel::Exact(p) if values_at(d, p).iter().any(|v| wants.iter().any(|w| crate::query::equal(v, w)))));
            hit.then_some(1.0)
        }
        "exists" => {
            let sels = parse_paths(spec.get("path"))?;
            let hit = sels.iter().any(|s| matches!(s, Sel::Exact(p) if values_at(d, p).iter().any(|v| !matches!(v, Bson::Null))));
            hit.then_some(1.0)
        }
        "range" => {
            let sels = parse_paths(spec.get("path"))?;
            let mut bounds: Vec<(&str, &Bson)> = Vec::new();
            for k in ["gt", "gte", "lt", "lte"] {
                if let Some(v) = spec.get(k) {
                    bounds.push((k, v));
                }
            }
            if bounds.is_empty() {
                return Err(bad("range requires at least one of gt, gte, lt, lte"));
            }
            let ok = |v: &Bson| {
                bounds.iter().all(|(k, b)| {
                    if type_rank(v) != type_rank(b) {
                        return false;
                    }
                    let c = compare(v, b);
                    match *k {
                        "gt" => c.is_gt(),
                        "gte" => c.is_ge(),
                        "lt" => c.is_lt(),
                        _ => c.is_le(),
                    }
                })
            };
            let hit = sels.iter().any(|s| matches!(s, Sel::Exact(p) if values_at(d, p).iter().any(|v| ok(v))));
            hit.then_some(1.0)
        }
        "wildcard" | "regex" => {
            let sels = parse_paths(spec.get("path"))?;
            let qs = queries(spec.get("query"))?;
            let analyzed = matches!(spec.get("allowAnalyzedField"), Some(Bson::Boolean(true)));
            let pats: Vec<String> = qs.iter().map(|q| format!("^(?:{})$", if name == "wildcard" { wildcard_to_regex(q) } else { q.clone() })).collect();
            let mut hit = false;
            for s in &sels {
                for v in strings_at(d, s) {
                    let candidates: Vec<String> = if analyzed { analyze(&v) } else { vec![v.clone()] };
                    for c in &candidates {
                        for p in &pats {
                            if crate::regex::matches_str(p, "s", c)? {
                                hit = true;
                            }
                        }
                    }
                }
            }
            hit.then_some(1.0)
        }
        "near" => crate::search_more::near(spec, d)?,
        "geoWithin" => crate::search_more::geo_within(spec, d)?,
        "geoShape" => crate::search_more::geo_shape(spec, d)?,
        "moreLikeThis" => crate::search_more::more_like_this(spec, d, ctx)?,
        "queryString" => crate::search_more::query_string(spec, d, ctx)?,
        "embeddedDocument" => crate::search_more::embedded_document(spec, d, ctx)?,
        other => return Err(bad(format!("{other} is not supported by this server's $search"))),
    };
    match r {
        Some(s) => Ok(Some(apply_score_option(spec, s)?)),
        None => Ok(None),
    }
}

fn escape_re(c: char) -> String {
    if "\\.+*?()|[]{}^$#&-~".contains(c) {
        format!("\\{c}")
    } else {
        c.to_string()
    }
}

pub(crate) fn wildcard_to_regex(p: &str) -> String {
    let mut out = String::new();
    let mut esc = false;
    for c in p.chars() {
        if esc {
            out.push_str(&escape_re(c));
            esc = false;
            continue;
        }
        match c {
            '\\' => esc = true,
            '*' => out.push_str(".*"),
            '?' => out.push('.'),
            other => out.push_str(&escape_re(other)),
        }
    }
    out
}

fn text(spec: &Document, d: &Document, ctx: &Ctx) -> R<Option<f64>> {
    let sels = parse_paths(spec.get("path"))?;
    let fuzzy = Fuzzy::parse(spec.get("fuzzy"))?;
    let all = match spec.get_str("matchCriteria") {
        Ok("all") => true,
        Ok("any") | Err(_) => false,
        Ok(other) => return Err(bad(format!("matchCriteria must be 'any' or 'all', got '{other}'"))),
    };
    let mut terms: Vec<String> = Vec::new();
    for q in queries(spec.get("query"))? {
        for t in analyze(&q) {
            if !terms.contains(&t) {
                terms.push(t);
            }
        }
    }
    if terms.is_empty() {
        return Ok(None);
    }
    let mut total = 0.0;
    let mut matched_any = false;
    let mut field_all = false;
    for sel in &sels {
        let toks: Vec<String> = strings_at(d, sel).iter().flat_map(|s| analyze(s)).collect();
        let dl = toks.len();
        let mut hits = 0;
        for t in &terms {
            let tf = toks.iter().filter(|x| fuzzy.matches(t, x)).count();
            if tf > 0 {
                hits += 1;
                matched_any = true;
                total += ctx.bm25(sel, t, &fuzzy, tf, dl);
            }
        }
        if hits == terms.len() {
            field_all = true;
        }
    }
    let ok = if all { field_all } else { matched_any };
    Ok(ok.then_some(total))
}

fn phrase(spec: &Document, d: &Document, ctx: &Ctx) -> R<Option<f64>> {
    let sels = parse_paths(spec.get("path"))?;
    let slop = match spec.get("slop") {
        None => 0,
        Some(v) => int_of(v).filter(|n| *n >= 0).ok_or_else(|| bad("slop must be a non-negative integer"))? as usize,
    };
    let phrases: Vec<Vec<String>> = queries(spec.get("query"))?.iter().map(|q| analyze(q)).filter(|p| !p.is_empty()).collect();
    let mut best: Option<f64> = None;
    for sel in &sels {
        for value in strings_at(d, sel) {
            let toks = analyze(&value);
            for p in &phrases {
                if let Some(count) = phrase_count(&toks, p, slop) {
                    let s: f64 = p.iter().map(|t| ctx.bm25(sel, t, &Fuzzy::default(), count, toks.len())).sum();
                    best = Some(best.unwrap_or(0.0) + s);
                }
            }
        }
    }
    Ok(best)
}

/// Number of occurrences of the phrase in the tokens, allowing `slop` extra positions in total.
pub(crate) fn phrase_count(toks: &[String], phrase: &[String], slop: usize) -> Option<usize> {
    let mut count = 0;
    for start in 0..toks.len() {
        if toks[start] != phrase[0] {
            continue;
        }
        let mut pos = start;
        let mut gaps = 0usize;
        let mut ok = true;
        for t in &phrase[1..] {
            let room = slop.saturating_sub(gaps);
            match toks.iter().enumerate().skip(pos + 1).take(room + 1).find(|(_, x)| *x == t) {
                Some((off, _)) => {
                    gaps += off - pos - 1;
                    pos = off;
                }
                None => {
                    ok = false;
                    break;
                }
            }
        }
        if ok && gaps <= slop {
            count += 1;
        }
    }
    (count > 0).then_some(count)
}

fn autocomplete(spec: &Document, d: &Document, ctx: &Ctx) -> R<Option<f64>> {
    let sels = parse_paths(spec.get("path"))?;
    let fuzzy = Fuzzy::parse(spec.get("fuzzy"))?;
    let sequential = match spec.get_str("tokenOrder") {
        Ok("sequential") => true,
        Ok("any") | Err(_) => false,
        Ok(other) => return Err(bad(format!("tokenOrder must be 'any' or 'sequential', got '{other}'"))),
    };
    let mut qtoks: Vec<String> = Vec::new();
    for q in queries(spec.get("query"))? {
        qtoks.extend(analyze(&q));
    }
    if qtoks.is_empty() {
        return Ok(None);
    }
    let gram_ok = |q: &str| q.chars().count() >= 2;
    let prefix_of = |q: &str, t: &str| {
        if fuzzy.max_edits > 0 {
            let n = q.chars().count().min(t.chars().count());
            let tp: String = t.chars().take(n).collect();
            fuzzy.matches(q, &tp)
        } else {
            let t15: String = t.chars().take(15).collect();
            t15.starts_with(q)
        }
    };
    let mut best: Option<f64> = None;
    for sel in &sels {
        for value in strings_at(d, sel) {
            let toks = analyze(&value);
            let ok = if sequential {
                toks.windows(qtoks.len()).any(|w| w.iter().zip(&qtoks).all(|(t, q)| gram_ok(q) && prefix_of(q, t)))
            } else {
                qtoks.iter().all(|q| gram_ok(q) && toks.iter().any(|t| prefix_of(q, t)))
            };
            if ok {
                let s: f64 = qtoks.iter().map(|q| ctx.bm25(sel, q, &Fuzzy::default(), 1, toks.len().max(1))).sum();
                best = Some(best.unwrap_or(0.0).max(s));
            }
        }
    }
    Ok(best)
}

fn clauses(spec: &Document, key: &str) -> R<Vec<Document>> {
    match spec.get(key) {
        None => Ok(vec![]),
        Some(Bson::Array(a)) => a.iter().map(|x| x.as_document().cloned().ok_or_else(|| bad(format!("compound.{key} clauses must be objects")))).collect(),
        Some(Bson::Document(x)) => Ok(vec![x.clone()]),
        Some(_) => Err(bad(format!("compound.{key} must be an array of operators"))),
    }
}

fn compound(spec: &Document, d: &Document, ctx: &Ctx) -> R<Option<f64>> {
    for k in spec.keys() {
        if !matches!(k.as_str(), "must" | "mustNot" | "should" | "filter" | "minimumShouldMatch" | "score") {
            return Err(bad(format!("unknown compound option '{k}'")));
        }
    }
    let (must, must_not, should, filter) = (clauses(spec, "must")?, clauses(spec, "mustNot")?, clauses(spec, "should")?, clauses(spec, "filter")?);
    if must.is_empty() && must_not.is_empty() && should.is_empty() && filter.is_empty() {
        return Err(bad("compound requires at least one of must, mustNot, should, filter"));
    }
    let min_should = match spec.get("minimumShouldMatch") {
        None => 0,
        Some(v) => int_of(v).filter(|n| *n >= 0).ok_or_else(|| bad("minimumShouldMatch must be a non-negative integer"))? as usize,
    };
    let mut score = 0.0;
    for c in &must {
        match eval(c, d, ctx)? {
            Some(s) => score += s,
            None => return Ok(None),
        }
    }
    for c in &filter {
        if eval(c, d, ctx)?.is_none() {
            return Ok(None);
        }
    }
    for c in &must_not {
        if eval(c, d, ctx)?.is_some() {
            return Ok(None);
        }
    }
    let mut should_hits = 0;
    for c in &should {
        if let Some(s) = eval(c, d, ctx)? {
            should_hits += 1;
            score += s;
        }
    }
    // a compound with only should (and mustNot) clauses needs at least one should clause to match
    let need = if must.is_empty() && filter.is_empty() && !should.is_empty() { min_should.max(1) } else { min_should };
    if should_hits < need {
        return Ok(None);
    }
    Ok(Some(score))
}

// ---------------------------------------------------------------------------------------------
// Stages
// ---------------------------------------------------------------------------------------------

/// Run a `$search` / `$searchMeta` stage over every document of the collection.
pub fn run_stage(name: &str, arg: &Bson, docs: Vec<Document>) -> R<Vec<Document>> {
    let Bson::Document(spec) = arg else { return Err(MongoError::new(7, "NoSuchKey", format!("{name} must be an object"))) };
    let mut count_total = false;
    let mut sort: Option<&Document> = None;
    let mut highlight: Option<&Document> = None;
    let mut operator: Option<(String, Bson)> = None;
    for (k, v) in spec {
        match k.as_str() {
            "index" | "tracking" | "concurrent" => {}
            "returnStoredSource" => {
                if matches!(v, Bson::Boolean(true)) {
                    return Err(bad("returnStoredSource is not supported by this server's $search"));
                }
            }
            "count" => {
                let Bson::Document(c) = v else { return Err(bad("count must be an object")) };
                count_total = c.get_str("type").ok() == Some("total");
            }
            "sort" => match v {
                Bson::Document(s) if name == "$search" => sort = Some(s),
                _ => return Err(bad("sort must be an object")),
            },
            "highlight" => match v {
                Bson::Document(h) if name == "$search" => highlight = Some(h),
                _ => return Err(bad("highlight must be an object")),
            },
            "scoreDetails" | "searchAfter" | "searchBefore" => return Err(bad(format!("{k} is not supported by this server's $search"))),
            _ => {
                if operator.is_some() {
                    return Err(bad(format!("{name} takes exactly one operator")));
                }
                if !matches!(v, Bson::Document(_)) {
                    return Err(bad(format!("{k} must be an object")));
                }
                operator = Some((k.clone(), v.clone()));
            }
        }
    }
    let Some((opname, opspec)) = operator else { return Err(bad(format!("{name} requires an operator"))) };
    let ctx = Ctx { docs: &docs, df: RefCell::new(HashMap::new()), avg_len: RefCell::new(HashMap::new()), mlt: RefCell::new(HashMap::new()) };
    // the `facet` collector wraps an operator (every document when it is absent)
    let (opdoc, facet_spec) = if opname == "facet" {
        let Bson::Document(fs) = &opspec else { return Err(bad("facet must be an object")) };
        let inner = match fs.get("operator") {
            None => None,
            Some(Bson::Document(o)) => Some(o.clone()),
            Some(_) => return Err(bad("facet.operator must be an object")),
        };
        (inner, Some(fs.clone()))
    } else {
        (Some(doc! { opname: opspec }), None)
    };
    let mut hits: Vec<(f64, usize)> = Vec::new();
    for (i, d) in docs.iter().enumerate() {
        match &opdoc {
            Some(op) => {
                if let Some(s) = eval(op, d, &ctx)? {
                    hits.push((s, i));
                }
            }
            None => hits.push((1.0, i)),
        }
    }
    let n = hits.len() as i64;
    let mut meta = if count_total { doc! { "count": { "total": n } } } else { doc! { "count": { "lowerBound": n } } };
    if let Some(fs) = &facet_spec {
        let hit_docs: Vec<&Document> = hits.iter().map(|(_, i)| &docs[*i]).collect();
        meta.insert("facet", crate::search_more::facets(fs, &hit_docs)?);
    }
    if name == "$searchMeta" {
        return Ok(vec![meta]);
    }
    hits.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal).then(a.1.cmp(&b.1)));
    let with_meta = facet_spec.is_some() || count_total;
    let mut out: Vec<Document> = Vec::with_capacity(hits.len());
    for (s, i) in hits {
        let mut d = docs[i].clone();
        d.insert(SCORE_KEY, s);
        if with_meta {
            d.insert(crate::search_more::META_KEY, meta.clone());
        }
        if let (Some(h), Some(op)) = (highlight, &opdoc) {
            d.insert(crate::search_more::HIGHLIGHT_KEY, crate::search_more::highlights(h, op, &docs[i])?);
        }
        out.push(d);
    }
    if let Some(s) = sort {
        crate::query::sort_docs(&mut out, &crate::query::parse_sort(s)?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn analyzer() {
        assert_eq!(analyze("The Quick-Brown fox's e-mail, 3.14 a_b user@example.com"), vec!["the", "quick", "brown", "fox's", "e", "mail", "3.14", "a_b", "user", "example.com"]);
    }

    #[test]
    fn fuzzy_and_phrase() {
        let f = Fuzzy { max_edits: 1, prefix: 0 };
        assert!(f.matches("fox", "fax") && f.matches("form", "from") && !f.matches("fox", "dog"));
        let t = analyze("the quick brown fox jumps");
        assert!(phrase_count(&t, &analyze("quick brown"), 0).is_some());
        assert!(phrase_count(&t, &analyze("quick fox"), 0).is_none());
        assert!(phrase_count(&t, &analyze("quick fox"), 1).is_some());
    }
}
