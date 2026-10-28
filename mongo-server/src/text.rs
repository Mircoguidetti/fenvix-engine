//! Text search: tokenizer, stemming, stop words, text index entries, `$text` queries and scores.
//!
//! Behaviour was derived from MongoDB 7.0 (text index version 3) by probing it:
//!
//! * tokens are split at a fixed set of delimiter code points (`DELIMS`; the apostrophe too, except in English);
//! * a token is lower-cased, dropped when it is a stop word of the language, stemmed (Snowball), and only then
//!   stripped of diacritics (so `résumé` and `RESUME` are different terms in English);
//! * a field's string gets scores per term: `weight * freq * coeff * adjustment` (see [`score_string`]);
//! * a query is OR over its terms, `"phrases"` must occur as raw case-insensitive substrings of an indexed field,
//!   `-term` and `-"phrase"` exclude documents.

use crate::error::MongoError;
use crate::query::path_values;
use bson::{Bson, Document};
use rust_stemmers::{Algorithm, Stemmer};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::OnceLock;
use unicode_normalization::char::is_combining_mark;
use unicode_normalization::UnicodeNormalization;

type R<T> = Result<T, MongoError>;

/// Name of the hidden field that carries the text score of a document through a command.
pub const SCORE_KEY: &str = "\u{1}textScore";

#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
pub enum Lang {
    None,
    Danish,
    Dutch,
    English,
    Finnish,
    French,
    German,
    Hungarian,
    Italian,
    Norwegian,
    Portuguese,
    Romanian,
    Russian,
    Spanish,
    Swedish,
    Turkish,
}

impl Lang {
    pub fn name(self) -> &'static str {
        match self {
            Lang::None => "none",
            Lang::Danish => "danish",
            Lang::Dutch => "dutch",
            Lang::English => "english",
            Lang::Finnish => "finnish",
            Lang::French => "french",
            Lang::German => "german",
            Lang::Hungarian => "hungarian",
            Lang::Italian => "italian",
            Lang::Norwegian => "norwegian",
            Lang::Portuguese => "portuguese",
            Lang::Romanian => "romanian",
            Lang::Russian => "russian",
            Lang::Spanish => "spanish",
            Lang::Swedish => "swedish",
            Lang::Turkish => "turkish",
        }
    }

    fn algorithm(self) -> Option<Algorithm> {
        Some(match self {
            Lang::None => return None,
            Lang::Danish => Algorithm::Danish,
            Lang::Dutch => Algorithm::Dutch,
            Lang::English => Algorithm::English,
            Lang::Finnish => Algorithm::Finnish,
            Lang::French => Algorithm::French,
            Lang::German => Algorithm::German,
            Lang::Hungarian => Algorithm::Hungarian,
            Lang::Italian => Algorithm::Italian,
            Lang::Norwegian => Algorithm::Norwegian,
            Lang::Portuguese => Algorithm::Portuguese,
            Lang::Romanian => Algorithm::Romanian,
            Lang::Russian => Algorithm::Russian,
            Lang::Spanish => Algorithm::Spanish,
            Lang::Swedish => Algorithm::Swedish,
            Lang::Turkish => Algorithm::Turkish,
        })
    }
}

/// Language by name or ISO 639-1 code (case-insensitive), as MongoDB accepts them.
pub fn parse_lang(s: &str) -> Option<Lang> {
    Some(match s.to_ascii_lowercase().as_str() {
        "none" => Lang::None,
        "danish" | "da" => Lang::Danish,
        "dutch" | "nl" => Lang::Dutch,
        "english" | "en" => Lang::English,
        "finnish" | "fi" => Lang::Finnish,
        "french" | "fr" => Lang::French,
        "german" | "de" => Lang::German,
        "hungarian" | "hu" => Lang::Hungarian,
        "italian" | "it" => Lang::Italian,
        "norwegian" | "nb" => Lang::Norwegian,
        "portuguese" | "pt" => Lang::Portuguese,
        "romanian" | "ro" => Lang::Romanian,
        "russian" | "ru" => Lang::Russian,
        "spanish" | "es" => Lang::Spanish,
        "swedish" | "sv" => Lang::Swedish,
        "turkish" | "tr" => Lang::Turkish,
        _ => return None,
    })
}

// ---------------------------------------------------------------------------------------------
// Tokenizer, folding, stop words, stemming
// ---------------------------------------------------------------------------------------------

/// Delimiter code points of MongoDB's text tokenizer (inclusive ranges), found by probing every code point
/// below U+12000 and the planes with punctuation. The apostrophe (U+0027) is a delimiter in every language but English.
const DELIMS: &[(u32, u32)] = &[
    (0x9, 0xd), (0x20, 0x26), (0x28, 0x2f), (0x3a, 0x40), (0x5b, 0x5e), (0x60, 0x60), (0x7b, 0x7e), (0x85, 0x85), (0xa0, 0xa7), (0xa9, 0xa9), (0xab, 0xae),
    (0xb0, 0xb1), (0xb6, 0xb6), (0xbb, 0xbb), (0xbf, 0xbf), (0xd7, 0xd7), (0xf7, 0xf7), (0x37e, 0x37e), (0x387, 0x387), (0x589, 0x58a), (0x5be, 0x5be),
    (0x5c3, 0x5c3), (0x60c, 0x60c), (0x61b, 0x61b), (0x61f, 0x61f), (0x6d4, 0x6d4), (0x700, 0x70a), (0x70c, 0x70c), (0x7f8, 0x7f9), (0x830, 0x83e),
    (0x85e, 0x85e), (0x964, 0x965), (0xe5a, 0xe5b), (0xf08, 0xf08), (0xf0d, 0xf12), (0x104a, 0x104b), (0x1361, 0x1368), (0x1400, 0x1400), (0x166d, 0x166e),
    (0x1680, 0x1680), (0x16eb, 0x16ed), (0x1735, 0x1736), (0x17d4, 0x17d6), (0x17da, 0x17da), (0x1802, 0x1806), (0x1808, 0x1809), (0x1944, 0x1945),
    (0x1aa8, 0x1aab), (0x1b5a, 0x1b5b), (0x1b5d, 0x1b5f), (0x1c3b, 0x1c3f), (0x1c7e, 0x1c7f), (0x2000, 0x200a), (0x2010, 0x2029), (0x202f, 0x203e),
    (0x2041, 0x2053), (0x2055, 0x205f), (0x207b, 0x207b), (0x208b, 0x208b), (0x2190, 0x245f), (0x2500, 0x2775), (0x2794, 0x2bff), (0x2e00, 0x2e7f),
    (0x3000, 0x3003), (0x3008, 0x3020), (0x3030, 0x3030), (0x30a0, 0x30a0), (0x30fb, 0x30fb), (0xa4fe, 0xa4ff), (0xa60d, 0xa60f), (0xa6f3, 0xa6f7),
    (0xa876, 0xa877), (0xa8ce, 0xa8cf), (0xa92f, 0xa92f), (0xa9c7, 0xa9c9), (0xaa5d, 0xaa5f), (0xaadf, 0xaadf), (0xaaf0, 0xaaf1), (0xabeb, 0xabeb),
    (0xfd3e, 0xfd3f), (0xfe31, 0xfe32), (0xfe41, 0xfe46), (0xfe50, 0xfe52), (0xfe54, 0xfe58), (0xfe63, 0xfe63), (0xff01, 0xff02), (0xff07, 0xff07),
    (0xff0c, 0xff0e), (0xff1a, 0xff1b), (0xff1f, 0xff1f), (0xff61, 0xff65), (0x1039f, 0x1039f), (0x103d0, 0x103d0), (0x10857, 0x10857), (0x1091f, 0x1091f),
    (0x10a56, 0x10a57), (0x10af0, 0x10af5), (0x10b3a, 0x10b3f), (0x10b99, 0x10b9c), (0x11047, 0x1104d), (0x110be, 0x110c1), (0x11141, 0x11143),
    (0x111c5, 0x111c6), (0x111cd, 0x111cd), (0x111de, 0x111df), (0x11238, 0x1123c), (0x112a9, 0x112a9), (0x115c2, 0x115c5), (0x115c9, 0x115d7),
    (0x11641, 0x11642), (0x1173c, 0x1173e), (0x1da87, 0x1da8a),
];

fn is_delim(c: char, lang: Lang) -> bool {
    if c == '\'' {
        return lang != Lang::English;
    }
    let cp = c as u32;
    DELIMS.binary_search_by(|&(lo, hi)| if cp < lo { std::cmp::Ordering::Greater } else if cp > hi { std::cmp::Ordering::Less } else { std::cmp::Ordering::Equal }).is_ok()
}

pub fn tokenize(s: &str, lang: Lang) -> Vec<&str> {
    s.split(|c| is_delim(c, lang)).filter(|t| !t.is_empty()).collect()
}

fn lower(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if c == '\u{130}' || c == '\u{2c2f}' {
            out.push(c);
        } else {
            out.extend(c.to_lowercase());
        }
    }
    out
}

fn strip_diacritics(s: &str) -> String {
    if s.is_ascii() {
        return s.to_string();
    }
    s.nfd().filter(|c| !is_combining_mark(*c) || (0x483..=0x489).contains(&(*c as u32))).nfc().collect()
}

fn stop_words(lang: Lang) -> &'static HashSet<String> {
    static ALL: OnceLock<HashMap<String, HashSet<String>>> = OnceLock::new();
    static EMPTY: OnceLock<HashSet<String>> = OnceLock::new();
    let all = ALL.get_or_init(|| {
        let raw: HashMap<String, Vec<String>> = serde_json::from_str(include_str!("text_data/stopwords.json")).unwrap_or_default();
        raw.into_iter().map(|(k, v)| (k, v.into_iter().collect())).collect()
    });
    match all.get(lang.name()) {
        Some(s) => s,
        None => EMPTY.get_or_init(HashSet::new),
    }
}

fn stemmer(lang: Lang) -> Option<&'static Stemmer> {
    static ALL: OnceLock<HashMap<Lang, Stemmer>> = OnceLock::new();
    let all = ALL.get_or_init(|| {
        [Lang::Danish, Lang::Dutch, Lang::English, Lang::Finnish, Lang::French, Lang::German, Lang::Hungarian, Lang::Italian, Lang::Norwegian, Lang::Portuguese, Lang::Romanian, Lang::Russian, Lang::Spanish, Lang::Swedish, Lang::Turkish]
            .into_iter()
            .filter_map(|l| l.algorithm().map(|a| (l, Stemmer::create(a))))
            .collect()
    });
    all.get(&lang)
}

/// Options that make a comparison more exact than the index terms.
#[derive(Clone, Copy, Default, Debug)]
pub struct Sens {
    pub case: bool,
    pub diacritic: bool,
}

/// Terms of `text` in order, with stop words removed and every remaining token stemmed.
pub fn terms(text: &str, lang: Lang, sens: Sens) -> Vec<String> {
    let mut out = Vec::new();
    for tok in tokenize(text, lang) {
        let lc = lower(tok);
        if lang != Lang::None && stop_words(lang).contains(&lc) {
            continue;
        }
        let base: &str = if sens.case { tok } else { &lc };
        let stemmed = match stemmer(lang) {
            Some(s) => s.stem(base).into_owned(),
            None => base.to_string(),
        };
        out.push(if sens.diacritic { stemmed } else { strip_diacritics(&stemmed) });
    }
    out
}

// ---------------------------------------------------------------------------------------------
// Index specification and scoring
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct TextSpec {
    /// Indexed text paths with their weights (`$**` = every string of the document).
    pub fields: Vec<(String, f64)>,
    pub wildcard: bool,
    pub default_lang: Lang,
    pub override_field: String,
    /// Key fields before / after the text fields (equality prefix and scalar suffix).
    pub prefix: Vec<(String, bool)>,
    pub suffix: Vec<(String, bool)>,
}

impl TextSpec {
    /// From the stored index spec: `key` is the user's key pattern (kept in `textKey`), weights and languages are
    /// the fields `listIndexes` shows.
    pub fn from_spec(spec: &Document) -> Option<TextSpec> {
        let user_key = spec.get_document("textKey").ok()?;
        let mut prefix = Vec::new();
        let mut suffix = Vec::new();
        let mut seen_text = false;
        let mut paths: Vec<String> = Vec::new();
        for (k, v) in user_key {
            if matches!(v, Bson::String(s) if s == "text") {
                seen_text = true;
                paths.push(k.clone());
            } else {
                let asc = !matches!(v, Bson::Int32(i) if *i < 0) && !matches!(v, Bson::Int64(i) if *i < 0) && !matches!(v, Bson::Double(f) if *f < 0.0);
                if seen_text {
                    suffix.push((k.clone(), asc));
                } else {
                    prefix.push((k.clone(), asc));
                }
            }
        }
        let weights = spec.get_document("weights").ok()?;
        let fields: Vec<(String, f64)> = weights.iter().map(|(k, v)| (k.clone(), crate::index::num_of(v).unwrap_or(1.0))).collect();
        let wildcard = fields.iter().any(|(f, _)| f == "$**") || paths.iter().any(|p| p == "$**");
        Some(TextSpec {
            fields,
            wildcard,
            default_lang: spec.get_str("default_language").ok().and_then(parse_lang).unwrap_or(Lang::English),
            override_field: spec.get_str("language_override").unwrap_or("language").to_string(),
            prefix,
            suffix,
        })
    }

    fn weight_of(&self, path: &str) -> f64 {
        self.fields.iter().find(|(f, _)| f == path).map(|(_, w)| *w).or_else(|| self.fields.iter().find(|(f, _)| f == "$**").map(|(_, w)| *w)).unwrap_or(1.0)
    }
}

/// The language of a document (its override field, or the index default).
pub fn doc_lang(spec: &TextSpec, d: &Document) -> R<Lang> {
    match d.get(&spec.override_field) {
        Some(Bson::String(s)) => parse_lang(s).ok_or_else(|| MongoError::new(17262, "Location17262", format!("language override unsupported: {s}"))),
        Some(Bson::Null) | None => Ok(spec.default_lang),
        Some(other) => Err(MongoError::new(17261, "Location17261", format!("language override must be a string, found {}", crate::update::type_name(other)))),
    }
}

/// Every string value of `d` that the index covers, with the weight of its field.
fn indexed_strings(spec: &TextSpec, d: &Document) -> Vec<(f64, String)> {
    fn walk(v: &Bson, w: f64, out: &mut Vec<(f64, String)>, deep: bool) {
        match v {
            Bson::String(s) => out.push((w, s.clone())),
            Bson::Array(a) => a.iter().for_each(|x| walk(x, w, out, deep)),
            Bson::Document(sub) if deep => sub.values().for_each(|x| walk(x, w, out, deep)),
            _ => {}
        }
    }
    let mut out = Vec::new();
    if spec.wildcard {
        for (k, v) in d {
            if k == &spec.override_field || k == "_id" {
                // the language override is a setting, not text; `_id` is rarely wanted (MongoDB indexes it if it is a string, keep that)
                if k == &spec.override_field {
                    continue;
                }
            }
            walk(v, spec.weight_of(k), &mut out, true);
        }
        // explicitly weighted paths add their own weight on top (only when they differ from the wildcard's)
        return out;
    }
    for (path, w) in &spec.fields {
        for v in path_values(d, path) {
            walk(v, *w, &mut out, false);
        }
    }
    out
}

struct Occ {
    count: u32,
    exp: f64,
    freq: f64,
}

/// MongoDB's per-string scoring (`FTSSpec::_scoreStringV2`): terms get `weight * freq * coeff * adjustment`.
fn score_string(raw: &str, lang: Lang, weight: f64, scores: &mut BTreeMap<String, f64>) {
    let mut occ: BTreeMap<String, Occ> = BTreeMap::new();
    let mut num_tokens = 0u32;
    for t in terms(raw, lang, Sens::default()) {
        let o = occ.entry(t).or_insert(Occ { count: 0, exp: 0.0, freq: 0.0 });
        o.exp = if o.exp > 0.0 { o.exp * 2.0 } else { 1.0 };
        o.count += 1;
        o.freq += 1.0 / o.exp;
        num_tokens += 1;
    }
    for (term, o) in occ {
        let coeff = (0.5 * o.count as f64 / num_tokens as f64) + 0.5;
        let mut adjustment = 1.0;
        if raw.len() == term.len() && raw.eq_ignore_ascii_case(&term) {
            adjustment += 0.1;
        }
        *scores.entry(term).or_insert(0.0) += weight * o.freq * coeff * adjustment;
    }
}

/// Term -> score of a document under a text index.
pub fn doc_scores(spec: &TextSpec, d: &Document) -> R<BTreeMap<String, f64>> {
    let lang = doc_lang(spec, d)?;
    let mut scores = BTreeMap::new();
    for (w, s) in indexed_strings(spec, d) {
        score_string(&s, lang, w, &mut scores);
    }
    Ok(scores)
}

// ---------------------------------------------------------------------------------------------
// Queries
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Debug, Default)]
pub struct TextQuery {
    /// Positive terms (insensitive form), including those of phrases.
    pub terms: Vec<String>,
    pub neg_terms: Vec<String>,
    /// Phrases as typed (compared as substrings of the indexed strings).
    pub phrases: Vec<String>,
    pub neg_phrases: Vec<String>,
    pub lang: Option<Lang>,
    pub sens: Sens,
    /// Positive / negated terms in the sensitive form (equal to `terms` when no sensitivity is requested).
    pub s_terms: Vec<String>,
    pub s_neg_terms: Vec<String>,
}

/// Split a `$search` string into words, `"phrases"`, `-negated` words and `-"negated phrases"`.
pub fn parse_query(search: &str, lang: Lang, sens: Sens) -> TextQuery {
    let chars: Vec<char> = search.chars().collect();
    let n = chars.len();
    let mut q = TextQuery { lang: Some(lang), sens, ..Default::default() };
    let mut words = String::new();
    let mut neg_words = String::new();
    let mut i = 0;
    while i < n {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        let neg = c == '-' && i + 1 < n && !chars[i + 1].is_whitespace();
        let start = if neg { i + 1 } else { i };
        if chars[start] == '"' {
            if let Some(rel) = chars[start + 1..].iter().position(|&x| x == '"') {
                let phrase: String = chars[start + 1..start + 1 + rel].iter().collect();
                if !phrase.is_empty() {
                    if neg {
                        q.neg_phrases.push(phrase);
                    } else {
                        words.push(' ');
                        words.push_str(&phrase);
                        q.phrases.push(phrase);
                    }
                }
                i = start + 1 + rel + 1;
                continue;
            }
            // unmatched quote: ignore it
            i = start + 1;
            continue;
        }
        let mut j = start;
        while j < n && !chars[j].is_whitespace() && chars[j] != '"' {
            j += 1;
        }
        let w: String = chars[start..j].iter().collect();
        if neg {
            neg_words.push(' ');
            neg_words.push_str(&w);
        } else {
            words.push(' ');
            words.push_str(&w);
        }
        i = j.max(i + 1);
    }
    let mut uniq = |ts: Vec<String>| -> Vec<String> {
        let mut seen = HashSet::new();
        ts.into_iter().filter(|t| seen.insert(t.clone())).collect()
    };
    q.terms = uniq(terms(&words, lang, Sens::default()));
    q.neg_terms = uniq(terms(&neg_words, lang, Sens::default()));
    if sens.case || sens.diacritic {
        q.s_terms = uniq(terms(&words, lang, sens));
        q.s_neg_terms = uniq(terms(&neg_words, lang, sens));
    } else {
        q.s_terms = q.terms.clone();
        q.s_neg_terms = q.neg_terms.clone();
    }
    q
}

/// Parsed `$text` operand.
pub fn parse_text_operand(v: &Bson, default_lang: Lang) -> R<TextQuery> {
    let d = match v {
        Bson::Document(d) => d,
        _ => return Err(MongoError::new(2, "BadValue", "$text expects an object".into())),
    };
    let mut search: Option<&str> = None;
    let mut lang = default_lang;
    let mut sens = Sens::default();
    for (k, val) in d {
        match k.as_str() {
            "$search" => match val {
                Bson::String(s) => search = Some(s),
                other => return Err(MongoError::new(14, "TypeMismatch", format!("\"$search\" had the wrong type. Expected string, found {}", crate::update::type_name(other)))),
            },
            "$language" => match val {
                Bson::String(s) => {
                    if !s.is_empty() {
                        lang = parse_lang(s).ok_or_else(|| MongoError::new(2, "BadValue", format!("unsupported language: \"{s}\" for text index version 3")))?;
                    }
                }
                other => return Err(MongoError::new(14, "TypeMismatch", format!("\"$language\" had the wrong type. Expected string, found {}", crate::update::type_name(other)))),
            },
            "$caseSensitive" => match val {
                Bson::Boolean(b) => sens.case = *b,
                other => return Err(MongoError::new(14, "TypeMismatch", format!("\"$caseSensitive\" had the wrong type. Expected boolean, found {}", crate::update::type_name(other)))),
            },
            "$diacriticSensitive" => match val {
                Bson::Boolean(b) => sens.diacritic = *b,
                other => return Err(MongoError::new(14, "TypeMismatch", format!("\"$diacriticSensitive\" had the wrong type. Expected boolean, found {}", crate::update::type_name(other)))),
            },
            "$__index" => {}
            other => return Err(MongoError::new(2, "BadValue", format!("extra field found in $text: {other}"))),
        }
    }
    let search = search.ok_or_else(|| MongoError::new(4, "NoSuchKey", "required parameter \"$search\" missing".into()))?;
    Ok(parse_query(search, lang, sens))
}

fn contains_ci(hay: &str, needle: &str, case: bool) -> bool {
    if case {
        return hay.contains(needle);
    }
    lower(hay).contains(&lower(needle))
}

/// Does `d` match the query? `Some(score)` when it does (the score is the sum of the scores of the matching
/// positive terms), `None` otherwise.
pub fn match_doc(spec: &TextSpec, q: &TextQuery, d: &Document) -> R<Option<f64>> {
    let scores = doc_scores(spec, d)?;
    let strings = indexed_strings(spec, d);
    // positive terms: at least one must be present
    if q.terms.is_empty() || !q.terms.iter().any(|t| scores.contains_key(t)) {
        return Ok(None);
    }
    let sensitive_mode = q.sens.case || q.sens.diacritic;
    if !sensitive_mode && q.neg_terms.iter().any(|t| scores.contains_key(t)) {
        return Ok(None);
    }
    if sensitive_mode {
        let lang = doc_lang(spec, d)?;
        let mut sensitive: HashSet<String> = HashSet::new();
        for (_, s) in &strings {
            sensitive.extend(terms(s, lang, q.sens));
        }
        if !q.s_terms.iter().any(|t| sensitive.contains(t)) {
            return Ok(None);
        }
        if q.s_neg_terms.iter().any(|t| sensitive.contains(t)) {
            return Ok(None);
        }
    }
    let case = q.sens.case;
    for p in &q.phrases {
        if !strings.iter().any(|(_, s)| contains_ci(s, p, case)) {
            return Ok(None);
        }
    }
    for p in &q.neg_phrases {
        if strings.iter().any(|(_, s)| contains_ci(s, p, case)) {
            return Ok(None);
        }
    }
    let score: f64 = q.terms.iter().filter_map(|t| scores.get(t)).sum();
    Ok(Some(score))
}

// ---------------------------------------------------------------------------------------------
// Integration with filters
// ---------------------------------------------------------------------------------------------

/// Cache of compiled `(spec, query)` pairs keyed by the serialized `$text` operand (the operand is the same for
/// every document of a command).
fn compiled(operand: &Document) -> R<(TextSpec, TextQuery)> {
    thread_local! {
        static CACHE: std::cell::RefCell<Option<(Vec<u8>, TextSpec, TextQuery)>> = const { std::cell::RefCell::new(None) };
    }
    let key = operand.to_vec().unwrap_or_default();
    let hit = CACHE.with(|c| c.borrow().as_ref().filter(|(k, _, _)| *k == key).map(|(_, s, q)| (s.clone(), q.clone())));
    if let Some(h) = hit {
        return Ok(h);
    }
    let Some(Bson::Document(idx)) = operand.get("$__index") else {
        return Err(MongoError::new(17313, "Location17313", "$match with $text is only allowed as the first pipeline stage".into()));
    };
    let spec = TextSpec::from_spec(idx).ok_or_else(|| MongoError::new(27, "IndexNotFound", "text index required for $text query".into()))?;
    let q = parse_text_operand(&Bson::Document(operand.clone()), spec.default_lang)?;
    CACHE.with(|c| *c.borrow_mut() = Some((key, spec.clone(), q.clone())));
    Ok((spec, q))
}

/// `$text` as a filter condition: the `$__index` field was added by [`resolve_filter`].
pub fn matches_operand(operand: &Bson, d: &Document) -> R<bool> {
    let Bson::Document(op) = operand else { return Err(MongoError::new(2, "BadValue", "$text expects an object".into())) };
    let (spec, q) = compiled(op)?;
    Ok(match_doc(&spec, &q, d)?.is_some())
}

/// Score of `d` for the `$text` condition of `filter` (None when the filter has none or `d` does not match).
pub fn score_of(filter: &Document, d: &Document) -> R<Option<f64>> {
    match find_operand_any(filter) {
        Some(Bson::Document(op)) => {
            let (spec, q) = compiled(op)?;
            match_doc(&spec, &q, d)
        }
        _ => Ok(None),
    }
}

/// The `$text` operand of a filter (top level or inside `$and`): the one the planner answers with the text index.
pub fn find_operand(filter: &Document) -> Option<&Bson> {
    find_operand_in(filter, false)
}

/// Like [`find_operand`], also looking inside `$or` clauses (for scoring documents).
pub fn find_operand_any(filter: &Document) -> Option<&Bson> {
    find_operand_in(filter, true)
}

fn find_operand_in(filter: &Document, through_or: bool) -> Option<&Bson> {
    for (k, v) in filter {
        if k == "$text" {
            return Some(v);
        }
        if k == "$and" || (through_or && k == "$or") {
            if let Bson::Array(a) = v {
                for s in a {
                    if let Bson::Document(sd) = s {
                        if let Some(x) = find_operand_in(sd, through_or) {
                            return Some(x);
                        }
                    }
                }
            }
        }
    }
    None
}

/// Remove the hidden score / distance fields from a result document.
/// Marks a document of a time-series collection whose `_id` the server made up (it is not shown).
pub const TS_AUTO: &str = "\u{1}tsauto";

pub fn strip_hidden(d: &mut Document) {
    if d.remove(TS_AUTO).is_some() {
        d.remove("_id");
    }
    strip_scores(d);
}

/// Like `strip_hidden`, for documents that are written back (the generated `_id` of a time-series document stays).
pub fn strip_scores(d: &mut Document) {
    for k in [SCORE_KEY, crate::search::SCORE_KEY, crate::search_more::HIGHLIGHT_KEY, crate::search_more::META_KEY, crate::geo::DIST_KEY, crate::geo::POINT_KEY] {
        d.remove(k);
    }
}

pub fn has_text(filter: &Document) -> bool {
    fn walk(f: &Document) -> bool {
        f.iter().any(|(k, v)| match (k.as_str(), v) {
            ("$text", _) => true,
            ("$and" | "$or" | "$nor", Bson::Array(a)) => a.iter().any(|s| matches!(s, Bson::Document(d) if walk(d))),
            _ => false,
        })
    }
    walk(filter)
}

/// Copy of `filter` whose `$text` operand carries the text index spec (`$__index`); the operand is validated.
/// Errors: no text index (27), `$text` inside `$or`/`$nor`/`$not` (2).
pub fn resolve_filter(filter: &Document, text_index: Option<&Document>) -> R<Document> {
    fn walk(f: &Document, idx: Option<&Document>, top: bool, out: &mut Document, count: &mut usize) -> R<()> {
        for (k, v) in f {
            match (k.as_str(), v) {
                ("$text", Bson::Document(op)) => {
                    *count += 1;
                    if !top {
                        return Err(MongoError::new(2, "BadValue", "$text is only allowed at the top level or inside $and".into()));
                    }
                    let spec = idx.ok_or_else(|| MongoError::new(27, "IndexNotFound", "text index required for $text query".into()))?;
                    let mut op2 = op.clone();
                    op2.insert("$__index", Bson::Document(spec.clone()));
                    out.insert("$text", Bson::Document(op2));
                }
                ("$text", _) => return Err(MongoError::new(2, "BadValue", "$text expects an object".into())),
                ("$and", Bson::Array(a)) => {
                    let mut arr = Vec::new();
                    for s in a {
                        match s {
                            Bson::Document(sd) => {
                                let mut o = Document::new();
                                walk(sd, idx, top, &mut o, count)?;
                                arr.push(Bson::Document(o));
                            }
                            other => arr.push(other.clone()),
                        }
                    }
                    out.insert("$and", Bson::Array(arr));
                }
                ("$or" | "$nor", Bson::Array(a)) => {
                    let mut arr = Vec::new();
                    for s in a {
                        match s {
                            Bson::Document(sd) => {
                                let mut o = Document::new();
                                walk(sd, idx, k == "$or", &mut o, count)?;
                                arr.push(Bson::Document(o));
                            }
                            other => arr.push(other.clone()),
                        }
                    }
                    out.insert(k.clone(), Bson::Array(arr));
                }
                _ => {
                    out.insert(k.clone(), v.clone());
                }
            }
        }
        Ok(())
    }
    let mut out = Document::new();
    let mut count = 0;
    walk(filter, text_index, true, &mut out, &mut count)?;
    if count > 1 {
        return Err(MongoError::new(2, "BadValue", "Too many text expressions".into()));
    }
    Ok(out)
}

/// Terms to look up in the index for the operand of the `$text` condition (insensitive, positive).
pub fn lookup_terms(operand: &Bson, spec: &TextSpec) -> R<Vec<String>> {
    Ok(parse_text_operand(operand, spec.default_lang)?.terms)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    fn spec() -> TextSpec {
        TextSpec::from_spec(&doc! { "textKey": { "t": "text" }, "weights": { "t": 1 }, "default_language": "english", "language_override": "language" }).unwrap()
    }

    #[test]
    fn scores_match_mongodb() {
        let s = spec();
        let sc = |t: &str| doc_scores(&s, &doc! { "t": t }).unwrap();
        assert_eq!(sc("hello")["hello"], 1.1);
        assert_eq!(sc("hello hello")["hello"], 1.5);
        assert_eq!(sc("hello world")["hello"], 0.75);
        assert_eq!(sc("hello hello hello hello")["hello"], 1.875);
        assert!((sc("the quick brown fox jumps over the lazy dog")["quick"] - 0.5833333333).abs() < 1e-9);
    }

    #[test]
    fn stems_before_stripping_diacritics() {
        let t = terms("résumé RESUME naïve NAIVE", Lang::English, Sens::default());
        assert_eq!(t[0], "resume");
        assert_eq!(t[1], "resum");
        assert_eq!(t[2], t[3]);
    }

    #[test]
    fn query_parsing() {
        let q = parse_query("quick \"brown fox\" -dogs -\"lazy cat\"", Lang::English, Sens::default());
        assert_eq!(q.phrases, vec!["brown fox"]);
        assert_eq!(q.neg_phrases, vec!["lazy cat"]);
        assert!(q.terms.contains(&"quick".to_string()) && q.terms.contains(&"fox".to_string()));
        assert_eq!(q.neg_terms, vec!["dog"]);
    }
}
