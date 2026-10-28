//! Collection validators (`validator`, `validationLevel`, `validationAction` of `create` / `collMod`): the check on every write and
//! the explanation (`errInfo.details`) of a document that failed it, built the way mongod builds it.

use crate::error::MongoError;
use crate::query::{compare, equal, is_numeric, matches, path_values};
use bson::{doc, Bson, Document};
use std::cmp::Ordering;

type R<T> = Result<T, MongoError>;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Level {
    Off,
    Strict,
    Moderate,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Action {
    Error,
    Warn,
}

#[derive(Clone, Debug)]
pub struct Validator {
    pub expr: Document,
    pub level: Level,
    pub action: Action,
}

impl Validator {
    /// The validator of a collection from its stored options; `None` when it has none (or an empty one).
    pub fn from_options(raw: &Document) -> Option<Validator> {
        let expr = raw.get_document("validator").ok()?.clone();
        if expr.is_empty() {
            return None;
        }
        let level = match raw.get_str("validationLevel") {
            Ok("off") => Level::Off,
            Ok("moderate") => Level::Moderate,
            _ => Level::Strict,
        };
        let action = match raw.get_str("validationAction") {
            Ok("warn") => Action::Warn,
            _ => Action::Error,
        };
        Some(Validator { expr, level, action })
    }
}

fn bad(msg: impl Into<String>) -> MongoError {
    MongoError::new(2, "BadValue", msg.into())
}

/// Check a validator expression when it is set: it must parse as a query without the operators a validator cannot have.
pub fn check_expression(v: &Document) -> R<()> {
    fn walk(d: &Document) -> R<()> {
        for (k, v) in d {
            match k.as_str() {
                "$where" => return Err(bad("Parsing of collection validator failed :: caused by :: $where is not allowed in this context")),
                "$text" => return Err(bad("$text is not allowed in this context")),
                "$and" | "$or" | "$nor" => {
                    if let Bson::Array(a) = v {
                        for s in a {
                            if let Bson::Document(sd) = s {
                                walk(sd)?;
                            }
                        }
                    }
                }
                _ => {
                    if let Bson::Document(od) = v {
                        for op in od.keys() {
                            if matches!(op.as_str(), "$near" | "$nearSphere") {
                                return Err(bad(format!("{op} is not allowed in this context")));
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
    crate::query::probe_filter(v)?;
    walk(v)
}

/// Does the document satisfy the validator?
pub fn satisfies(doc: &Document, v: &Validator) -> R<bool> {
    matches(doc, &v.expr)
}

/// The error of a document that failed the validator.
pub fn failure(doc: &Document, v: &Validator) -> MongoError {
    let details = explain_failure(doc, &v.expr);
    let mut info = Document::new();
    info.insert("failingDocumentId", doc.get("_id").cloned().unwrap_or(Bson::Null));
    info.insert("details", details);
    let mut e = MongoError::new(121, "DocumentValidationFailure", "Document failed validation".into());
    e.extra = Some(doc! { "errInfo": info });
    e
}

// ---- match expressions -----------------------------------------------------------------------------------------------

enum Node {
    And(Vec<Node>),
    Or(Vec<Node>),
    Nor(Vec<Node>),
    Not(Box<Node>),
    /// path, operator (with the `$`), argument, the specification as the user wrote it (`{path: {op: arg}}`)
    Leaf(String, String, Bson, Document),
    Schema(Document),
    Expr(Bson),
}

fn parse_clause(k: &str, v: &Bson, out: &mut Vec<Node>) {
    match k {
        "$and" | "$or" | "$nor" => {
            let kids: Vec<Node> = match v {
                Bson::Array(a) => a.iter().filter_map(|s| s.as_document()).map(parse_doc).collect(),
                _ => vec![],
            };
            out.push(match k {
                "$and" => Node::And(kids),
                "$or" => Node::Or(kids),
                _ => Node::Nor(kids),
            });
        }
        "$jsonSchema" => out.push(Node::Schema(v.as_document().cloned().unwrap_or_default())),
        "$expr" => out.push(Node::Expr(v.clone())),
        "$comment" => {}
        path => match v {
            Bson::Document(d) if !d.is_empty() && d.keys().all(|o| o.starts_with('$')) => {
                let has_regex = d.contains_key("$regex");
                for (op, arg) in d {
                    if op == "$options" {
                        continue;
                    }
                    if op == "$not" {
                        let mut inner = Vec::new();
                        match arg {
                            Bson::Document(nd) if nd.contains_key("$regex") => {
                                let pat = nd.get_str("$regex").unwrap_or("").to_string();
                                let opts = nd.get_str("$options").unwrap_or("").to_string();
                                if let (Ok(pattern), Ok(options)) = (bson::raw::CString::try_from(pat), bson::raw::CString::try_from(opts)) {
                                    let re = Bson::RegularExpression(bson::Regex { pattern, options });
                                    inner.push(Node::Leaf(path.to_string(), "$regex".into(), re.clone(), doc! { path: re }));
                                }
                            }
                            Bson::Document(nd) => parse_clause(path, &Bson::Document(nd.clone()), &mut inner),
                            re @ Bson::RegularExpression(_) => inner.push(Node::Leaf(path.to_string(), "$regex".into(), re.clone(), doc! { path: re.clone() })),
                            other => parse_clause(path, &doc! { "$eq": other.clone() }.into(), &mut inner),
                        }
                        let child = if inner.len() == 1 { inner.pop().unwrap() } else { Node::And(inner) };
                        out.push(Node::Not(Box::new(child)));
                        continue;
                    }
                    let spec_inner = if op == "$regex" && has_regex {
                        let mut s = Document::new();
                        s.insert("$regex", arg.clone());
                        if let Some(o) = d.get("$options") {
                            s.insert("$options", o.clone());
                        }
                        s
                    } else {
                        doc! { op.clone(): arg.clone() }
                    };
                    out.push(Node::Leaf(path.to_string(), op.clone(), arg.clone(), doc! { path: spec_inner }));
                }
            }
            Bson::RegularExpression(_) => out.push(Node::Leaf(path.to_string(), "$regex".into(), v.clone(), doc! { path: v.clone() })),
            other => out.push(Node::Leaf(path.to_string(), "$eq".into(), other.clone(), doc! { path: other.clone() })),
        },
    }
}

fn parse_doc(d: &Document) -> Node {
    let mut v = Vec::new();
    for (k, x) in d {
        parse_clause(k, x, &mut v);
    }
    if v.len() == 1 { v.pop().unwrap() } else { Node::And(v) }
}

fn node_matches(n: &Node, d: &Document) -> bool {
    match n {
        Node::And(c) => c.iter().all(|x| node_matches(x, d)),
        Node::Or(c) => c.iter().any(|x| node_matches(x, d)),
        Node::Nor(c) => !c.iter().any(|x| node_matches(x, d)),
        Node::Not(c) => !node_matches(c, d),
        Node::Leaf(_, _, _, spec) => matches(d, spec).unwrap_or(false),
        Node::Schema(s) => crate::json_schema::validate(&Bson::Document(d.clone()), s).unwrap_or(false),
        Node::Expr(e) => crate::expr::eval(e, &crate::expr::Scope::new(d)).ok().flatten().map(|v| crate::expr::truthy(Some(&v))).unwrap_or(false),
    }
}

fn type_name(v: &Bson) -> &'static str {
    crate::update::type_name(v)
}

/// The values a path reads from a document, shaped the way mongod reports them.
fn considered(d: &Document, path: &str) -> Option<(&'static str, Bson)> {
    let vals = path_values(d, path);
    if vals.is_empty() {
        return None;
    }
    if vals.len() == 1 && !matches!(vals[0], Bson::Array(_)) {
        return Some(("consideredValue", vals[0].clone()));
    }
    let mut out = Vec::new();
    for v in vals {
        match v {
            Bson::Array(a) => out.extend(a.iter().cloned()),
            other => out.push(other.clone()),
        }
    }
    Some(("consideredValues", Bson::Array(out)))
}

fn leaf_details(path: &str, op: &str, arg: &Bson, spec: &Document, d: &Document, succeeded: bool) -> Document {
    let mut out = doc! { "operatorName": op, "specifiedAs": spec.clone() };
    let vals = path_values(d, path);
    let reason: &str = match op {
        "$eq" | "$gt" | "$gte" | "$lt" | "$lte" => {
            if vals.is_empty() && !succeeded {
                "field was missing"
            } else if succeeded {
                "comparison succeeded"
            } else {
                "comparison failed"
            }
        }
        "$ne" => {
            if succeeded { "comparison failed" } else { "comparison succeeded" }
        }
        "$in" => {
            if vals.is_empty() && !succeeded { "field was missing" } else if succeeded { "matching value found in array" } else { "no matching value found in array" }
        }
        "$nin" => {
            if succeeded { "no matching value found in array" } else { "matching value found in array" }
        }
        "$exists" => {
            let want = !matches!(arg, Bson::Boolean(false)) && !matches!(arg, Bson::Int32(0)) && !matches!(arg, Bson::Int64(0));
            if want != succeeded { "path does not exist" } else { "path does exist" }
        }
        "$type" => if succeeded { "type matched" } else { "type did not match" },
        "$regex" => if succeeded { "regular expression did match" } else { "regular expression did not match" },
        "$size" => if succeeded { "array length was equal to given size" } else { "array length was not equal to given size" },
        "$mod" => if succeeded { "$mod evaluated to expected remainder" } else { "$mod did not evaluate to expected remainder" },
        "$all" => if succeeded { "array contained all specified values" } else { "array did not contain all specified values" },
        "$elemMatch" => if succeeded { "array satisfied the child predicate" } else { "array did not satisfy the child predicate" },
        "$bitsAllSet" | "$bitsAnySet" | "$bitsAllClear" | "$bitsAnyClear" => if succeeded { "bitwise operator matched" } else { "bitwise operator failed to match" },
        _ => if succeeded { "predicate matched" } else { "predicate did not match" },
    };
    if !succeeded && matches!(op, "$size" | "$all" | "$elemMatch") && !vals.is_empty() && !matches!(vals[0], Bson::Array(_)) {
        out.insert("reason", "type did not match");
        out.insert("consideredType", type_name(vals[0]));
        out.insert("expectedType", "array");
        out.insert("consideredValue", vals[0].clone());
        return out;
    }
    out.insert("reason", reason);
    if op == "$exists" {
        return out;
    }
    let subject_is_array = matches!(op, "$size" | "$all" | "$elemMatch");
    let c = if subject_is_array { vals.first().map(|v| ("consideredValue", (*v).clone())) } else { considered(d, path) };
    if let Some((k, v)) = c {
        // a type test also names the type it found
        let is_type = op == "$type";
        let tv = v.clone();
        out.insert(k, v);
        if is_type && k == "consideredValue" {
            out.insert("consideredType", type_name(&tv));
        }
    }
    out
}

/// Why the node did not match (`succeeded == false`) or why it matched (`succeeded == true`); `None` when it is not so.
fn explain(n: &Node, d: &Document, succeeded: bool) -> Option<Document> {
    let m = node_matches(n, d);
    if m != succeeded {
        return None;
    }
    Some(match n {
        Node::And(c) => {
            let list: Vec<Bson> = c.iter().enumerate().filter_map(|(i, x)| explain(x, d, succeeded).map(|e| Bson::Document(doc! { "index": i as i32, "details": e }))).collect();
            doc! { "operatorName": "$and", if succeeded { "clausesSatisfied" } else { "clausesNotSatisfied" }: list }
        }
        Node::Or(c) => {
            let list: Vec<Bson> = c.iter().enumerate().filter_map(|(i, x)| explain(x, d, succeeded).map(|e| Bson::Document(doc! { "index": i as i32, "details": e }))).collect();
            doc! { "operatorName": "$or", if succeeded { "clausesSatisfied" } else { "clausesNotSatisfied" }: list }
        }
        Node::Nor(c) => {
            // a $nor fails because a clause matched; it matches because none did
            let list: Vec<Bson> = c.iter().enumerate().filter_map(|(i, x)| explain(x, d, !succeeded).map(|e| Bson::Document(doc! { "index": i as i32, "details": e }))).collect();
            doc! { "operatorName": "$nor", if succeeded { "clausesNotSatisfied" } else { "clausesSatisfied" }: list }
        }
        Node::Not(c) => doc! { "operatorName": "$not", "details": explain(c, d, !succeeded).unwrap_or_default() },
        Node::Leaf(path, op, arg, spec) => leaf_details(path, op, arg, spec, d, succeeded),
        Node::Schema(s) => {
            if succeeded {
                doc! { "operatorName": "$jsonSchema", "reason": "schema matched" }
            } else {
                schema_details(d, s)
            }
        }
        Node::Expr(e) => doc! { "operatorName": "$expr", "specifiedAs": { "$expr": e.clone() }, "reason": if succeeded { "expression matched" } else { "expression did not match" }, "expressionResult": succeeded },
    })
}

pub fn explain_failure(d: &Document, validator: &Document) -> Document {
    let root = parse_doc(validator);
    explain(&root, d, false).unwrap_or_default()
}

// ---- $jsonSchema ---------------------------------------------------------------------------------------------------

fn schema_details(d: &Document, schema: &Document) -> Document {
    // a schema that wants some other type than an object can never match a document
    let root_type_ok = match (schema.get("bsonType"), schema.get("type")) {
        (Some(t), _) | (None, Some(t)) => type_allows_object(t),
        _ => true,
    };
    if !root_type_ok {
        return doc! { "operatorName": "$jsonSchema", "specifiedAs": { "$jsonSchema": schema.clone() }, "reason": "expression always evaluates to false" };
    }
    let v = Bson::Document(d.clone());
    let rules: Vec<Bson> = schema_failures(&v, schema).into_iter().map(Bson::Document).collect();
    doc! { "operatorName": "$jsonSchema", "schemaRulesNotSatisfied": rules }
}

fn type_allows_object(t: &Bson) -> bool {
    match t {
        Bson::String(s) => s == "object",
        Bson::Array(a) => a.iter().any(|x| x.as_str() == Some("object")),
        _ => true,
    }
}

fn spec1(schema: &Document, key: &str) -> Document {
    let mut d = Document::new();
    if let Some(v) = schema.get(key) {
        d.insert(key, v.clone());
    }
    d
}

fn passes(v: &Bson, schema: &Document) -> bool {
    crate::json_schema::validate(v, schema).unwrap_or(true)
}

fn int_of(v: &Bson) -> i64 {
    crate::query::int_arg(v).unwrap_or(0)
}

/// The keywords of `schema` that `v` does not satisfy, in the order mongod reports them.
pub fn schema_failures(v: &Bson, schema: &Document) -> Vec<Document> {
    let mut out: Vec<Document> = Vec::new();
    let type_key = if schema.contains_key("bsonType") { Some("bsonType") } else if schema.contains_key("type") { Some("type") } else { None };
    let type_fails = type_key.map(|k| !passes(v, &doc! { k: schema.get(k).cloned().unwrap_or(Bson::Null) })).unwrap_or(false);
    if !type_fails {
        match v {
            Bson::String(s) => {
                if let Some(p) = schema.get("pattern") {
                    if !passes(v, &spec1(schema, "pattern")) {
                        out.push(doc! { "operatorName": "pattern", "specifiedAs": { "pattern": p.clone() }, "reason": "regular expression did not match", "consideredValue": v.clone() });
                    }
                }
                for k in ["maxLength", "minLength"] {
                    if schema.contains_key(k) && !passes(v, &spec1(schema, k)) {
                        out.push(doc! { "operatorName": k, "specifiedAs": spec1(schema, k), "reason": "specified string length was not satisfied", "consideredValue": s.clone() });
                    }
                }
            }
            n if is_numeric(n) => {
                if schema.contains_key("multipleOf") && !passes(v, &spec1(schema, "multipleOf")) {
                    out.push(doc! { "operatorName": "multipleOf", "specifiedAs": spec1(schema, "multipleOf"), "reason": "considered value is not a multiple of the specified value", "consideredValue": v.clone() });
                }
                for (k, ex) in [("maximum", "exclusiveMaximum"), ("minimum", "exclusiveMinimum")] {
                    if schema.contains_key(k) {
                        let mut sub = spec1(schema, k);
                        if let Some(e) = schema.get(ex) {
                            sub.insert(ex, e.clone());
                        }
                        if !passes(v, &sub) {
                            out.push(doc! { "operatorName": k, "specifiedAs": sub, "reason": "comparison failed", "consideredValue": v.clone() });
                        }
                    }
                }
            }
            Bson::Array(a) => {
                for k in ["minItems", "maxItems"] {
                    if schema.contains_key(k) && !passes(v, &spec1(schema, k)) {
                        out.push(doc! { "operatorName": k, "specifiedAs": spec1(schema, k), "reason": "array did not match specified length", "consideredValue": v.clone(), "numberOfItems": a.len() as i32 });
                    }
                }
                if matches!(schema.get("uniqueItems"), Some(Bson::Boolean(true))) && !passes(v, &spec1(schema, "uniqueItems")) {
                    let mut dup = Bson::Null;
                    'o: for i in 0..a.len() {
                        for j in i + 1..a.len() {
                            if compare(&a[i], &a[j]) == Ordering::Equal {
                                dup = a[i].clone();
                                break 'o;
                            }
                        }
                    }
                    out.push(doc! { "operatorName": "uniqueItems", "specifiedAs": spec1(schema, "uniqueItems"), "reason": "found a duplicate item", "consideredValue": v.clone(), "duplicatedValue": dup });
                }
                match schema.get("items") {
                    Some(Bson::Document(sd)) => {
                        if let Some((i, _)) = a.iter().enumerate().find(|(_, e)| !passes(e, sd)) {
                            let inner: Vec<Bson> = schema_failures(&a[i], sd).into_iter().map(Bson::Document).collect();
                            out.push(doc! { "operatorName": "items", "reason": "At least one item did not match the sub-schema", "itemIndex": i as i32, "details": inner });
                        }
                    }
                    Some(Bson::Array(list)) => {
                        let mut fails = Vec::new();
                        for (i, (e, sub)) in a.iter().zip(list).enumerate() {
                            if let Bson::Document(sd) = sub {
                                if !passes(e, sd) {
                                    let inner: Vec<Bson> = schema_failures(e, sd).into_iter().map(Bson::Document).collect();
                                    fails.push(Bson::Document(doc! { "itemIndex": i as i64, "details": inner }));
                                }
                            }
                        }
                        if !fails.is_empty() {
                            out.push(doc! { "operatorName": "items", "details": fails });
                        }
                        if a.len() > list.len() {
                            if let Some(Bson::Boolean(false)) = schema.get("additionalItems") {
                                out.push(doc! { "operatorName": "additionalItems", "specifiedAs": { "additionalItems": false }, "reason": "found additional items", "additionalItems": a[list.len()..].to_vec() });
                            }
                        }
                    }
                    _ => {}
                }
            }
            Bson::Document(d) => {
                let props = schema.get_document("properties").ok();
                let pats = schema.get_document("patternProperties").ok();
                if let Some(p) = props {
                    let mut list = Vec::new();
                    for (name, sub) in p {
                        if let (Some(val), Bson::Document(sd)) = (d.get(name), sub) {
                            if !passes(val, sd) {
                                let inner: Vec<Bson> = schema_failures(val, sd).into_iter().map(Bson::Document).collect();
                                list.push(Bson::Document(doc! { "propertyName": name.clone(), "details": inner }));
                            }
                        }
                    }
                    if !list.is_empty() {
                        out.push(doc! { "operatorName": "properties", "propertiesNotSatisfied": list });
                    }
                }
                if let Some(ap) = schema.get("additionalProperties") {
                    let extra: Vec<(&String, &Bson)> = d.iter().filter(|(n, _)| !props.map(|p| p.contains_key(n.as_str())).unwrap_or(false) && !pats.map(|p| p.keys().any(|pat| crate::regex::matches_str(pat, "", n).unwrap_or(false))).unwrap_or(false)).collect();
                    match ap {
                        Bson::Boolean(false) if !extra.is_empty() => {
                            out.push(doc! { "operatorName": "additionalProperties", "specifiedAs": { "additionalProperties": false }, "additionalProperties": extra.iter().map(|(n, _)| Bson::String((*n).clone())).collect::<Vec<_>>() });
                        }
                        Bson::Document(sd) => {
                            if let Some((n, val)) = extra.iter().find(|(_, val)| !passes(val, sd)) {
                                let inner: Vec<Bson> = schema_failures(val, sd).into_iter().map(Bson::Document).collect();
                                out.push(doc! { "operatorName": "additionalProperties", "reason": "at least one additional property did not match the subschema", "failingProperty": (*n).clone(), "details": inner });
                            }
                        }
                        _ => {}
                    }
                }
                if let Some(pp) = pats {
                    let mut list = Vec::new();
                    for (pat, sub) in pp {
                        if let Bson::Document(sd) = sub {
                            for (name, val) in d {
                                if crate::regex::matches_str(pat, "", name).unwrap_or(false) && !passes(val, sd) {
                                    let inner: Vec<Bson> = schema_failures(val, sd).into_iter().map(Bson::Document).collect();
                                    list.push(Bson::Document(doc! { "propertyName": name.clone(), "regexMatched": pat.clone(), "details": inner }));
                                }
                            }
                        }
                    }
                    if !list.is_empty() {
                        out.push(doc! { "operatorName": "patternProperties", "details": list });
                    }
                }
                if let Some(Bson::Array(req)) = schema.get("required") {
                    let missing: Vec<Bson> = req.iter().filter(|f| f.as_str().map(|f| !d.contains_key(f)).unwrap_or(false)).cloned().collect();
                    if !missing.is_empty() {
                        out.push(doc! { "operatorName": "required", "specifiedAs": { "required": req.clone() }, "missingProperties": missing });
                    }
                }
                for k in ["minProperties", "maxProperties"] {
                    if schema.contains_key(k) && !passes(v, &spec1(schema, k)) {
                        out.push(doc! { "operatorName": k, "specifiedAs": spec1(schema, k), "reason": "specified number of properties was not satisfied", "numberOfProperties": d.len() as i32 });
                    }
                }
                if let Some(Bson::Document(deps)) = schema.get("dependencies") {
                    let mut fails = Vec::new();
                    for (field, dep) in deps {
                        if d.contains_key(field) {
                            if let Bson::Array(a) = dep {
                                let missing: Vec<Bson> = a.iter().filter(|x| x.as_str().map(|f| !d.contains_key(f)).unwrap_or(false)).cloned().collect();
                                if !missing.is_empty() {
                                    fails.push(Bson::Document(doc! { "conditionalProperty": field.clone(), "missingProperties": missing }));
                                }
                            }
                        }
                    }
                    if !fails.is_empty() {
                        out.push(doc! { "operatorName": "dependencies", "failingDependencies": fails });
                    }
                }
            }
            _ => {}
        }
        let _ = (int_of, equal);
    }
    for k in ["allOf", "anyOf", "oneOf"] {
        if let Some(Bson::Array(a)) = schema.get(k) {
            let results: Vec<bool> = a.iter().map(|s| s.as_document().map(|sd| passes(v, sd)).unwrap_or(true)).collect();
            let failed = |i: usize| -> Bson {
                let inner: Vec<Bson> = a[i].as_document().map(|sd| schema_failures(v, sd)).unwrap_or_default().into_iter().map(Bson::Document).collect();
                Bson::Document(doc! { "index": i as i32, "details": inner })
            };
            match k {
                "allOf" if results.iter().any(|r| !r) => {
                    let list: Vec<Bson> = (0..a.len()).filter(|i| !results[*i]).map(failed).collect();
                    out.push(doc! { "operatorName": "allOf", "schemasNotSatisfied": list });
                }
                "anyOf" if results.iter().all(|r| !r) => {
                    let list: Vec<Bson> = (0..a.len()).map(failed).collect();
                    out.push(doc! { "operatorName": "anyOf", "schemasNotSatisfied": list });
                }
                "oneOf" => {
                    let hits: Vec<usize> = (0..a.len()).filter(|i| results[*i]).collect();
                    if hits.is_empty() {
                        let list: Vec<Bson> = (0..a.len()).map(failed).collect();
                        out.push(doc! { "operatorName": "oneOf", "schemasNotSatisfied": list });
                    } else if hits.len() > 1 {
                        out.push(doc! { "operatorName": "oneOf", "reason": "more than one subschema matched", "matchingSchemaIndexes": hits.iter().map(|i| Bson::Int32(*i as i32)).collect::<Vec<_>>() });
                    }
                }
                _ => {}
            }
        }
    }
    if let Some(Bson::Document(sd)) = schema.get("not") {
        if passes(v, sd) {
            out.push(doc! { "operatorName": "not", "reason": "child expression matched" });
        }
    }
    if let Some(Bson::Array(e)) = schema.get("enum") {
        if !e.iter().any(|x| equal(x, v)) {
            out.push(doc! { "operatorName": "enum", "specifiedAs": { "enum": e.clone() }, "reason": "value was not found in enum", "consideredValue": v.clone() });
        }
    }
    if let Some(k) = type_key {
        if type_fails {
            out.push(doc! { "operatorName": k, "specifiedAs": spec1(schema, k), "reason": "type did not match", "consideredValue": v.clone(), "consideredType": type_name(v) });
        }
    }
    out
}
