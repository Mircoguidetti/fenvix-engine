//! Aggregation pipeline: $match, $group ($sum, $avg, $min, $max, $count, $first, $last, $push),
//! $sort, $limit, $skip, $project, $count. Stages run over documents already in memory.

use crate::error::MongoError;
use crate::layout::encode_sortable;
use crate::query::{self, compare, is_number, matches, parse_projection, parse_sort, path_values, project, sort_docs};
use bson::{Bson, Document};
use std::collections::HashMap;

type R<T> = Result<T, MongoError>;

fn add_numbers(a: &Bson, b: &Bson) -> Bson {
    match (a, b) {
        (Bson::Int32(x), Bson::Int32(y)) => match x.checked_add(*y) {
            Some(s) => Bson::Int32(s),
            None => Bson::Int64(*x as i64 + *y as i64),
        },
        (Bson::Int32(_) | Bson::Int64(_), Bson::Int32(_) | Bson::Int64(_)) => {
            let (x, y) = (int(a), int(b));
            x.checked_add(y).map(Bson::Int64).unwrap_or(Bson::Double(x as f64 + y as f64))
        }
        _ => Bson::Double(query::as_f64(a) + query::as_f64(b)),
    }
}

fn int(v: &Bson) -> i64 {
    match v {
        Bson::Int32(x) => *x as i64,
        Bson::Int64(x) => *x,
        _ => 0,
    }
}

/// Evaluate an aggregation expression against a document; `None` means "missing".
fn eval(expr: &Bson, doc: &Document) -> R<Option<Bson>> {
    crate::expr::eval(expr, &crate::expr::Scope::new(doc))
}

enum Acc {
    Sum(Bson),
    Avg(f64, i64),
    Min(Option<Bson>),
    Max(Option<Bson>),
    Count(i64),
    First(Option<Bson>, bool),
    Last(Bson),
    Push(Vec<Bson>),
    AddToSet(Vec<Bson>),
    Merge(Document),
    Std { n: i64, mean: f64, m2: f64 },
    /// `$firstN`, `$lastN`, `$maxN`, `$minN`
    NList(Vec<Bson>),
    /// `$top`, `$bottom`, `$topN`, `$bottomN`: (original document with the output under a hidden key)
    Top(Vec<Document>),
    Js(Bson),
}

const OUT_KEY: &str = "\u{1}out";

struct AccSpec {
    name: String,
    op: String,
    expr: Bson,
}

fn parse_accumulators(spec: &Document) -> R<Vec<AccSpec>> {
    let mut out = Vec::new();
    for (name, v) in spec {
        if name == "_id" {
            continue;
        }
        if name.contains('.') {
            return Err(MongoError::new(40235, "Location40235", format!("The field name '{name}' cannot contain '.'")));
        }
        let d = v.as_document().ok_or_else(|| {
            MongoError::new(40234, "Location40234", format!("The field '{name}' must be an accumulator object"))
        })?;
        if d.len() != 1 {
            return Err(MongoError::new(40238, "Location40238", format!("The field '{name}' must specify one accumulator")));
        }
        let (op, expr) = d.iter().next().unwrap();
        if !matches!(op.as_str(), "$sum" | "$avg" | "$min" | "$max" | "$count" | "$first" | "$last" | "$push" | "$addToSet" | "$mergeObjects" | "$stdDevPop" | "$stdDevSamp" | "$firstN" | "$lastN" | "$maxN" | "$minN" | "$top" | "$bottom" | "$topN" | "$bottomN" | "$accumulator" | "$median" | "$percentile") {
            return Err(MongoError::new(15952, "Location15952", format!("unknown group operator '{op}'")));
        }
        out.push(AccSpec { name: name.clone(), op: op.clone(), expr: expr.clone() });
    }
    Ok(out)
}

fn init(op: &str) -> Acc {
    match op {
        "$sum" => Acc::Sum(Bson::Int32(0)),
        "$avg" => Acc::Avg(0.0, 0),
        "$min" => Acc::Min(None),
        "$max" => Acc::Max(None),
        "$count" => Acc::Count(0),
        "$first" => Acc::First(None, false),
        "$last" => Acc::Last(Bson::Null),
        "$addToSet" => Acc::AddToSet(Vec::new()),
        "$mergeObjects" => Acc::Merge(Document::new()),
        "$stdDevPop" | "$stdDevSamp" => Acc::Std { n: 0, mean: 0.0, m2: 0.0 },
        "$firstN" | "$lastN" | "$maxN" | "$minN" => Acc::NList(Vec::new()),
        "$top" | "$bottom" | "$topN" | "$bottomN" => Acc::Top(Vec::new()),
        "$accumulator" => Acc::Js(Bson::Null),
        _ => Acc::Push(Vec::new()),
    }
}

fn arg_doc<'a>(spec: &'a AccSpec) -> R<&'a Document> {
    spec.expr.as_document().ok_or_else(|| MongoError::new(5787900, "Location5787900", format!("specification of {} must be an object", spec.op)))
}

/// Run one accumulator over a list of documents (used by window functions).
pub fn accumulate(op: &str, expr: &Bson, docs: &[Document]) -> R<Bson> {
    let known = ["$sum", "$avg", "$min", "$max", "$count", "$first", "$last", "$push", "$addToSet", "$mergeObjects", "$stdDevPop", "$stdDevSamp", "$median", "$percentile"];
    if !known.contains(&op) {
        return Err(MongoError::new(15952, "Location15952", format!("unknown window operator '{op}'")));
    }
    let spec = AccSpec { name: String::new(), op: op.to_string(), expr: expr.clone() };
    let mut acc = init(op);
    for d in docs {
        update_acc(&mut acc, &spec, d)?;
    }
    finish_spec(acc, &spec)
}

fn update_acc(acc: &mut Acc, spec: &AccSpec, doc: &Document) -> R<()> {
    match (spec.op.as_str(), &mut *acc) {
        ("$median" | "$percentile", Acc::Push(v)) => {
            let d = arg_doc(spec)?;
            if let Some(x) = eval(d.get("input").unwrap_or(&Bson::Null), doc)? {
                v.push(x);
            }
            return Ok(());
        }
        ("$firstN" | "$lastN" | "$maxN" | "$minN", Acc::NList(v)) => {
            let d = arg_doc(spec)?;
            if let Some(x) = eval(d.get("input").unwrap_or(&Bson::Null), doc)? {
                v.push(x);
            }
            return Ok(());
        }
        ("$top" | "$bottom" | "$topN" | "$bottomN", Acc::Top(v)) => {
            let d = arg_doc(spec)?;
            let mut item = doc.clone();
            item.insert(OUT_KEY, eval(d.get("output").unwrap_or(&Bson::Null), doc)?.unwrap_or(Bson::Null));
            v.push(item);
            return Ok(());
        }
        ("$accumulator", Acc::Js(state)) => {
            let d = arg_doc(spec)?;
            let body = |k: &str| -> R<String> {
                match d.get(k) {
                    Some(Bson::String(s)) => Ok(s.clone()),
                    Some(Bson::JavaScriptCode(c)) => Ok(c.clone()),
                    _ => Err(MongoError::new(4544712, "Location4544712", format!("$accumulator requires '{k}'"))),
                }
            };
            let args = |k: &str, doc: &Document| -> R<Vec<Bson>> {
                match d.get(k) {
                    Some(Bson::Array(a)) => a.iter().map(|e| Ok(eval(e, doc)?.unwrap_or(Bson::Null))).collect(),
                    _ => Ok(vec![]),
                }
            };
            if matches!(state, Bson::Null) {
                *state = crate::js::call_function(&body("init")?, &args("initArgs", &Document::new())?)?;
            }
            let mut call_args = vec![state.clone()];
            call_args.extend(args("accumulateArgs", doc)?);
            *state = crate::js::call_function(&body("accumulate")?, &call_args)?;
            return Ok(());
        }
        _ => {}
    }
    let val = if spec.op == "$count" { None } else { eval(&spec.expr, doc)? };
    match acc {
        Acc::Merge(m) => match val {
            Some(Bson::Document(d)) => {
                for (k, v) in d {
                    m.insert(k, v);
                }
            }
            None | Some(Bson::Null) | Some(Bson::Undefined) => {}
            Some(o) => return Err(MongoError::new(40400, "Location40400", format!("$mergeObjects requires object inputs, but input {} is of type {}", crate::update::fmt_bson(&o), crate::update::type_name(&o)))),
        },
        Acc::Std { n, mean, m2 } => {
            if let Some(v) = val.filter(is_number) {
                let x = query::as_f64(&v);
                *n += 1;
                let delta = x - *mean;
                *mean += delta / *n as f64;
                *m2 += delta * (x - *mean);
            }
        }
        Acc::NList(_) | Acc::Top(_) | Acc::Js(_) => {}
        Acc::Sum(s) => {
            if let Some(v) = val.filter(is_number) {
                *s = add_numbers(s, &v);
            }
        }
        Acc::Avg(sum, n) => {
            if let Some(v) = val.filter(is_number) {
                *sum += query::as_f64(&v);
                *n += 1;
            }
        }
        Acc::Min(cur) => {
            if let Some(v) = val.filter(|v| !matches!(v, Bson::Null | Bson::Undefined)) {
                if cur.as_ref().map(|c| compare(&v, c).is_lt()).unwrap_or(true) {
                    *cur = Some(v);
                }
            }
        }
        Acc::Max(cur) => {
            if let Some(v) = val.filter(|v| !matches!(v, Bson::Null | Bson::Undefined)) {
                if cur.as_ref().map(|c| compare(&v, c).is_gt()).unwrap_or(true) {
                    *cur = Some(v);
                }
            }
        }
        Acc::Count(n) => *n += 1,
        Acc::First(cur, set) => {
            if !*set {
                *cur = Some(val.unwrap_or(Bson::Null));
                *set = true;
            }
        }
        Acc::Last(cur) => *cur = val.unwrap_or(Bson::Null),
        Acc::Push(v) => {
            if let Some(x) = val {
                v.push(x);
            }
        }
        Acc::AddToSet(v) => {
            if let Some(x) = val {
                if !v.iter().any(|e| query::equal(e, &x)) {
                    v.push(x);
                }
            }
        }
    }
    Ok(())
}

fn finish_spec(acc: Acc, spec: &AccSpec) -> R<Bson> {
    Ok(match (spec.op.as_str(), acc) {
        ("$stdDevPop", Acc::Std { n, m2, .. }) => if n == 0 { Bson::Null } else { Bson::Double((m2 / n as f64).sqrt()) },
        ("$stdDevSamp", Acc::Std { n, m2, .. }) => if n < 2 { Bson::Null } else { Bson::Double((m2 / (n - 1) as f64).sqrt()) },
        ("$firstN" | "$lastN" | "$maxN" | "$minN", Acc::NList(mut v)) => {
            let d = arg_doc(spec)?;
            let n = match d.get("n").map(|e| eval(e, &Document::new())).transpose()?.flatten() {
                Some(Bson::Int32(i)) if i > 0 => i as usize,
                Some(Bson::Int64(i)) if i > 0 => i as usize,
                Some(Bson::Double(f)) if f > 0.0 && f.fract() == 0.0 => f as usize,
                Some(Bson::Int32(_) | Bson::Int64(_)) => return Err(MongoError::new(5787908, "Location5787908", "'n' must be greater than 0".into())),
                _ => return Err(MongoError::new(5787902, "Location5787902", "Value for 'n' must be of integral type".into())),
            };
            match spec.op.as_str() {
                "$firstN" => v.truncate(n),
                "$lastN" => {
                    let skip = v.len().saturating_sub(n);
                    v = v.split_off(skip);
                }
                op => {
                    v.retain(|x| !matches!(x, Bson::Null | Bson::Undefined));
                    v.sort_by(compare);
                    if op == "$maxN" {
                        v.reverse();
                    }
                    v.truncate(n);
                }
            }
            Bson::Array(v)
        }
        ("$top" | "$bottom" | "$topN" | "$bottomN", Acc::Top(mut docs)) => {
            let d = arg_doc(spec)?;
            let sort = d.get_document("sortBy").map_err(|_| MongoError::new(5788001, "Location5788001", "sortBy must be an object".into()))?;
            sort_docs(&mut docs, &parse_sort(sort)?);
            let outs: Vec<Bson> = docs.iter().map(|x| x.get(OUT_KEY).cloned().unwrap_or(Bson::Null)).collect();
            match spec.op.as_str() {
                "$top" => outs.into_iter().next().unwrap_or(Bson::Null),
                "$bottom" => outs.into_iter().last().unwrap_or(Bson::Null),
                op => {
                    let n = match d.get("n").map(|e| eval(e, &Document::new())).transpose()?.flatten() {
                        Some(Bson::Int32(i)) if i > 0 => i as usize,
                        Some(Bson::Int64(i)) if i > 0 => i as usize,
                        _ => return Err(MongoError::new(5787902, "Location5787902", "Value for 'n' must be of integral type and greater than 0".into())),
                    };
                    if op == "$topN" {
                        Bson::Array(outs.into_iter().take(n).collect())
                    } else {
                        let skip = outs.len().saturating_sub(n);
                        Bson::Array(outs.into_iter().skip(skip).collect())
                    }
                }
            }
        }
        ("$median" | "$percentile", Acc::Push(v)) => {
            let (_, ps, multi) = crate::agg_more::percentile_spec(&spec.op, &spec.expr)?;
            let r = crate::agg_more::percentiles(&v, &ps);
            if multi { Bson::Array(r) } else { r.into_iter().next().unwrap_or(Bson::Null) }
        }
        ("$accumulator", Acc::Js(state)) => {
            let d = arg_doc(spec)?;
            match d.get("finalize") {
                Some(Bson::String(f)) => crate::js::call_function(f, &[state])?,
                Some(Bson::JavaScriptCode(f)) => crate::js::call_function(f, &[state])?,
                _ => state,
            }
        }
        (_, acc) => finish(acc),
    })
}

fn finish(acc: Acc) -> Bson {
    match acc {
        Acc::Merge(m) => Bson::Document(m),
        Acc::Std { .. } | Acc::NList(_) | Acc::Top(_) => Bson::Null,
        Acc::Js(s) => s,
        Acc::Sum(s) => s,
        Acc::Avg(sum, n) => {
            if n == 0 { Bson::Null } else { Bson::Double(sum / n as f64) }
        }
        Acc::Min(v) | Acc::Max(v) => v.unwrap_or(Bson::Null),
        Acc::Count(n) => Bson::Int32(n as i32),
        Acc::First(v, _) => v.unwrap_or(Bson::Null),
        Acc::Last(v) => v,
        Acc::Push(v) | Acc::AddToSet(v) => Bson::Array(v),
    }
}

fn group(docs: Vec<Document>, spec: &Document) -> R<Vec<Document>> {
    for (_, v) in spec {
        let e = match v {
            Bson::Document(d) if d.len() == 1 => d.values().next().unwrap(),
            other => other,
        };
        crate::expr::validate(e)?;
    }
    let id_expr = spec.get("_id").ok_or_else(|| MongoError::new(15955, "Location15955", "a group specification must include an _id".into()))?;
    let accs = parse_accumulators(spec)?;
    let mut order: Vec<Vec<u8>> = Vec::new();
    let mut groups: HashMap<Vec<u8>, (Bson, Vec<Acc>)> = HashMap::new();
    for d in &docs {
        let key = eval(id_expr, d)?.unwrap_or(Bson::Null);
        let kb = encode_sortable(&key);
        if !groups.contains_key(&kb) {
            order.push(kb.clone());
            groups.insert(kb.clone(), (key, accs.iter().map(|a| init(&a.op)).collect()));
        }
        let (_, state) = groups.get_mut(&kb).unwrap();
        for (acc, spec) in state.iter_mut().zip(&accs) {
            update_acc(acc, spec, d)?;
        }
    }
    let mut out = Vec::with_capacity(order.len());
    for kb in order {
        let (key, state) = groups.remove(&kb).unwrap();
        let mut d = Document::new();
        d.insert("_id", key);
        for (acc, spec) in state.into_iter().zip(&accs) {
            d.insert(spec.name.clone(), finish_spec(acc, spec)?);
        }
        out.push(d);
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// $project / $addFields / $set
// ---------------------------------------------------------------------------

enum Node {
    Include,
    Exclude,
    Computed(Bson),
    Sub(Vec<(String, Node)>),
}

fn is_expr_doc(d: &Document) -> bool {
    d.keys().any(|k| k.starts_with('$'))
}

fn insert_node(nodes: &mut Vec<(String, Node)>, path: &[&str], leaf: Node, full: &str, project: bool) -> R<()> {
    let collision = |deeper: bool| {
        if project {
            let code = if deeper { 31249 } else { 31250 };
            MongoError::new(code, Box::leak(format!("Location{code}").into_boxed_str()), format!("Invalid $project :: caused by :: Path collision at {full}"))
        } else {
            MongoError::new(40176, "Location40176", format!("specification contains two conflicting paths. Cannot specify both '{}' and '{full}'", path[0]))
        }
    };
    let pos = nodes.iter().position(|(k, _)| k == path[0]);
    if path.len() == 1 {
        if pos.is_some() {
            return Err(collision(false));
        }
        nodes.push((path[0].to_string(), leaf));
        return Ok(());
    }
    match pos {
        Some(i) => match &mut nodes[i].1 {
            Node::Sub(ch) => insert_node(ch, &path[1..], leaf, full, project),
            _ => Err(collision(true)),
        },
        None => {
            let mut ch = Vec::new();
            insert_node(&mut ch, &path[1..], leaf, full, project)?;
            nodes.push((path[0].to_string(), Node::Sub(ch)));
            Ok(())
        }
    }
}

fn build_tree(spec: &Document, project: bool, prefix: &str, nodes: &mut Vec<(String, Node)>) -> R<()> {
    for (k, v) in spec {
        let full = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
        let parts: Vec<&str> = k.split('.').collect();
        let leaf = match v {
            Bson::Document(d) if project && d.is_empty() => {
                return Err(MongoError::new(51270, "Location51270", format!("Invalid $project :: caused by :: An empty sub-projection is not a valid value. Found empty object at path {full}")));
            }
            Bson::Document(d) if !is_expr_doc(d) && !d.is_empty() => {
                // A nested specification: build it as its own subtree and graft it in.
                let mut sub = Vec::new();
                build_tree(d, project, &full, &mut sub)?;
                let mut tmp = Vec::new();
                insert_node(&mut tmp, &parts, Node::Sub(sub), &full, project)?;
                merge_into(nodes, tmp, &full, project)?;
                continue;
            }
            Bson::Boolean(b) if project => if *b { Node::Include } else { Node::Exclude },
            n if project && query::is_number(n) => if query::as_f64(n) != 0.0 { Node::Include } else { Node::Exclude },
            other => Node::Computed(other.clone()),
        };
        insert_node(nodes, &parts, leaf, &full, project)?;
    }
    Ok(())
}

fn merge_into(dst: &mut Vec<(String, Node)>, src: Vec<(String, Node)>, full: &str, project: bool) -> R<()> {
    for (k, n) in src {
        match dst.iter().position(|(dk, _)| *dk == k) {
            None => dst.push((k, n)),
            Some(i) => match (&mut dst[i].1, n) {
                (Node::Sub(a), Node::Sub(b)) => merge_into(a, b, full, project)?,
                _ => {
                    return Err(if project {
                        MongoError::new(31249, "Location31249", format!("Invalid $project :: caused by :: Path collision at {full}"))
                    } else {
                        MongoError::new(40176, "Location40176", format!("specification contains two conflicting paths. Cannot specify both '{k}' and '{full}'"))
                    })
                }
            },
        }
    }
    Ok(())
}

fn has_computed(nodes: &[(String, Node)]) -> bool {
    nodes.iter().any(|(_, n)| match n {
        Node::Computed(_) => true,
        Node::Sub(c) => has_computed(c),
        _ => false,
    })
}

fn has_exclude(nodes: &[(String, Node)]) -> Option<String> {
    for (k, n) in nodes {
        match n {
            Node::Exclude => return Some(k.clone()),
            Node::Sub(c) => {
                if let Some(x) = has_exclude(c) {
                    return Some(format!("{k}.{x}"));
                }
            }
            _ => {}
        }
    }
    None
}

fn apply_inclusion(d: &Document, nodes: &[(String, Node)], root: &Document) -> R<Document> {
    let mut out = Document::new();
    let mut emitted: Vec<&str> = Vec::new();
    for (k, v) in d {
        let Some((_, node)) = nodes.iter().find(|(nk, _)| nk == k) else { continue };
        emitted.push(k);
        match node {
            Node::Include => {
                out.insert(k.clone(), v.clone());
            }
            Node::Exclude | Node::Computed(_) => {}
            Node::Sub(ch) => match v {
                Bson::Document(vd) => {
                    out.insert(k.clone(), Bson::Document(apply_inclusion(vd, ch, root)?));
                }
                Bson::Array(a) => {
                    let mut arr = Vec::new();
                    for e in a {
                        if let Bson::Document(ed) = e {
                            arr.push(Bson::Document(apply_inclusion(ed, ch, root)?));
                        } else if has_computed(ch) {
                            arr.push(Bson::Document(apply_inclusion(&Document::new(), ch, root)?));
                        }
                    }
                    out.insert(k.clone(), Bson::Array(arr));
                }
                _ => {
                    if has_computed(ch) {
                        out.insert(k.clone(), Bson::Document(apply_inclusion(&Document::new(), ch, root)?));
                    }
                }
            },
        }
    }
    // Computed fields come after the included ones, in specification order.
    for (k, node) in nodes {
        match node {
            Node::Computed(e) => {
                if let Some(x) = eval(e, root)? {
                    out.insert(k.clone(), x);
                }
            }
            Node::Sub(ch) if has_computed(ch) && !emitted.contains(&k.as_str()) => {
                let sub = apply_inclusion(&Document::new(), ch, root)?;
                out.insert(k.clone(), Bson::Document(sub));
            }
            _ => {}
        }
    }
    Ok(out)
}

fn flatten(nodes: &[(String, Node)], prefix: &str, out: &mut Document) {
    for (k, n) in nodes {
        let p = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
        match n {
            Node::Include => {
                out.insert(p, 1);
            }
            Node::Exclude => {
                out.insert(p, 0);
            }
            Node::Sub(c) => flatten(c, &p, out),
            Node::Computed(_) => {}
        }
    }
}

fn check_project_shapes(nodes: &[(String, Node)]) -> R<()> {
    for (_, n) in nodes {
        match n {
            Node::Computed(e) => {
                crate::expr::check_single_operator(e, 16410, "FieldPath field names may not start with '$'. Consider using $getField or $setField. Found:")?;
                if let Bson::Document(d) = e {
                    if let Some((k, _)) = d.iter().next() {
                        if k.starts_with('$') && !crate::expr::is_known_operator(k) {
                            return Err(MongoError::new(31325, "Location31325", format!("Invalid $project :: caused by :: Unknown expression {k}")));
                        }
                    }
                }
            }
            Node::Sub(c) => check_project_shapes(c)?,
            _ => {}
        }
    }
    Ok(())
}

fn validate_tree(nodes: &[(String, Node)]) -> R<()> {
    for (_, n) in nodes {
        match n {
            Node::Computed(e) => crate::expr::validate(e)?,
            Node::Sub(c) => validate_tree(c)?,
            _ => {}
        }
    }
    Ok(())
}

fn project_stage(docs: Vec<Document>, spec: &Document) -> R<Vec<Document>> {
    if spec.is_empty() {
        return Err(MongoError::new(51272, "Location51272", "Invalid $project :: caused by :: projection specification must have at least one field".into()));
    }
    let mut tree = Vec::new();
    build_tree(spec, true, "", &mut tree)?;
    if !has_computed(&tree) {
        let mut flat = Document::new();
        flatten(&tree, "", &mut flat);
        return Ok(match parse_projection(&flat)? {
            Some(p) => docs.iter().map(|d| project(d, &p)).collect(),
            None => docs,
        });
    }
    check_project_shapes(&tree)?;
    validate_tree(&tree)?;
    // A computed `_id` is an ordinary computed field; otherwise `_id` leads the output unless excluded.
    let id_computed = matches!(tree.iter().find(|(k, _)| k == "_id"), Some((_, Node::Computed(_))));
    let id_node = if id_computed { None } else { tree.iter().position(|(k, _)| k == "_id").map(|i| tree.remove(i).1) };
    if let Some(x) = has_exclude(&tree) {
        return Err(MongoError::new(31254, "Location31254", format!("Invalid $project :: caused by :: Cannot do exclusion on field {x} in inclusion projection")));
    }
    let mut out = Vec::with_capacity(docs.len());
    for d in docs {
        let mut r = Document::new();
        match &id_node {
            Some(Node::Exclude) => {}
            _ if id_computed => {}
            _ => {
                if let Some(id) = d.get("_id") {
                    r.insert("_id", id.clone());
                }
            }
        }
        for (k, v) in apply_inclusion(&d, &tree, &d)? {
            r.insert(k, v);
        }
        out.push(r);
    }
    Ok(out)
}

fn add_fields_doc(d: &Document, nodes: &[(String, Node)], root: &Document) -> R<Document> {
    let mut out = d.clone();
    for (k, node) in nodes {
        match node {
            Node::Computed(e) => match eval(e, root)? {
                Some(x) => {
                    out.insert(k.clone(), x);
                }
                None => {
                    out.remove(k);
                }
            },
            Node::Sub(ch) => {
                let new = add_fields_value(out.get(k), ch, root)?;
                out.insert(k.clone(), new);
            }
            _ => {}
        }
    }
    Ok(out)
}

/// A nested `$addFields` specification applied to an existing value: documents are merged into,
/// arrays are applied element by element, anything else is replaced by a fresh document.
fn add_fields_value(cur: Option<&Bson>, ch: &[(String, Node)], root: &Document) -> R<Bson> {
    Ok(match cur {
        Some(Bson::Document(vd)) => Bson::Document(add_fields_doc(vd, ch, root)?),
        Some(Bson::Array(a)) => {
            let mut arr = Vec::with_capacity(a.len());
            for e in a {
                arr.push(add_fields_value(Some(e), ch, root)?);
            }
            Bson::Array(arr)
        }
        _ => Bson::Document(add_fields_doc(&Document::new(), ch, root)?),
    })
}

fn add_fields_stage(docs: Vec<Document>, spec: &Document, name: &str) -> R<Vec<Document>> {
    let mut tree = Vec::new();
    build_tree(spec, false, "", &mut tree)?;
    validate_tree(&tree)?;
    for (_, n) in &tree {
        if let Node::Computed(e) = n {
            crate::expr::check_single_operator(e, 40181, &format!("an expression specification must contain exactly one field, the name of the expression. Found several fields in (stage {name}):"))?;
        }
    }
    let mut out = Vec::with_capacity(docs.len());
    for d in &docs {
        out.push(add_fields_doc(d, &tree, d)?);
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// $unwind
// ---------------------------------------------------------------------------

fn loc(code: i32, msg: impl Into<String>) -> MongoError {
    // The server reports these as "Location<code>".
    MongoError::new(code, Box::leak(format!("Location{code}").into_boxed_str()), msg.into())
}

fn get_exact<'a>(doc: &'a Document, parts: &[&str]) -> Option<&'a Bson> {
    let mut cur = doc.get(parts[0])?;
    for p in &parts[1..] {
        cur = match cur {
            Bson::Document(d) => d.get(*p)?,
            _ => return None,
        };
    }
    Some(cur)
}

fn unwind(docs: Vec<Document>, arg: &Bson) -> R<Vec<Document>> {
    let (path, idx, preserve) = match arg {
        Bson::String(s) => (s.clone(), None, false),
        Bson::Document(d) => {
            let mut path = None;
            let mut idx = None;
            let mut preserve = false;
            for (k, v) in d {
                match k.as_str() {
                    "path" => match v {
                        Bson::String(s) => path = Some(s.clone()),
                        _ => return Err(loc(28808, "expected a string as the path for $unwind stage, got ".to_string() + crate::update::type_name(v))),
                    },
                    "includeArrayIndex" => match v {
                        Bson::String(s) => {
                            if s.starts_with('$') {
                                return Err(loc(28822, format!("includeArrayIndex option to $unwind stage should not be prefixed with a '$': {s}")));
                            }
                            idx = Some(s.clone());
                        }
                        _ => return Err(loc(28810, format!("expected a non-empty string for the includeArrayIndex option to $unwind stage, got {}", crate::update::type_name(v)))),
                    },
                    "preserveNullAndEmptyArrays" => match v {
                        Bson::Boolean(b) => preserve = *b,
                        _ => return Err(loc(28809, format!("expected a boolean for the preserveNullAndEmptyArrays option to $unwind stage, got {}", crate::update::type_name(v)))),
                    },
                    other => return Err(loc(28811, format!("unrecognized option to $unwind stage: {other}"))),
                }
            }
            (path.ok_or_else(|| loc(28812, "no path specified to $unwind stage"))?, idx, preserve)
        }
        _ => return Err(loc(15981, "expected either a string or an object as specification for $unwind stage, got ".to_string() + crate::update::type_name(arg))),
    };
    let Some(path) = path.strip_prefix('$').map(str::to_string) else {
        return Err(loc(28818, format!("path option to $unwind stage should be prefixed with a '$': {path}")));
    };
    if path.is_empty() {
        return Err(loc(40352, "FieldPath cannot be constructed with empty string"));
    }
    if path.ends_with('.') && !path.ends_with("..") {
        return Err(loc(40353, "FieldPath must not end with a '.'."));
    }
    if path.split('.').any(str::is_empty) {
        return Err(loc(15998, "FieldPath field names may not be empty strings."));
    }
    let parts: Vec<&str> = path.split('.').collect();
    let mut out = Vec::new();
    for d in docs {
        let cur = get_exact(&d, &parts).cloned();
        let set_idx = |doc: &mut Document, v: Bson| -> R<()> {
            if let Some(i) = &idx {
                crate::update::set_path(doc, &i.split('.').collect::<Vec<_>>(), v)?;
            }
            Ok(())
        };
        match cur {
            Some(Bson::Array(a)) if !a.is_empty() => {
                for (i, e) in a.into_iter().enumerate() {
                    let mut nd = d.clone();
                    crate::update::set_path(&mut nd, &parts, e)?;
                    set_idx(&mut nd, Bson::Int64(i as i64))?;
                    out.push(nd);
                }
            }
            Some(Bson::Array(_)) => {
                if preserve {
                    let mut nd = d.clone();
                    crate::update::unset_path(&mut nd, &parts);
                    set_idx(&mut nd, Bson::Null)?;
                    out.push(nd);
                }
            }
            None | Some(Bson::Null) | Some(Bson::Undefined) => {
                if preserve {
                    let mut nd = d.clone();
                    set_idx(&mut nd, Bson::Null)?;
                    out.push(nd);
                }
            }
            Some(_) => {
                let mut nd = d.clone();
                set_idx(&mut nd, Bson::Null)?;
                out.push(nd);
            }
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// $lookup
// ---------------------------------------------------------------------------

pub type Foreign = HashMap<String, Vec<Document>>;

/// Names of the collections `$lookup` stages in `pipeline` read from (the caller preloads them).
pub fn lookup_collections(pipeline: &[Bson]) -> Vec<String> {
    let mut all = Vec::new();
    crate::agg_ext::extra_collections(pipeline, &mut all);
    let mut out: Vec<String> = Vec::new();
    for c in all {
        if !out.contains(&c) {
            out.push(c);
        }
    }
    out
}

/// Values a `$lookup` local field offers: the aggregation field-path value; an array contributes its
/// elements, and a missing field or an empty array counts as null.
fn local_values(doc: &Document, path: &str) -> Vec<Bson> {
    match crate::expr::field_path(doc, path) {
        None => vec![Bson::Null],
        Some(Bson::Array(a)) if a.is_empty() => vec![Bson::Null],
        Some(Bson::Array(a)) => a,
        Some(v) => vec![v],
    }
}

/// Values a foreign document can be matched on: the field-path value, plus the elements of an array.
fn foreign_values(doc: &Document, path: &str) -> Vec<Bson> {
    match crate::expr::field_path(doc, path) {
        None => vec![Bson::Null],
        Some(Bson::Array(a)) => {
            let mut out = vec![Bson::Array(a.clone())];
            out.extend(a);
            out
        }
        Some(v) => vec![v],
    }
}

fn lookup(docs: Vec<Document>, arg: &Bson, foreign: &Foreign) -> R<Vec<Document>> {
    let spec = arg.as_document().ok_or_else(|| MongoError::failed_to_parse("the $lookup specification must be a non-empty object".into()))?;
    let mut from = None;
    let (mut lf, mut ff, mut as_) = (None, None, None);
    let mut let_spec: Option<Document> = None;
    let mut pipeline: Option<Vec<Bson>> = None;
    for (k, v) in spec {
        let s = |v: &Bson| -> R<String> {
            v.as_str().map(str::to_string).ok_or_else(|| MongoError::failed_to_parse(format!("$lookup argument '{k}' must be a string, found {k}: {}", crate::update::fmt_bson(v))))
        };
        match k.as_str() {
            "from" => from = Some(s(v)?),
            "localField" => lf = Some(s(v)?),
            "foreignField" => ff = Some(s(v)?),
            "as" => as_ = Some(s(v)?),
            "let" => {
                let_spec = Some(v.as_document().cloned().ok_or_else(|| MongoError::failed_to_parse("$lookup: 'let' must be an object".into()))?);
            }
            "pipeline" => {
                pipeline = Some(match v {
                    Bson::Array(p) => p.clone(),
                    _ => return Err(MongoError::failed_to_parse("$lookup: 'pipeline' must be an array".into())),
                });
            }
            other => return Err(MongoError::failed_to_parse(format!("unknown argument to $lookup: {other}"))),
        }
    }
    let as_ = as_.ok_or_else(|| MongoError::failed_to_parse("must specify 'as' field for a $lookup".into()))?;
    let from = from.ok_or_else(|| MongoError::failed_to_parse("must specify 'from' field for a $lookup".into()))?;
    if let Some(p) = pipeline {
        let (lf, ff) = match (lf, ff) {
            (Some(l), Some(f)) => (Some(l), Some(f)),
            (None, None) => (None, None),
            _ => return Err(MongoError::failed_to_parse("$lookup requires both or neither of 'localField' and 'foreignField' to be specified".into())),
        };
        let empty = Vec::new();
        let fdocs = foreign.get(&from).unwrap_or(&empty);
        let as_parts: Vec<&str> = as_.split('.').collect();
        let mut out = Vec::with_capacity(docs.len());
        for mut d in docs {
            let mut vars: Vec<(String, Bson)> = Vec::new();
            if let Some(ls) = &let_spec {
                for (name, e) in ls {
                    vars.push((name.clone(), eval(e, &d)?.unwrap_or(Bson::Null)));
                }
            }
            let candidates: Vec<Document> = match (&lf, &ff) {
                (Some(l), Some(f)) => {
                    let keys: Vec<Vec<u8>> = local_values(&d, l).iter().map(encode_sortable).collect();
                    fdocs.iter().filter(|fd| foreign_values(fd, f).iter().any(|v| keys.contains(&encode_sortable(v)))).cloned().collect()
                }
                _ => fdocs.clone(),
            };
            let res = if matches!(p.first().and_then(|s| s.as_document()).map(|d| d.contains_key("$geoNear")), Some(true)) {
                let geo: Vec<(String, bool)> = foreign.get(&format!("\u{0}geo:{from}")).map(|m| m.iter().map(|d| (d.get_str("field").unwrap_or("").to_string(), d.get_bool("sphere").unwrap_or(false))).collect()).unwrap_or_default();
                let stage = p[0].as_document().and_then(|d| d.get("$geoNear")).cloned().unwrap_or(Bson::Null);
                let near = crate::cmd_data::geo_near_pure(candidates, &stage, &geo)?;
                crate::expr::with_ambient(vars, || run_pipeline(near, &p[1..], foreign))?
            } else {
                crate::expr::with_ambient(vars, || run_pipeline(candidates, &p, foreign))?
            };
            crate::update::set_path(&mut d, &as_parts, Bson::Array(res.into_iter().map(Bson::Document).collect()))?;
            out.push(d);
        }
        return Ok(out);
    }
    let (lf, ff) = match (lf, ff) {
        (Some(l), Some(f)) => (l, f),
        (None, None) => return Err(MongoError::failed_to_parse("$lookup requires either 'pipeline' or both 'localField' and 'foreignField' to be specified".into())),
        (Some(_), None) => return Err(MongoError::failed_to_parse("$lookup requires both or neither of 'localField' and 'foreignField' to be specified".into())),
        (None, Some(_)) => return Err(MongoError::failed_to_parse("$lookup requires both or neither of 'localField' and 'foreignField' to be specified".into())),
    };
    let empty = Vec::new();
    let fdocs = foreign.get(&from).unwrap_or(&empty);
    let mut index: HashMap<Vec<u8>, Vec<usize>> = HashMap::new();
    for (i, fd) in fdocs.iter().enumerate() {
        let mut seen: Vec<Vec<u8>> = Vec::new();
        for v in foreign_values(fd, &ff) {
            let k = encode_sortable(&v);
            if !seen.contains(&k) {
                index.entry(k.clone()).or_default().push(i);
                seen.push(k);
            }
        }
    }
    let as_parts: Vec<&str> = as_.split('.').collect();
    let mut out = Vec::with_capacity(docs.len());
    for mut d in docs {
        let mut hits: Vec<usize> = Vec::new();
        for v in local_values(&d, &lf) {
            if let Some(ix) = index.get(&encode_sortable(&v)) {
                hits.extend(ix);
            }
        }
        hits.sort_unstable();
        hits.dedup();
        let arr: Vec<Bson> = hits.into_iter().map(|i| Bson::Document(fdocs[i].clone())).collect();
        crate::update::set_path(&mut d, &as_parts, Bson::Array(arr))?;
        out.push(d);
    }
    Ok(out)
}

fn positive_int(v: &Bson, stage: &str) -> R<usize> {
    let code = if stage == "$skip" { 5107200 } else { 5107201 };
    let shown = crate::update::fmt_bson(v);
    let n = match v {
        Bson::Int32(i) => *i as i64,
        Bson::Int64(i) => *i,
        Bson::Double(f) if f.fract() == 0.0 => *f as i64,
        Bson::Double(_) => return Err(MongoError::new(code, if code == 5107200 { "Location5107200" } else { "Location5107201" }, format!("invalid argument to {stage} stage: Expected an integer: {stage}: {shown}"))),
        _ => return Err(MongoError::new(code, if code == 5107200 { "Location5107200" } else { "Location5107201" }, format!("invalid argument to {stage} stage: Expected a number in: {stage}: {shown}"))),
    };
    if n < 0 {
        return Err(MongoError::new(code, if code == 5107200 { "Location5107200" } else { "Location5107201" }, format!("invalid argument to {stage} stage: Expected a non-negative number in: {stage}: {shown}")));
    }
    if stage == "$limit" && n == 0 {
        return Err(MongoError::new(15958, "Location15958", "the limit must be positive".into()));
    }
    Ok(n as usize)
}

/// Does a match filter use `$where` anywhere?
fn has_where(f: &Document) -> bool {
    f.iter().any(|(k, v)| {
        k == "$where"
            || match v {
                Bson::Array(a) => a.iter().any(|x| x.as_document().map(has_where).unwrap_or(false)),
                Bson::Document(d) => has_where(d),
                _ => false,
            }
    })
}

pub fn run_pipeline(mut docs: Vec<Document>, pipeline: &[Bson], foreign: &Foreign) -> R<Vec<Document>> {
    for stage in pipeline {
        let sd = stage.as_document().filter(|d| d.len() == 1).ok_or_else(|| {
            MongoError::new(40323, "Location40323", "A pipeline stage specification object must contain exactly one field.".into())
        })?;
        let (name, arg) = sd.iter().next().unwrap();
        // Search scores and geo distances travel with the documents through stages that map one document to one.
        let carried: Option<Vec<Vec<(&'static str, Bson)>>> = if matches!(name.as_str(), "$project" | "$addFields" | "$set" | "$unset" | "$replaceRoot" | "$replaceWith") {
            let keys = [crate::text::SCORE_KEY, crate::search::SCORE_KEY, crate::geo::DIST_KEY, crate::geo::POINT_KEY];
            if docs.iter().any(|d| keys.iter().any(|k| d.contains_key(*k))) {
                Some(docs.iter().map(|d| keys.iter().filter_map(|k| d.get(*k).map(|v| (*k, v.clone()))).collect()).collect())
            } else {
                None
            }
        } else {
            None
        };
        match name.as_str() {
            "$match" => {
                let f = arg.as_document().ok_or_else(|| MongoError::new(15959, "Location15959", "the match filter must be an expression in an object".into()))?;
                if has_where(f) {
                    return Err(MongoError::new(2, "BadValue", "$where is not allowed inside of a $match aggregation expression".into()));
                }
                let mut kept = Vec::new();
                for d in docs {
                    if matches(&d, f)? {
                        kept.push(d);
                    }
                }
                docs = kept;
            }
            "$sort" => {
                let spec = arg.as_document().ok_or_else(|| MongoError::new(15973, "Location15973", "the $sort key specification must be an object".into()))?;
                sort_docs(&mut docs, &parse_sort(spec)?);
            }
            "$limit" => docs.truncate(positive_int(arg, "$limit")?),
            "$skip" => {
                let n = positive_int(arg, "$skip")?;
                docs = docs.into_iter().skip(n).collect();
            }
            "$project" => {
                let spec = arg.as_document().ok_or_else(|| MongoError::new(15969, "Location15969", "$project specification must be an object".into()))?;
                docs = project_stage(docs, spec)?;
            }
            "$addFields" | "$set" => {
                let spec = arg.as_document().ok_or_else(|| MongoError::new(40272, "Location40272", format!("{name} specification stage must be an object, got {}", crate::update::type_name(arg))))?;
                docs = add_fields_stage(docs, spec, name)?;
            }
            "$unwind" => docs = unwind(docs, arg)?,
            "$lookup" => docs = lookup(docs, arg, foreign)?,
            "$group" => {
                let spec = arg.as_document().ok_or_else(|| MongoError::new(15947, "Location15947", "a group's fields must be specified in an object".into()))?;
                docs = group(docs, spec)?;
            }
            "$count" => {
                let field = arg.as_str().filter(|s| !s.is_empty() && !s.starts_with('$') && !s.contains('.')).ok_or_else(|| {
                    MongoError::new(40156, "Location40156", "the count field must be a non-empty string".into())
                })?;
                let n = docs.len();
                docs = if n == 0 { vec![] } else { vec![bson::doc! { field: Bson::Int32(n as i32) }] };
            }
            other => match crate::agg_ext::run_stage(other, arg, std::mem::take(&mut docs), foreign)? {
                Some(d) => docs = d,
                None => return Err(MongoError::new(40324, "Location40324", format!("Unrecognized pipeline stage name: '{other}'"))),
            },
        }
        if let Some(c) = carried {
            if c.len() == docs.len() {
                for (d, kv) in docs.iter_mut().zip(c) {
                    for (k, v) in kv {
                        if !d.contains_key(k) {
                            d.insert(k, v);
                        }
                    }
                }
            }
        }
    }
    Ok(docs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    fn run(docs: Vec<Document>, p: Vec<Document>) -> Vec<Document> {
        run_pipeline(docs, &p.into_iter().map(Bson::Document).collect::<Vec<_>>(), &Foreign::new()).unwrap()
    }

    #[test]
    fn group_accumulators() {
        let docs = vec![doc! {"k": "a", "v": 1}, doc! {"k": "a", "v": 3}, doc! {"k": "b", "v": 5}, doc! {"k": "b"}];
        let out = run(docs, vec![doc! {"$group": {"_id": "$k", "s": {"$sum": "$v"}, "n": {"$sum": 1}, "a": {"$avg": "$v"}, "mn": {"$min": "$v"}, "mx": {"$max": "$v"}, "c": {"$count": {}}}}]);
        assert_eq!(out[0], doc! {"_id": "a", "s": 4, "n": 2, "a": 2.0, "mn": 1, "mx": 3, "c": 2});
        assert_eq!(out[1], doc! {"_id": "b", "s": 5, "n": 2, "a": 5.0, "mn": 5, "mx": 5, "c": 2});
    }

    #[test]
    fn project_add_fields_unwind_lookup() {
        let docs = vec![doc! {"_id": 1, "a": 2, "arr": [1, 2]}, doc! {"_id": 2, "a": 3, "arr": []}];
        let out = run(docs.clone(), vec![doc! {"$project": {"b": {"$add": ["$a", 1]}, "a": 1}}]);
        assert_eq!(out[0], doc! {"_id": 1, "a": 2, "b": 3});
        let out = run(docs.clone(), vec![doc! {"$addFields": {"c": {"$size": "$arr"}}}]);
        assert_eq!(out[1], doc! {"_id": 2, "a": 3, "arr": [], "c": 0});
        let out = run(docs.clone(), vec![doc! {"$unwind": {"path": "$arr", "includeArrayIndex": "i"}}]);
        assert_eq!(out, vec![doc! {"_id": 1, "a": 2, "arr": 1, "i": 0_i64}, doc! {"_id": 1, "a": 2, "arr": 2, "i": 1_i64}]);
        let mut foreign = Foreign::new();
        foreign.insert("f".into(), vec![doc! {"_id": 9, "k": 2}, doc! {"_id": 8, "k": 5}]);
        let p: Vec<Bson> = vec![Bson::Document(doc! {"$lookup": {"from": "f", "localField": "a", "foreignField": "k", "as": "m"}})];
        let out = run_pipeline(docs, &p, &foreign).unwrap();
        assert_eq!(out[0].get_array("m").unwrap().len(), 1);
        assert_eq!(out[1].get_array("m").unwrap().len(), 0);
    }

    #[test]
    fn count_via_match_group() {
        let docs = vec![doc! {"a": 1}, doc! {"a": 2}, doc! {"a": 3}];
        let out = run(docs, vec![doc! {"$match": {"a": {"$gt": 1}}}, doc! {"$group": {"_id": 1, "n": {"$sum": 1}}}]);
        assert_eq!(out, vec![doc! {"_id": 1, "n": 2}]);
    }
}
