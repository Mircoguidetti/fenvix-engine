//! Commands that change the capped state of a collection: `collMod` with `cappedSize` / `cappedMax`, `convertToCapped` and
//! `cloneCollectionAsCapped`.

use crate::cmd::{collection_name, ok, Server, R};
use crate::coll::Writes;
use crate::dbs::Entry;
use crate::error::MongoError;
use crate::layout::{doc_prefix, idx_collection_prefix};
use bson::{doc, Bson, Document};
use tenant_bench::lifecycle::Op;

const MAX_SIZE: i64 = 1125899906842624;
/// What mongod shows as `max` once `cappedMax` was set to 0 or less: no limit.
const NO_MAX: i64 = 2147483647;

fn wrong_type(cmd: &str, field: &str, v: &Bson) -> MongoError {
    MongoError::type_mismatch(format!(
        "BSON field '{cmd}.{field}' is the wrong type '{}', expected types '[long, int, decimal, double']",
        crate::update::type_name(v)
    ))
}

fn number(cmd: &str, field: &str, v: &Bson) -> R<i64> {
    match v {
        Bson::Int32(i) => Ok(*i as i64),
        Bson::Int64(i) => Ok(*i),
        Bson::Double(f) => Ok(*f as i64),
        Bson::Decimal128(d) => Ok(d.to_string().parse::<f64>().map(|f| f as i64).unwrap_or(0)),
        other => Err(wrong_type(cmd, field, other)),
    }
}

/// The options document of a capped collection: capped, size, max, then whatever else the collection has.
fn with_cap(existing: &Document, size: Bson, max: Option<Bson>) -> Document {
    let mut d = doc! { "capped": true, "size": size };
    if let Some(m) = max {
        d.insert("max", m);
    }
    for (k, v) in existing {
        if !matches!(k.as_str(), "capped" | "size" | "max") {
            d.insert(k.clone(), v.clone());
        }
    }
    d
}

/// Every document of a collection in natural order: (sequence number, encoded `_id`, BSON size).
async fn natural_docs(g: &mut Entry, coll: &str) -> R<Vec<(u64, Vec<u8>, i64)>> {
    let np = crate::natural::prefix(coll);
    let end = crate::keyenc::prefix_end(&np);
    let dp = doc_prefix(coll);
    let mut start = np.clone();
    let mut out = Vec::new();
    loop {
        let rows = g.scan_range(&start, end.as_deref(), false, Some(512), 1 << 20).await?;
        let n = rows.len();
        for (k, id_enc) in &rows {
            let seq = u64::from_be_bytes(k[np.len()..np.len() + 8].try_into().unwrap_or([0; 8]));
            let mut dk = dp.clone();
            dk.extend(id_enc.iter());
            if let Some(v) = g.get_live(&dk).await? {
                out.push((seq, id_enc.to_vec(), crate::natural::bson_part(&v).len() as i64));
            }
        }
        if n < 512 {
            break;
        }
        let mut nk = rows.last().unwrap().0.to_vec();
        nk.push(0);
        start = nk;
    }
    Ok(out)
}

impl Server {
    /// `collMod` with `cappedSize` and / or `cappedMax`.
    pub(crate) async fn coll_mod_capped(&self, g: &mut Entry, coll: &str, cmd: &Document) -> R<()> {
        let size = match cmd.get("cappedSize") {
            None => None,
            Some(v) => {
                let n = number("collMod", "cappedSize", v)?;
                if n < 1 {
                    return Err(MongoError::new(51024, "Location51024", format!("BSON field 'cappedSize' value must be >= 1, actual value '{n}'")));
                }
                if n > MAX_SIZE {
                    return Err(MongoError::new(51024, "Location51024", format!("BSON field 'cappedSize' value must be <= {MAX_SIZE}, actual value '{n}'")));
                }
                Some(n)
            }
        };
        let max = match cmd.get("cappedMax") {
            None => None,
            Some(v) => Some(number("collMod", "cappedMax", v)?),
        };
        let opts = g.coll_opts(coll).await?;
        let Some(cap) = opts.as_ref().and_then(|o| o.cap.clone()) else {
            return Err(MongoError::new(72, "InvalidOptions", "Collection must be capped.".into()));
        };
        let raw = opts.as_ref().map(|o| o.raw.clone()).unwrap_or_default();
        let new_size = match (size, cmd.get("cappedSize")) {
            (Some(n), Some(v)) => crate::capped::typed(v, n),
            _ => raw.get("size").cloned().unwrap_or(Bson::Int64(cap.size)),
        };
        let new_max = match (max, cmd.get("cappedMax")) {
            (Some(m), _) if m <= 0 => Some(Bson::Int32(NO_MAX as i32)),
            (Some(m), Some(v)) => Some(crate::capped::typed(v, m)),
            _ => raw.get("max").cloned(),
        };
        let new = with_cap(&raw, new_size, new_max);
        g.apply(vec![Op::Put(crate::collopts::meta_key(coll), new.to_vec()?)]).await?;
        g.set_coll_opts(coll, Some(crate::collopts::CollOpts::parse(new)));
        Ok(())
    }

    /// `convertToCapped`: the collection becomes capped with `size` bytes, keeping the newest documents that fit. Secondary
    /// indexes are not kept (mongod rebuilds the collection through a clone that only keeps `_id_`).
    pub(crate) async fn convert_to_capped(&self, db: &str, cmd: &Document) -> R<Document> {
        let coll = collection_name(cmd, "convertToCapped")?;
        let size = match cmd.get("size") {
            None => 0,
            Some(v) => number("convertToCapped", "size", v)?,
        };
        if size <= 0 {
            return Err(MongoError::new(72, "InvalidOptions", "invalid command spec".into()));
        }
        let mut g = self.dbs.lock(db).await;
        if !g.collections().await?.iter().any(|n| n == coll) {
            return Err(MongoError::namespace_not_found(format!("source collection {db}.{coll} does not exist")));
        }
        let opts = g.coll_opts(coll).await?;
        let raw = opts.as_ref().map(|o| o.raw.clone()).unwrap_or_default();
        let max = opts.as_ref().and_then(|o| o.cap.as_ref()).and_then(|c| c.max);
        let new = with_cap(&raw, crate::capped::typed(cmd.get("size").unwrap_or(&Bson::Int64(0)), size), raw.get("max").cloned());
        let mut ops: Vec<Op> = Vec::new();
        // the secondary indexes go
        let indexes = g.indexes(coll).await?;
        if !indexes.is_empty() {
            for (k, _) in g.scan(&idx_collection_prefix(coll)).await? {
                ops.push(Op::Delete(k.to_vec()));
            }
            ops.push(Entry::indexes_op(coll, &[])?);
        }
        let docs = natural_docs(&mut g, coll).await?;
        let (mut n, mut bytes) = (docs.len() as i64, docs.iter().map(|d| d.2).sum::<i64>());
        let mut w = Writes::new();
        for (_, id_enc, len) in &docs {
            if !((bytes > size || max.map(|m| n > m).unwrap_or(false)) && n > 1) {
                break;
            }
            let mut key = doc_prefix(coll);
            key.extend(id_enc);
            let Some(v) = g.get_live(&key).await? else { continue };
            let victim = Document::from_reader(crate::natural::bson_part(&v))?;
            w.delete_quiet(&mut g, db, coll, &[], &victim).await?;
            n -= 1;
            bytes -= len;
        }
        ops.extend(w.final_ops()?);
        ops.push(Op::Put(crate::collopts::meta_key(coll), new.to_vec()?));
        ops.push(Op::Put(crate::capped::stat_key(coll), doc! { "docs": n, "bytes": bytes }.to_vec()?));
        g.apply(ops).await?;
        g.forget_coll_opts(coll);
        Ok(ok())
    }

    /// `cloneCollectionAsCapped`: a new capped collection with the newest documents of the source that fit.
    pub(crate) async fn clone_as_capped(&self, db: &str, cmd: &Document) -> R<Document> {
        let from = collection_name(cmd, "cloneCollectionAsCapped")?;
        let to = match cmd.get("toCollection") {
            Some(Bson::String(s)) => s.clone(),
            _ => return Err(MongoError::type_mismatch("'toCollection' must be of type String".into())),
        };
        let size = match cmd.get("size") {
            None => 0,
            Some(v) => number("cloneCollectionAsCapped", "size", v)?,
        };
        if size <= 0 {
            return Err(MongoError::new(72, "InvalidOptions", "invalid command spec".into()));
        }
        let mut g = self.dbs.lock(db).await;
        let mut names = g.collections().await?;
        if !names.iter().any(|n| n == from) {
            return Err(MongoError::namespace_not_found(format!("source collection {db}.{from} does not exist")));
        }
        if names.iter().any(|n| *n == to) {
            return Err(MongoError::new(48, "NamespaceExists", format!("cloneCollectionAsCapped failed - destination collection {db}.{to} already exists. source collection: {db}.{from}")));
        }
        let mut raw = doc! { "capped": true, "size": crate::capped::typed(cmd.get("size").unwrap_or(&Bson::Int64(0)), size) };
        if let Some(c) = g.coll_opts(from).await?.and_then(|o| o.raw.get("collation").cloned()) {
            raw.insert("collation", c);
        }
        names.push(to.clone());
        g.set_coll_opts(&to, Some(crate::collopts::CollOpts::parse(raw.clone())));
        let docs = natural_docs(&mut g, from).await?;
        let mut w = Writes::new();
        let ns = format!("{db}.{to}");
        let dp = doc_prefix(from);
        for (_, id_enc, _) in &docs {
            let mut key = dp.clone();
            key.extend(id_enc);
            let Some(v) = g.get_live(&key).await? else { continue };
            let d = Document::from_reader(crate::natural::bson_part(&v))?;
            w.put(&mut g, &to, &ns, &mut [], &d, None).await?;
        }
        crate::capped::enforce(&mut g, db, &mut w, &[], &to).await?;
        let mut ops = vec![Entry::collections_op(&names)?, Op::Put(crate::collopts::meta_key(&to), raw.to_vec()?)];
        ops.extend(w.final_ops()?);
        g.apply(ops).await?;
        Ok(ok())
    }
}
