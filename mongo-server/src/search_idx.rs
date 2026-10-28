//! Atlas Search index management: `createSearchIndexes`, `updateSearchIndex`, `dropSearchIndex` and the `$listSearchIndexes` stage.
//!
//! mongod 7.0 Community answers all of them with "no such command" / "only allowed on MongoDB Atlas", so there is no reference server:
//! the behaviour follows the documented Atlas commands and the drivers' index-management specification. The index definitions are
//! stored per collection (`meta/searchidx/<collection>`); an index is `READY` and `queryable` as soon as it is created (nothing is built
//! in the background), and `$search` does not need an index definition to run (see `search.rs`).

use crate::cmd::{ok, Server, R};
use crate::dbs::Entry;
use crate::error::MongoError;
use bson::{doc, Bson, DateTime, Document};
use tenant_bench::lifecycle::Op;

fn key(coll: &str) -> Vec<u8> {
    let mut k = b"meta/searchidx/".to_vec();
    k.extend(crate::collopts::meta_key(coll).split_off(b"meta/copt/".len()));
    k
}

fn type_err(field: &str, v: &Bson, expected: &str) -> MongoError {
    MongoError::type_mismatch(format!("BSON field '{field}' is the wrong type '{}', expected {expected}", crate::update::type_name(v)))
}

fn missing(field: &str) -> MongoError {
    MongoError::new(40414, "IDLFailedToParse", format!("BSON field '{field}' is missing but a required field"))
}

fn new_id() -> String {
    let b: [u8; 12] = rand::random();
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// `{name}` / `{id}` selector of update, drop and list.
fn selector(cmd: &Document, cmdname: &str, required: bool) -> R<(Option<String>, Option<String>)> {
    let get = |f: &str| -> R<Option<String>> {
        match cmd.get(f) {
            None => Ok(None),
            Some(Bson::String(s)) => Ok(Some(s.clone())),
            Some(v) => Err(type_err(&format!("{cmdname}.{f}"), v, "type 'string'")),
        }
    };
    let (name, id) = (get("name")?, get("id")?);
    if name.is_some() && id.is_some() {
        return Err(MongoError::new(2, "BadValue", "Cannot set both name and id.".into()));
    }
    if required && name.is_none() && id.is_none() {
        return Err(MongoError::new(2, "BadValue", "Must set either name or id.".into()));
    }
    Ok((name, id))
}

impl Server {
    async fn load_search_indexes(&self, g: &mut Entry, coll: &str) -> R<Vec<Document>> {
        Ok(match g.get(&key(coll)).await? {
            Some(b) => Document::from_reader(&b[..]).map_err(|e| MongoError::internal(e.to_string()))?.get_array("indexes").map(|a| a.iter().filter_map(|x| x.as_document().cloned()).collect()).unwrap_or_default(),
            None => Vec::new(),
        })
    }

    async fn store_search_indexes(&self, g: &mut Entry, coll: &str, list: &[Document]) -> R<()> {
        let op = if list.is_empty() { Op::Delete(key(coll)) } else { Op::Put(key(coll), doc! { "indexes": list.iter().cloned().map(Bson::Document).collect::<Vec<_>>() }.to_vec()?) };
        g.apply(vec![op]).await?;
        Ok(())
    }

    /// The collection must exist, like `dropIndexes`.
    async fn search_target(&self, g: &mut Entry, db: &str, coll: &str) -> R<()> {
        if !g.collections().await?.iter().any(|n| n == coll) {
            return Err(MongoError::namespace_not_found(format!("Collection '{db}.{coll}' does not exist")));
        }
        Ok(())
    }

    pub(crate) async fn create_search_indexes(&self, db: &str, cmd: &Document) -> R<Document> {
        crate::fields::check("createSearchIndexes", cmd)?;
        let coll = match cmd.get("createSearchIndexes") {
            Some(Bson::String(s)) => s.clone(),
            Some(v) => return Err(type_err("createSearchIndexes.createSearchIndexes", v, "type 'string'")),
            None => return Err(missing("createSearchIndexes.createSearchIndexes")),
        };
        let specs = match cmd.get("indexes") {
            Some(Bson::Array(a)) => a,
            Some(v) => return Err(type_err("createSearchIndexes.indexes", v, "type 'array'")),
            None => return Err(missing("createSearchIndexes.indexes")),
        };
        let mut new: Vec<Document> = Vec::new();
        for (i, s) in specs.iter().enumerate() {
            let Bson::Document(s) = s else { return Err(type_err(&format!("createSearchIndexes.indexes.{i}"), s, "type 'object'")) };
            for k in s.keys() {
                if !matches!(k.as_str(), "name" | "definition" | "type") {
                    return Err(MongoError::new(40415, "IDLUnknownField", format!("BSON field 'createSearchIndexes.indexes.{i}.{k}' is an unknown field.")));
                }
            }
            let name = match s.get("name") {
                None => "default".to_string(),
                Some(Bson::String(n)) => n.clone(),
                Some(v) => return Err(type_err(&format!("createSearchIndexes.indexes.{i}.name"), v, "type 'string'")),
            };
            let definition = match s.get("definition") {
                Some(Bson::Document(d)) => d.clone(),
                Some(v) => return Err(type_err(&format!("createSearchIndexes.indexes.{i}.definition"), v, "type 'object'")),
                None => return Err(missing(&format!("createSearchIndexes.indexes.{i}.definition"))),
            };
            let ty = match s.get("type") {
                None => "search".to_string(),
                Some(Bson::String(t)) if t == "search" || t == "vectorSearch" => t.clone(),
                Some(Bson::String(t)) => return Err(MongoError::new(2, "BadValue", format!("Invalid search index type '{t}'; expected 'search' or 'vectorSearch'"))),
                Some(v) => return Err(type_err(&format!("createSearchIndexes.indexes.{i}.type"), v, "type 'string'")),
            };
            if new.iter().any(|d| d.get_str("name").ok() == Some(name.as_str())) {
                return Err(MongoError::new(68, "IndexAlreadyExists", format!("Duplicate Index: {name}")));
            }
            new.push(doc! { "id": new_id(), "name": name, "type": ty, "definition": definition, "version": 1_i32, "createdAt": DateTime::now() });
        }
        let mut g = self.dbs.lock(db).await;
        self.search_target(&mut g, db, &coll).await?;
        let mut list = self.load_search_indexes(&mut g, &coll).await?;
        for n in &new {
            if list.iter().any(|d| d.get_str("name").ok() == n.get_str("name").ok()) {
                return Err(MongoError::new(68, "IndexAlreadyExists", format!("Duplicate Index: {}", n.get_str("name").unwrap_or(""))));
            }
        }
        let created: Vec<Bson> = new.iter().map(|n| Bson::Document(doc! { "id": n.get_str("id").unwrap_or(""), "name": n.get_str("name").unwrap_or("") })).collect();
        list.extend(new);
        self.store_search_indexes(&mut g, &coll, &list).await?;
        Ok(doc! { "indexesCreated": created, "ok": 1.0 })
    }

    pub(crate) async fn drop_search_index(&self, db: &str, cmd: &Document) -> R<Document> {
        crate::fields::check("dropSearchIndex", cmd)?;
        let coll = match cmd.get("dropSearchIndex") {
            Some(Bson::String(s)) => s.clone(),
            Some(v) => return Err(type_err("dropSearchIndex.dropSearchIndex", v, "type 'string'")),
            None => return Err(missing("dropSearchIndex.dropSearchIndex")),
        };
        let (name, id) = selector(cmd, "dropSearchIndex", true)?;
        let mut g = self.dbs.lock(db).await;
        self.search_target(&mut g, db, &coll).await?;
        let mut list = self.load_search_indexes(&mut g, &coll).await?;
        let Some(pos) = list.iter().position(|d| name.as_deref().map(|n| d.get_str("name").ok() == Some(n)).unwrap_or(false) || id.as_deref().map(|i| d.get_str("id").ok() == Some(i)).unwrap_or(false)) else {
            return Err(MongoError::new(27, "IndexNotFound", format!("Index {} not found.", name.or(id).unwrap_or_default())));
        };
        list.remove(pos);
        self.store_search_indexes(&mut g, &coll, &list).await?;
        Ok(ok())
    }

    pub(crate) async fn update_search_index(&self, db: &str, cmd: &Document) -> R<Document> {
        crate::fields::check("updateSearchIndex", cmd)?;
        let coll = match cmd.get("updateSearchIndex") {
            Some(Bson::String(s)) => s.clone(),
            Some(v) => return Err(type_err("updateSearchIndex.updateSearchIndex", v, "type 'string'")),
            None => return Err(missing("updateSearchIndex.updateSearchIndex")),
        };
        let definition = match cmd.get("definition") {
            Some(Bson::Document(d)) => d.clone(),
            Some(v) => return Err(type_err("updateSearchIndex.definition", v, "type 'object'")),
            None => return Err(missing("updateSearchIndex.definition")),
        };
        let (name, id) = selector(cmd, "updateSearchIndex", true)?;
        let mut g = self.dbs.lock(db).await;
        self.search_target(&mut g, db, &coll).await?;
        let mut list = self.load_search_indexes(&mut g, &coll).await?;
        let Some(pos) = list.iter().position(|d| name.as_deref().map(|n| d.get_str("name").ok() == Some(n)).unwrap_or(false) || id.as_deref().map(|i| d.get_str("id").ok() == Some(i)).unwrap_or(false)) else {
            return Err(MongoError::new(27, "IndexNotFound", format!("Index {} not found.", name.or(id).unwrap_or_default())));
        };
        let v = list[pos].get_i32("version").unwrap_or(1) + 1;
        list[pos].insert("definition", definition);
        list[pos].insert("version", v);
        list[pos].insert("updatedAt", DateTime::now());
        self.store_search_indexes(&mut g, &coll, &list).await?;
        Ok(ok())
    }

    /// `$listSearchIndexes: {}` | `{name}` | `{id}`: the documents the stage yields.
    pub(crate) async fn list_search_indexes_stage(&self, g: &mut Entry, coll: &str, spec: &Bson) -> R<Vec<Document>> {
        let Bson::Document(spec) = spec else {
            return Err(MongoError::new(40324, "Location40324", format!("$listSearchIndexes must be an object, found: {}", crate::update::type_name(spec))));
        };
        for k in spec.keys() {
            if k != "name" && k != "id" {
                return Err(MongoError::new(40415, "IDLUnknownField", format!("BSON field '$listSearchIndexes.{k}' is an unknown field.")));
            }
        }
        let (name, id) = selector(spec, "$listSearchIndexes", false)?;
        if !g.collections().await?.iter().any(|n| n == coll) {
            return Ok(Vec::new());
        }
        let list = self.load_search_indexes(g, coll).await?;
        Ok(list
            .into_iter()
            .filter(|d| name.as_deref().map(|n| d.get_str("name").ok() == Some(n)).unwrap_or(true) && id.as_deref().map(|i| d.get_str("id").ok() == Some(i)).unwrap_or(true))
            .map(|d| {
                let def = d.get_document("definition").cloned().unwrap_or_default();
                let version = d.get_i32("version").unwrap_or(1);
                let created = d.get_datetime("createdAt").copied().unwrap_or_else(|_| DateTime::now());
                doc! {
                    "id": d.get_str("id").unwrap_or(""), "name": d.get_str("name").unwrap_or(""), "type": d.get_str("type").unwrap_or("search"),
                    "status": "READY", "queryable": true,
                    "latestDefinitionVersion": { "version": version, "createdAt": created },
                    "latestDefinition": def.clone(),
                    "statusDetail": [{ "hostname": "localhost", "status": "READY", "queryable": true, "mainIndex": { "status": "READY", "queryable": true, "definition": def, "definitionVersion": { "version": version, "createdAt": created } } }],
                }
            })
            .collect())
    }

    /// Dropping a collection drops its search index definitions.
    pub(crate) fn search_idx_delete_op(coll: &str) -> Op {
        Op::Delete(key(coll))
    }
}
