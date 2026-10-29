//! Write concern: the `writeConcern` option of the write commands. The engine is a single data-bearing member (or a standalone),
//! so every `w` up to `majority` is satisfied at once; a larger `w`, or a tag that the replica set configuration does not define,
//! is reported as mongod reports it: an error before the write on a standalone, a `writeConcernError` after the write in a
//! replica set.

use crate::cmd::{Server, R};
use crate::error::MongoError;
use bson::{doc, Bson, Document};

const PROVENANCE: [&str; 5] = ["clientSupplied", "customDefault", "getLastErrorDefaults", "implicitDefault", "internalWriteDefault"];
/// Commands that answer `Command does not support writeConcern` when they get one.
const READ_ONLY: [&str; 9] = ["find", "count", "distinct", "listCollections", "listIndexes", "collStats", "dbStats", "listDatabases", "validate"];

#[derive(Clone, Debug)]
enum W {
    None,
    Num(i64),
    Majority,
    Tag(String),
    Tags(Document),
}

#[derive(Clone, Debug)]
pub(crate) struct WriteConcern {
    w: W,
    /// The write concern as echoed in `errInfo`.
    echo: Document,
}

fn wrong(field: &str, v: &Bson, types: &str) -> MongoError {
    MongoError::type_mismatch(format!("BSON field 'WriteConcernOptions.{field}' is the wrong type '{}', expected types '[{types}']", crate::update::type_name(v)))
}

fn number(v: &Bson) -> Option<f64> {
    match v {
        Bson::Int32(i) => Some(*i as f64),
        Bson::Int64(i) => Some(*i as f64),
        Bson::Double(f) => Some(*f),
        Bson::Decimal128(d) => d.to_string().parse::<f64>().ok(),
        _ => None,
    }
}

fn truthy(v: &Bson) -> bool {
    match v {
        Bson::Boolean(b) => *b,
        other => number(other).map(|n| n != 0.0).unwrap_or(false),
    }
}

impl WriteConcern {
    fn parse(wc: &Document, provenance: &str) -> R<WriteConcern> {
        let (mut w, mut w_echo) = (W::None, None);
        let (mut j, mut fsync, mut wtimeout) = (None, None, None);
        let mut prov = provenance.to_string();
        for (k, v) in wc {
            match k.as_str() {
                "w" => match v {
                    Bson::Null => {
                        w = W::Tag(String::new());
                        w_echo = Some(Bson::String(String::new()));
                    }
                    Bson::String(s) if s == "majority" => {
                        w = W::Majority;
                        w_echo = Some(v.clone());
                    }
                    Bson::String(s) => {
                        w = W::Tag(s.clone());
                        w_echo = Some(v.clone());
                    }
                    Bson::Document(d) if d.is_empty() => return Err(MongoError::failed_to_parse("tagged write concern requires tags".into())),
                    Bson::Document(d) => {
                        w = W::Tags(d.clone());
                        // (the tag counts are echoed as 64-bit integers)
                        w_echo = Some(Bson::Document(d.iter().map(|(k, t)| (k.clone(), number(t).map(|n| Bson::Int64(n as i64)).unwrap_or_else(|| t.clone()))).collect()));
                    }
                    other => match number(other) {
                        Some(n) if (0.0..=50.0).contains(&n) => {
                            w = W::Num(n as i64);
                            w_echo = Some(match other {
                                Bson::Int64(_) => Bson::Int64(n as i64),
                                _ => Bson::Int32(n as i32),
                            });
                        }
                        Some(n) => {
                            let shown = if n.fract() == 0.0 { format!("{}", n as i64) } else { format!("{n}") };
                            return Err(MongoError::failed_to_parse(format!("w has to be a non-negative number and not greater than 50; found: {shown}")));
                        }
                        None => return Err(MongoError::failed_to_parse(format!("w has to be a number, string, or object; found: {}", crate::update::type_name(other)))),
                    },
                },
                "j" => {
                    if !matches!(v, Bson::Boolean(_)) && number(v).is_none() {
                        return Err(wrong("j", v, "bool, long, int, decimal, double"));
                    }
                    j = Some(truthy(v));
                }
                "fsync" => {
                    if !matches!(v, Bson::Boolean(_)) && number(v).is_none() {
                        return Err(wrong("fsync", v, "bool, long, int, decimal, double"));
                    }
                    fsync = Some(truthy(v));
                }
                "wtimeout" => {
                    // (mongod reads anything that is not a number as 0)
                    wtimeout = Some(number(v).map(|n| n as i64).unwrap_or(0));
                }
                "provenance" => match v {
                    Bson::String(s) if PROVENANCE.contains(&s.as_str()) => prov = s.clone(),
                    other => {
                        let shown = match other {
                            Bson::String(s) => s.clone(),
                            o => o.to_string(),
                        };
                        return Err(MongoError::bad_value(format!("Enumeration value '{shown}' for field 'WriteConcernOptions.provenance' is not a valid value.")));
                    }
                },
                other => return Err(MongoError::new(40415, "IDLUnknownField", format!("BSON field 'WriteConcernOptions.{other}' is an unknown field."))),
            }
        }
        if j == Some(true) && fsync == Some(true) {
            return Err(MongoError::failed_to_parse("fsync and j options cannot be used together".into()));
        }
        let mut echo = Document::new();
        if let Some(e) = w_echo {
            echo.insert("w", e);
        }
        if let Some(j) = j {
            echo.insert("j", j);
        }
        if let Some(f) = fsync {
            echo.insert("fsync", f);
        }
        let t = wtimeout.unwrap_or(0);
        echo.insert("wtimeout", if t >= i32::MIN as i64 && t <= i32::MAX as i64 { Bson::Int32(t as i32) } else { Bson::Int64(t) });
        echo.insert("provenance", prov);
        Ok(WriteConcern { w, echo })
    }

    /// The error of a write concern that this node cannot satisfy: (code, name, message).
    fn unsatisfied(&self) -> Option<(i32, &'static str, String)> {
        match &self.w {
            W::Num(n) if *n > 1 => Some((100, "UnsatisfiableWriteConcern", "Not enough data-bearing nodes".into())),
            W::Tag(s) => Some((79, "UnknownReplWriteConcern", format!("No write concern mode named '{s}' found in replica set configuration"))),
            W::Tags(d) => Some((4, "NoSuchKey", format!("No replica set tag key {} in config", d.keys().next().cloned().unwrap_or_default()))),
            _ => None,
        }
    }

    /// The client accepts an acknowledgement before the write is durable: `j: false` with `w` 1 (or no `w`), or `w: 0`
    /// without `j: true`. Only databases with relaxed durability switched on use it (`ctl/durability.rs`).
    pub(crate) fn relaxed(&self) -> bool {
        let j = self.echo.get_bool("j").ok();
        match self.w {
            W::Num(0) => j != Some(true),
            W::Num(1) | W::None => j == Some(false),
            _ => false,
        }
    }

    fn unacknowledged(&self) -> bool {
        matches!(self.w, W::Num(0)) && self.echo.get_bool("j").ok() != Some(true)
    }
}

impl Server {
    /// The write concern a command runs with, checked before it runs. `Ok(None)`: none to apply.
    pub(crate) fn command_write_concern(&self, name: &str, cmd: &Document) -> R<Option<WriteConcern>> {
        let in_txn = cmd.contains_key("autocommit") && !matches!(name, "commitTransaction" | "abortTransaction");
        match cmd.get("writeConcern") {
            Some(Bson::Document(wc)) => {
                if READ_ONLY.contains(&name) {
                    return Err(MongoError::new(72, "InvalidOptions", "Command does not support writeConcern".into()));
                }
                if in_txn {
                    return Ok(None);
                }
                let wc = WriteConcern::parse(wc, "clientSupplied")?;
                self.check_standalone(&wc)?;
                Ok(Some(wc))
            }
            Some(other) => {
                if READ_ONLY.contains(&name) {
                    return Err(MongoError::new(72, "InvalidOptions", "Command does not support writeConcern".into()));
                }
                Err(MongoError::type_mismatch(format!("\"writeConcern\" had the wrong type. Expected object, found {}", crate::update::type_name(other))))
            }
            None => {
                if in_txn || !self.is_rs() || !is_write(name) {
                    return Ok(None);
                }
                // the cluster-wide default (setDefaultRWConcern) applies to a write that names none
                let def = self.default_write_concern();
                match def {
                    Some(d) => Ok(Some(WriteConcern::parse(&d, "customDefault")?)),
                    None => Ok(None),
                }
            }
        }
    }

    fn check_standalone(&self, wc: &WriteConcern) -> R<()> {
        if self.is_rs() {
            return Ok(());
        }
        match &wc.w {
            W::Num(n) if *n > 1 => Err(MongoError::bad_value("cannot use 'w' > 1 when a host is not replicated".into())),
            W::Tag(s) => Err(MongoError::bad_value(format!("cannot use non-majority 'w' mode \"{s}\" when a host is not a member of a replica set"))),
            _ => Ok(()),
        }
    }

    /// Add the `writeConcernError` (or apply `w: 0`) to the reply of a command that ran.
    pub(crate) fn finish_write_concern(&self, name: &str, cmd: &Document, wc: &WriteConcern, reply: &mut Document) -> R<()> {
        if wc.unacknowledged() {
            if matches!(name, "insert" | "update" | "delete") {
                let is_update = name == "update";
                reply.remove("writeErrors");
                reply.remove("upserted");
                reply.remove("retriedStmtIds");
                reply.insert("n", 0);
                if is_update {
                    reply.insert("nModified", 0);
                    // (n, nModified, ok)
                    let ok = reply.remove("ok");
                    if let Some(ok) = ok {
                        reply.insert("ok", ok);
                    }
                }
            }
            return Ok(());
        }
        if !self.is_rs() {
            return Ok(());
        }
        let Some((code, code_name, msg)) = wc.unsatisfied() else { return Ok(()) };
        if name == "dropDatabase" {
            let n = reply.remove("\u{0}ncolls").and_then(|b| b.as_i32()).unwrap_or(0);
            let ts = self.dbs.changes.cluster_now();
            return Err(MongoError::new(code, code_name, format!("dropDatabase {} failed waiting for {n} collection drop(s) (most recent drop optime: {{ ts: Timestamp({}, {}), t: 1 }}) to replicate. :: caused by :: {msg}", cmd.get_str("$db").unwrap_or(""), ts.time, ts.increment)));
        }
        let err = doc! { "code": code, "codeName": code_name, "errmsg": msg, "errInfo": { "writeConcern": wc.echo.clone() } };
        let ok = reply.remove("ok");
        reply.insert("writeConcernError", err);
        if let Some(ok) = ok {
            reply.insert("ok", ok);
        }
        Ok(())
    }
}

/// Commands that write (the default write concern applies to them).
fn is_write(name: &str) -> bool {
    matches!(
        name,
        "insert" | "update" | "delete" | "findAndModify" | "findandmodify" | "create" | "drop" | "dropDatabase" | "createIndexes" | "dropIndexes" | "collMod" | "renameCollection" | "convertToCapped" | "cloneCollectionAsCapped"
    )
}
