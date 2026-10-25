use bson::{doc, Document};

/// A MongoDB command error: `{ok: 0, errmsg, code, codeName}`.
#[derive(Debug, Clone)]
pub struct MongoError {
    pub code: i32,
    pub name: &'static str,
    pub msg: String,
    /// Additional fields of the error reply (e.g. `keyPattern` / `keyValue` of a duplicate key).
    pub extra: Option<Document>,
}

impl MongoError {
    pub fn new(code: i32, name: &'static str, msg: String) -> Self {
        Self { code, name, msg, extra: None }
    }
    pub fn bad_value(msg: String) -> Self {
        Self::new(2, "BadValue", msg)
    }
    pub fn type_mismatch(msg: String) -> Self {
        Self::new(14, "TypeMismatch", msg)
    }
    pub fn failed_to_parse(msg: String) -> Self {
        Self::new(9, "FailedToParse", msg)
    }
    pub fn namespace_not_found(msg: String) -> Self {
        Self::new(26, "NamespaceNotFound", msg)
    }
    pub fn namespace_exists(msg: String) -> Self {
        Self::new(48, "NamespaceExists", msg)
    }
    pub fn internal(msg: String) -> Self {
        Self::new(1, "InternalError", msg)
    }
    pub fn command_not_found(name: &str) -> Self {
        Self::new(59, "CommandNotFound", format!("no such command: '{name}'"))
    }
    pub fn cursor_not_found(id: i64) -> Self {
        Self::new(43, "CursorNotFound", format!("cursor id {id} not found"))
    }

    /// E11000 duplicate key error for `index` on `ns`; `value` is the offending `{field: value}` pair(s).
    pub fn duplicate_key(ns: &str, index: &str, pattern: &Document, value: &Document) -> Self {
        let mut e = Self::new(
            11000,
            "DuplicateKey",
            format!("E11000 duplicate key error collection: {ns} index: {index} dup key: {}", crate::update::fmt_doc(value)),
        );
        e.extra = Some(doc! { "keyPattern": pattern.clone(), "keyValue": value.clone() });
        e
    }

    pub fn to_reply(&self) -> Document {
        let mut d = doc! { "ok": 0.0, "errmsg": self.msg.clone(), "code": self.code, "codeName": self.name };
        if let Some(x) = &self.extra {
            for (k, v) in x {
                d.insert(k.clone(), v.clone());
            }
        }
        d
    }

    /// The same error as an entry of a `writeErrors` array.
    pub fn to_write_error(&self, index: usize) -> Document {
        let mut d = doc! { "index": index as i32, "code": self.code, "errmsg": self.msg.clone() };
        if let Some(x) = &self.extra {
            for (k, v) in x {
                d.insert(k.clone(), v.clone());
            }
        }
        d
    }
}

impl std::fmt::Display for MongoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({}): {}", self.name, self.code, self.msg)
    }
}

impl std::error::Error for MongoError {}

impl From<anyhow::Error> for MongoError {
    fn from(e: anyhow::Error) -> Self {
        MongoError::internal(format!("{e:#}"))
    }
}

impl From<bson::error::Error> for MongoError {
    fn from(e: bson::error::Error) -> Self {
        MongoError::internal(format!("bson: {e}"))
    }
}
