use bson::{doc, Bson, Document};

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
    pub fn write_conflict() -> Self {
        let mut e = Self::new(112, "WriteConflict", "Caused by :: Write conflict during plan execution and yielding is disabled. :: Please retry your operation or multi-document transaction.".into());
        e.extra = Some(doc! { "errorLabels": ["TransientTransactionError"] });
        e
    }
    pub fn cursor_not_found(id: i64) -> Self {
        Self::new(43, "CursorNotFound", format!("cursor id {id} not found"))
    }

    /// E11000 duplicate key error for `index` on `ns`; `value` is the offending `{field: value}` pair(s).
    pub fn duplicate_key(ns: &str, index: &str, pattern: &Document, value: &Document) -> Self {
        Self::duplicate_key_c(ns, index, pattern, value, None)
    }

    /// E11000 for an index with a collation: the strings of the key are shown (and reported in `keyValue`) as their hex
    /// collation keys, and the collation is part of the message and of the error document.
    pub fn duplicate_key_c(ns: &str, index: &str, pattern: &Document, value: &Document, c: Option<&crate::collation::Collation>) -> Self {
        let Some(c) = c else {
            let mut e = Self::new(11000, "DuplicateKey", format!("E11000 duplicate key error collection: {ns} index: {index} dup key: {}", crate::update::fmt_doc(value)));
            e.extra = Some(doc! { "keyPattern": pattern.clone(), "keyValue": value.clone() });
            return e;
        };
        let mut shown = Document::new();
        let mut key_value = Document::new();
        let mut hex: Vec<Bson> = Vec::new();
        for (k, v) in value {
            match v {
                Bson::String(s) => {
                    let mut kb = Vec::new();
                    c.sort_key(s, &mut kb);
                    let h: String = kb.iter().map(|b| format!("{b:02x}")).collect();
                    shown.insert(k.clone(), format!("CollationKey(0x{h})"));
                    key_value.insert(k.clone(), h);
                    hex.push(Bson::Boolean(true));
                }
                other => {
                    shown.insert(k.clone(), other.clone());
                    key_value.insert(k.clone(), other.clone());
                    hex.push(Bson::Boolean(false));
                }
            }
        }
        let cd = c.to_doc();
        let mut e = Self::new(11000, "DuplicateKey", format!("E11000 duplicate key error collection: {ns} index: {index} collation: {} dup key: {}", crate::update::fmt_doc(&cd), crate::update::fmt_doc(&shown)));
        e.extra = Some(doc! { "keyPattern": pattern.clone(), "keyValue": key_value, "hexEncoded": hex, "collation": cd });
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
        if let Some(h) = e.downcast_ref::<crate::lease::LeaseHeld>() {
            // 134 is on the drivers' retryable-read list and is not a state-change code, so it does not
            // make them mark the server unknown and clear their connection pools.
            let mut m = MongoError::new(134, "TenantLeaseHeld", h.to_string());
            m.extra = Some(doc! { "errorLabels": ["RetryableWriteError"] });
            return m;
        }
        if let Some(r) = e.downcast_ref::<crate::dbs::Reopened>() {
            // nothing was applied: retryable like a lease held elsewhere (inside a transaction too)
            let mut m = MongoError::new(134, "DatabaseReopened", r.to_string());
            m.extra = Some(doc! { "errorLabels": ["TransientTransactionError", "RetryableWriteError"] });
            return m;
        }
        if let Some(r) = e.downcast_ref::<crate::locallog::RecoveryPending>() {
            // same code as a lease held elsewhere: retryable, and not a state change for the drivers
            let mut m = MongoError::new(134, "DatabaseRecoveryPending", r.to_string());
            m.extra = Some(doc! { "errorLabels": ["RetryableWriteError"] });
            return m;
        }
        if tenant_bench::fx::is_fenced(&e) {
            // fix-store-errors (test_65 C2): this process's writer was fenced by a newer owner of the database ("detected
            // newer DB client"). The write's WAL object was refused, so nothing of it was applied: retryable, like a
            // database reopened after a lease loss (inside a transaction too). It was code 1, which drivers do not retry.
            let mut m = MongoError::new(134, "DatabaseFenced", format!("database written by a newer owner; nothing of this write was applied, retry: {e:#}"));
            m.extra = Some(doc! { "errorLabels": ["TransientTransactionError", "RetryableWriteError"] });
            return m;
        }
        if let Some(r) = e.downcast_ref::<tenant_bench::fx::Retry>() {
            // a revoked hold or a stale version of our manifest (fx/hold.rs, rule 5 and 6): retryable, and inside a
            // transaction the driver runs the whole transaction again
            let mut m = MongoError::new(134, "SealedRunsRetry", r.to_string());
            m.extra = Some(doc! { "errorLabels": ["TransientTransactionError", "RetryableWriteError"] });
            return m;
        }
        if let Some(db) = tenant_bench::lifecycle::note_missing_sst(&e) {
            // a reader that kept a manifest whose SSTs a compaction replaced and a collector deleted (sst-not-found): the
            // next command opens a new reader on the latest manifest, so the read is retried, not failed
            let mut m = MongoError::new(134, "StaleReader", format!("database {db}: the reader's view of the database was out of date (an SST it named was compacted and removed); retry: {e:#}"));
            m.extra = Some(doc! { "errorLabels": ["TransientTransactionError", "RetryableWriteError"] });
            return m;
        }
        MongoError::internal(format!("{e:#}"))
    }
}

impl From<bson::error::Error> for MongoError {
    fn from(e: bson::error::Error) -> Self {
        MongoError::internal(format!("bson: {e}"))
    }
}

/// P-fix-slow-store item 5: a write whose batch was staged and whose durability wait failed. Its outcome is unknown (it
/// may be durable already, or become so by the WAL flush of an abandon), so the answer is never a definite error: code
/// 134, as the other "retry" answers of this server (not a state-change code, so drivers keep their pools), with the
/// `RetryableWriteError` label as mongod gives an interrupted write, and for a commit `UnknownTransactionCommitResult`.
/// A retryable write's retry is deduplicated by its session record (`retry.rs`; after a lost opening it is read back from
/// the database's next opening). Errors that already say what happened keep their code and labels.
pub fn outcome_unknown(name: &str, e: MongoError) -> MongoError {
    if e.extra.as_ref().is_some_and(|x| x.contains_key("errorLabels")) || control_definite() {
        return e;
    }
    OUTCOMES_UNKNOWN.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let mut m = MongoError::new(134, "WriteOutcomeUnknown", format!("the write was staged, then its durability could not be confirmed ({}); it may or may not have been applied: retry it (a retryable write is applied once)", e.msg));
    m.extra = Some(if matches!(name, "commitTransaction") {
        doc! { "errorLabels": ["UnknownTransactionCommitResult", "RetryableWriteError"] }
    } else {
        doc! { "errorLabels": ["RetryableWriteError"] }
    });
    m
}

/// Writes answered with `outcome_unknown`.
pub static OUTCOMES_UNKNOWN: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// TEST ONLY (negative control of ctl test_90 item 5, `FENVIX_TEST_CONTROL_OUTCOME_DEFINITE=1`): the code before the fix
/// (a failed durability wait answered as it came, a retry not reading the record back).
pub fn control_definite() -> bool {
    static C: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *C.get_or_init(|| std::env::var("FENVIX_TEST_CONTROL_OUTCOME_DEFINITE").as_deref() == Ok("1"))
}
