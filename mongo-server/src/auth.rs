//! Authentication (SCRAM-SHA-1 / SCRAM-SHA-256 over saslStart / saslContinue), users and roles (stored like mongod does,
//! in `admin.system.users` and `admin.system.roles`), and the authorization check of every command.

use crate::authz::{self, Need, Priv, Res};
use crate::cmd::{bool_arg, ok, R, Server};
use crate::error::MongoError;
use crate::query::matches;
use crate::scram::{self, Mech};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use bson::{doc, spec::BinarySubtype, Binary, Bson, Document};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::Mutex;

const USERS: &str = "system.users";
const ROLES: &str = "system.roles";

#[derive(Default)]
pub struct AuthState {
    /// `--auth`: commands need authorization.
    pub enabled: AtomicBool,
    /// Bumped by every user or role change; connections re-resolve their privileges when it moved.
    gen: AtomicU64,
    conns: Mutex<HashMap<i64, Conn>>,
}

#[derive(Default)]
struct Conn {
    local: bool,
    peer: Option<std::net::IpAddr>,
    server: Option<std::net::IpAddr>,
    /// RFC 2253 subject of the TLS client certificate.
    x509: Option<String>,
    /// (user, db) authenticated on this connection.
    users: Vec<(String, String)>,
    conv: Option<ConvState>,
    next_conv: i32,
    privs: Option<(u64, Vec<Priv>)>,
}

struct ConvState {
    id: i32,
    db: String,
    conv: scram::Conv,
    skip_empty: bool,
    stage: u8,
}

fn unauthorized(msg: String) -> MongoError {
    MongoError::new(13, "Unauthorized", msg)
}

/// mongod's refusal text. The command is shown with its secrets redacted (see `redact_command`).
fn not_authorized(db: &str, name: &str, cmd: &Document) -> MongoError {
    let redacted = redact_command(name, cmd);
    let mut shown: Document = redacted.iter().filter(|(k, _)| !matches!(k.as_str(), "lsid" | "$clusterTime" | "$readPreference")).map(|(k, v)| (k.clone(), v.clone())).collect();
    shown.insert("$db", db);
    unauthorized(format!("not authorized on {db} to execute command {}", crate::update::fmt_doc(&shown)))
}

fn auth_failed() -> MongoError {
    MongoError::new(18, "AuthenticationFailed", "Authentication failed.".into())
}


/// Commands that carry a secret in their own fields: a password, a SCRAM payload, a legacy authentication key.
const SECRET_COMMANDS: &[&str] = &["createUser", "updateUser", "authenticate", "saslStart", "saslContinue", "changeUserPassword"];
/// Fields of those commands whose value is a secret (or lets one be derived).
const SECRET_FIELDS: &[&str] = &["pwd", "password", "payload", "key", "nonce", "proof", "clientProof"];

/// A copy of a command that is safe to put in an error message, in the profile or in `currentOp`: the values of the secret
/// fields of the commands that carry secrets are replaced by "xxx" (the way mongod redacts `pwd` and `payload`). Every other
/// command is returned as it is, so the cost is one clone, as before.
pub(crate) fn redact_command(name: &str, cmd: &Document) -> Document {
    if !SECRET_COMMANDS.contains(&name) {
        return cmd.clone();
    }
    cmd.iter().map(|(k, v)| (k.clone(), if SECRET_FIELDS.contains(&k.as_str()) { Bson::String("xxx".into()) } else { v.clone() })).collect()
}

fn bin(b: Vec<u8>) -> Bson {
    Bson::Binary(Binary { subtype: BinarySubtype::Generic, bytes: b })
}

fn role_pair(b: &Bson, default_db: &str) -> R<(String, String)> {
    match b {
        Bson::String(s) => Ok((s.clone(), default_db.to_string())),
        Bson::Document(d) => {
            let r = d.get_str("role").map_err(|_| MongoError::new(2, "BadValue", "Missing 'role' field".into()))?;
            let db = d.get_str("db").map_err(|_| MongoError::new(2, "BadValue", "Missing 'db' field".into()))?;
            Ok((r.to_string(), db.to_string()))
        }
        _ => Err(MongoError::new(2, "BadValue", "Role names must be either strings or objects".into())),
    }
}

/// `10.0.0.0/8`, `::1`, `192.168.1.5`: does the CIDR (or single address) contain `ip`?
pub(crate) fn parse_cidr(c: &str) -> Option<(std::net::IpAddr, u32)> {
    let (a, bits) = match c.split_once('/') {
        Some((a, b)) => (a, Some(b.parse::<u32>().ok()?)),
        None => (c, None),
    };
    let ip: std::net::IpAddr = a.parse().ok()?;
    let max = if ip.is_ipv4() { 32 } else { 128 };
    let bits = bits.unwrap_or(max);
    (bits <= max).then_some((ip, bits))
}

fn cidr_contains(c: &str, ip: std::net::IpAddr) -> bool {
    use std::net::IpAddr::*;
    let Some((net, bits)) = parse_cidr(c) else { return false };
    let ip = match (net, ip) {
        (V4(_), V6(v6)) => match v6.to_ipv4_mapped() {
            Some(v4) => V4(v4),
            None => return false,
        },
        (_, ip) => ip,
    };
    let (a, b): (u128, u128) = match (net, ip) {
        (V4(a), V4(b)) => (u32::from(a) as u128, u32::from(b) as u128),
        (V6(a), V6(b)) => (u128::from(a), u128::from(b)),
        _ => return false,
    };
    let width = if net.is_ipv4() { 32 } else { 128 };
    if bits == 0 {
        return true;
    }
    let shift = width - bits;
    (a >> shift) == (b >> shift)
}

fn validate_restrictions(v: &Bson) -> R<()> {
    let Bson::Array(a) = v else { return Err(MongoError::new(14, "TypeMismatch", "BSON field 'authenticationRestrictions' is the wrong type, expected array".into())) };
    for r in a {
        let Some(d) = r.as_document() else { return Err(MongoError::new(14, "TypeMismatch", "authenticationRestrictions entries must be objects".into())) };
        for (k, val) in d {
            if k != "clientSource" && k != "serverAddress" {
                return Err(MongoError::new(40415, "IDLUnknownField", format!("BSON field 'authenticationRestrictions.{k}' is an unknown field.")));
            }
            for c in val.as_array().into_iter().flatten().filter_map(|x| x.as_str()) {
                if parse_cidr(c).is_none() {
                    return Err(MongoError::new(2, "BadValue", "Invalid IP address in CIDR string".into()));
                }
            }
        }
    }
    Ok(())
}

fn user_name_doc(d: &Document, db: &str) -> R<(String, String)> {
    if let Some(k) = d.keys().find(|k| !matches!(k.as_str(), "user" | "db")) {
        return Err(MongoError::new(2, "BadValue", format!("UserName contains an unknown field named: '{k}")));
    }
    Ok((d.get_str("user").unwrap_or("").to_string(), d.get_str("db").unwrap_or(db).to_string()))
}

fn role_doc(r: &(String, String)) -> Bson {
    Bson::Document(doc! { "role": r.0.clone(), "db": r.1.clone() })
}

impl Server {
    pub fn auth_connection_opened(&self, conn_id: i64, peer: Option<std::net::IpAddr>, server: Option<std::net::IpAddr>, x509: Option<String>) {
        let local = peer.map(|a| a.is_loopback()).unwrap_or(false);
        self.auth.conns.lock().unwrap().insert(conn_id, Conn { local, peer, server, x509, next_conv: 1, ..Default::default() });
    }

    pub fn auth_connection_closed(&self, conn_id: i64) {
        self.auth.conns.lock().unwrap().remove(&conn_id);
    }

    fn with_conn<T>(&self, conn_id: i64, f: impl FnOnce(&mut Conn) -> T) -> T {
        let mut m = self.auth.conns.lock().unwrap();
        let c = m.entry(conn_id).or_insert_with(|| Conn { next_conv: 1, ..Default::default() });
        f(c)
    }

    // ---- storage -------------------------------------------------------------------------------

    async fn auth_docs(&self, coll: &str, filter: &Document) -> R<Vec<Document>> {
        let mut g = self.dbs.read("admin").await?;
        self.matching(&mut g, "admin", coll, filter, None, None).await
    }

    pub(crate) async fn find_user(&self, db: &str, user: &str) -> R<Option<Document>> {
        Ok(self.auth_docs(USERS, &doc! { "_id": format!("{db}.{user}") }).await?.into_iter().next())
    }

    async fn insert_auth_doc(&self, coll: &str, d: Document) -> R<()> {
        let r = self.insert("admin", &doc! { "insert": coll, "documents": [d] }, &mut None).await?;
        if let Ok(errs) = r.get_array("writeErrors") {
            if let Some(e) = errs.first().and_then(|e| e.as_document()) {
                return Err(MongoError::new(e.get_i32("code").unwrap_or(1), "DuplicateKey", e.get_str("errmsg").unwrap_or("").to_string()));
            }
        }
        self.auth.gen.fetch_add(1, Relaxed);
        Ok(())
    }

    async fn replace_auth_doc(&self, coll: &str, d: Document) -> R<()> {
        let id = d.get("_id").cloned().unwrap_or(Bson::Null);
        self.update("admin", &doc! { "update": coll, "updates": [{ "q": { "_id": id }, "u": d }] }, &mut None).await?;
        self.auth.gen.fetch_add(1, Relaxed);
        Ok(())
    }

    async fn delete_auth_docs(&self, coll: &str, filter: Document) -> R<i64> {
        let r = self.delete("admin", &doc! { "delete": coll, "deletes": [{ "q": filter, "limit": 0 }] }, &mut None).await?;
        self.auth.gen.fetch_add(1, Relaxed);
        Ok(r.get_i32("n").unwrap_or(0) as i64)
    }

    /// Control layer (erase): remove the users and roles defined on database `db` (they live in `admin.system.users` and
    /// `admin.system.roles`, outside the database's own objects). Open connections lose their privileges at once: the
    /// generation counter moves, so every connection resolves its privileges again, and a user that is gone has none.
    pub(crate) async fn erase_database_auth(&self, db: &str) -> R<(i64, i64)> {
        let users = self.delete_auth_docs(USERS, doc! { "db": db }).await?;
        let roles = self.delete_auth_docs(ROLES, doc! { "db": db }).await?;
        Ok((users, roles))
    }

    /// Control layer (create, fork): does any user or role still exist with `db` as its database?
    pub(crate) async fn database_has_auth(&self, db: &str) -> R<bool> {
        Ok(!self.auth_docs(USERS, &doc! { "db": db }).await?.is_empty() || !self.auth_docs(ROLES, &doc! { "db": db }).await?.is_empty())
    }

    async fn any_users(&self) -> R<bool> {
        Ok(!self.auth_docs(USERS, &Document::new()).await?.is_empty())
    }

    // ---- roles and privileges -------------------------------------------------------------------

    /// Privileges of `roles` (with everything they inherit).
    pub(crate) async fn role_privileges(&self, roles: &[(String, String)]) -> R<Vec<Priv>> {
        let custom: HashMap<String, Document> = self.auth_docs(ROLES, &Document::new()).await?.into_iter().filter_map(|d| Some((d.get_str("_id").ok()?.to_string(), d))).collect();
        let mut seen: HashSet<(String, String)> = HashSet::new();
        let mut stack: Vec<(String, String)> = roles.to_vec();
        let mut out = Vec::new();
        while let Some((r, db)) = stack.pop() {
            if !seen.insert((r.clone(), db.clone())) {
                continue;
            }
            if let Some((privs, inh)) = authz::builtin(&r, &db) {
                out.extend(privs);
                stack.extend(inh);
            } else if let Some(d) = custom.get(&format!("{db}.{r}")) {
                for p in d.get_array("privileges").into_iter().flatten().filter_map(|p| p.as_document()).filter_map(Priv::from_doc) {
                    out.push(p);
                }
                for b in d.get_array("roles").into_iter().flatten() {
                    if let Ok(p) = role_pair(b, &db) {
                        stack.push(p);
                    }
                }
            }
        }
        Ok(out)
    }

    /// All roles a role set expands to, for `rolesInfo` / `usersInfo` (`inheritedRoles`).
    async fn inherited_roles(&self, roles: &[(String, String)]) -> R<Vec<(String, String)>> {
        let custom: HashMap<String, Document> = self.auth_docs(ROLES, &Document::new()).await?.into_iter().filter_map(|d| Some((d.get_str("_id").ok()?.to_string(), d))).collect();
        let mut seen: Vec<(String, String)> = Vec::new();
        let mut stack: Vec<(String, String)> = roles.iter().rev().cloned().collect();
        while let Some((r, db)) = stack.pop() {
            if seen.contains(&(r.clone(), db.clone())) {
                continue;
            }
            seen.push((r.clone(), db.clone()));
            let next: Vec<(String, String)> = if let Some((_, inh)) = authz::builtin(&r, &db) {
                inh
            } else if let Some(d) = custom.get(&format!("{db}.{r}")) {
                d.get_array("roles").into_iter().flatten().filter_map(|b| role_pair(b, &db).ok()).collect()
            } else {
                vec![]
            };
            for n in next.into_iter().rev() {
                stack.push(n);
            }
        }
        Ok(seen)
    }

    async fn role_exists(&self, r: &str, db: &str) -> R<bool> {
        if authz::is_builtin_role(r, db) {
            return Ok(true);
        }
        Ok(!self.auth_docs(ROLES, &doc! { "_id": format!("{db}.{r}") }).await?.is_empty())
    }

    async fn user_roles_of(&self, user: &str, db: &str) -> R<Vec<(String, String)>> {
        Ok(match self.find_user(db, user).await? {
            Some(d) => d.get_array("roles").into_iter().flatten().filter_map(|b| role_pair(b, db).ok()).collect(),
            None => vec![],
        })
    }

    async fn conn_privileges(&self, conn_id: i64) -> R<Vec<Priv>> {
        let gen = self.auth.gen.load(Relaxed);
        let (users, cached) = self.with_conn(conn_id, |c| (c.users.clone(), c.privs.as_ref().filter(|(g, _)| *g == gen).map(|(_, p)| p.clone())));
        if let Some(p) = cached {
            return Ok(p);
        }
        let mut roles = Vec::new();
        for (u, db) in &users {
            roles.extend(self.user_roles_of(u, db).await?);
        }
        let privs = self.role_privileges(&roles).await?;
        self.with_conn(conn_id, |c| c.privs = Some((gen, privs.clone())));
        Ok(privs)
    }

    // ---- authorization of a command -----------------------------------------------------------

    pub(crate) async fn authorize(&self, conn_id: i64, db: &str, name: &str, cmd: &Document) -> R<()> {
        if !self.auth.enabled.load(Relaxed) {
            return Ok(());
        }
        let Some(needs) = authz::required(name, db, cmd) else { return Ok(()) };
        let (authed, local) = self.with_conn(conn_id, |c| (!c.users.is_empty(), c.local));
        if !authed {
            // localhost exception: until the first user exists, the first user can be created from the machine itself
            if local && matches!(name, "createUser" | "createRole") && db == "admin" && !self.any_users().await? {
                return Ok(());
            }
            if local && matches!(name, "createUser") && !self.any_users().await? {
                return Err(unauthorized(format!("command {name} requires authentication")));
            }
            return Err(unauthorized(format!("command {name} requires authentication")));
        }
        let privs = self.conn_privileges(conn_id).await?;
        let missing: Vec<&Need> = needs.iter().filter(|(r, a)| !authz::allows(&privs, r, a)).collect();
        if missing.is_empty() {
            if !self.may_grant(&privs, conn_id, db, name, cmd).await? {
                return Err(not_authorized(db, name, cmd));
            }
            return Ok(());
        }
        Err(not_authorized(db, name, cmd))
    }

    /// Roles and privileges handed out by `createUser`, `updateUser`, `grantRolesToUser`, `createRole`, `updateRole`,
    /// `grantRolesToRole` and `grantPrivilegesToRole` must lie within what the caller may hand out. For every privilege the
    /// role (with everything it inherits) or the command carries:
    /// * on one named database: the caller needs the `grantRole` action on that database (mongod's rule). A user admin of
    ///   `app1` can therefore hand out roles on `app1`, and nothing on `app2`, `admin` or the cluster;
    /// * on the cluster, on every database, on any resource, or on the time-series buckets: the caller must already hold
    ///   those very privileges (all the actions), so `userAdminAnyDatabase` cannot hand out `root` and a custom role that
    ///   inherits `root` is no way around it.
    async fn may_grant(&self, caller: &[Priv], _conn_id: i64, db: &str, name: &str, cmd: &Document) -> R<bool> {
        let with_roles = matches!(name, "createUser" | "updateUser" | "grantRolesToUser" | "createRole" | "updateRole" | "grantRolesToRole");
        let with_privs = matches!(name, "createRole" | "updateRole" | "grantPrivilegesToRole");
        if !with_roles && !with_privs {
            return Ok(true);
        }
        let mut roles: Vec<(String, String)> = Vec::new();
        if with_roles {
            for b in cmd.get_array("roles").into_iter().flatten() {
                if let Ok(p) = role_pair(b, db) {
                    roles.push(p);
                }
            }
        }
        let mut wanted = self.role_privileges(&roles).await?;
        if with_privs {
            wanted.extend(cmd.get_array("privileges").into_iter().flatten().filter_map(|p| p.as_document()).filter_map(Priv::from_doc));
        }
        Ok(wanted.iter().all(|p| match &p.res {
            Res::Ns { db: pdb, .. } if !pdb.is_empty() => authz::allows(caller, &authz::ns(pdb, ""), "grantRole"),
            // equal or covered: the buckets resource does not cover itself in `Res::covers`
            res => p.actions.iter().all(|a| caller.iter().any(|c| (c.res == *res || c.res.covers(res)) && c.actions.iter().any(|x| x == a || x == "anyAction"))),
        }))
    }

    /// The databases a user without the `listDatabases` privilege may see: those with any privilege.
    pub(crate) async fn visible_databases(&self, conn_id: i64, names: Vec<String>) -> R<Vec<String>> {
        if !self.auth.enabled.load(Relaxed) {
            return Ok(names);
        }
        let privs = self.conn_privileges(conn_id).await?;
        if authz::allows(&privs, &Res::Cluster, "listDatabases") {
            return Ok(names);
        }
        Ok(names
            .into_iter()
            .filter(|n| privs.iter().any(|p| matches!(&p.res, Res::Any) || matches!(&p.res, Res::Ns { db, .. } if db.is_empty() || db == n)))
            .collect())
    }

    // ---- hello / connectionStatus / logout ---------------------------------------------------------

    pub(crate) async fn auth_hello(&self, cmd: &Document, reply: &mut Document) -> R<()> {
        if let Ok(spec) = cmd.get_str("saslSupportedMechs") {
            if let Some((db, user)) = spec.split_once('.') {
                let mechs: Vec<Bson> = match self.find_user(db, user).await? {
                    Some(u) => {
                        let c = u.get_document("credentials").cloned().unwrap_or_default();
                        ["SCRAM-SHA-1", "SCRAM-SHA-256"].iter().filter(|m| c.contains_key(**m)).map(|m| Bson::String(m.to_string())).collect()
                    }
                    None => vec![],
                };
                if !mechs.is_empty() {
                    reply.insert("saslSupportedMechs", mechs);
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn connection_status(&self, conn_id: i64, cmd: &Document) -> R<Document> {
        let users = self.with_conn(conn_id, |c| c.users.clone());
        let mut roles: Vec<(String, String)> = Vec::new();
        for (u, db) in &users {
            for r in self.user_roles_of(u, db).await? {
                if !roles.contains(&r) {
                    roles.push(r);
                }
            }
        }
        let mut info = doc! {
            "authenticatedUsers": users.iter().map(|(u, d)| Bson::Document(doc! { "user": u.clone(), "db": d.clone() })).collect::<Vec<_>>(),
            "authenticatedUserRoles": roles.iter().map(role_doc).collect::<Vec<_>>(),
        };
        if bool_arg(cmd.get("showPrivileges"), false) {
            let privs = self.role_privileges(&roles).await?;
            info.insert("authenticatedUserPrivileges", privs.iter().map(|p| Bson::Document(p.to_doc())).collect::<Vec<_>>());
        }
        Ok(doc! { "authInfo": info, "ok": 1.0 })
    }

    pub(crate) fn logout(&self, conn_id: i64, db: &str) -> R<Document> {
        self.with_conn(conn_id, |c| {
            c.users.retain(|(_, d)| d != db);
            c.privs = None;
        });
        Ok(ok())
    }

    // ---- SCRAM -----------------------------------------------------------------------------------------

    fn payload_of(cmd: &Document) -> R<Vec<u8>> {
        match cmd.get("payload") {
            Some(Bson::Binary(b)) => Ok(b.bytes.clone()),
            Some(Bson::String(s)) => Ok(s.as_bytes().to_vec()),
            _ => Err(MongoError::new(2, "BadValue", "BSON field 'payload' is missing but a required field".into())),
        }
    }

    pub(crate) async fn sasl_start(&self, conn_id: i64, db: &str, cmd: &Document) -> R<Document> {
        let mech_name = cmd.get_str("mechanism").map_err(|_| MongoError::new(2, "BadValue", "BSON field 'mechanism' is missing but a required field".into()))?;
        let Some(mech) = Mech::parse(mech_name) else {
            return Err(MongoError::new(2, "BadValue", format!("Unsupported mechanism {mech_name}")));
        };
        let payload = String::from_utf8(Self::payload_of(cmd)?).map_err(|_| auth_failed())?;
        let (user, bare, cnonce) = scram::parse_client_first(&payload).ok_or_else(|| MongoError::new(2, "BadValue", "Invalid SCRAM client-first message".into()))?;
        let opts = cmd.get_document("options").ok();
        let skip_empty = opts.map(|o| bool_arg(o.get("skipEmptyExchange"), false)).unwrap_or(false);
        let Some(udoc) = self.find_user(db, &user).await? else {
            return Err(auth_failed());
        };
        let creds = udoc.get_document("credentials").ok().and_then(|c| c.get_document(mech.name()).ok()).and_then(scram::Creds::from_doc);
        let Some(creds) = creds else {
            return Err(MongoError::new(334, "MechanismUnavailable", format!("Unable to use {} based authentication for user: {user}", mech.name())));
        };
        self.check_restrictions(conn_id, &udoc)?;
        let (conv, first) = scram::server_first(mech, &user, &bare, &cnonce, creds);
        let id = self.with_conn(conn_id, |c| {
            let id = c.next_conv;
            c.next_conv += 1;
            c.conv = Some(ConvState { id, db: db.to_string(), conv, skip_empty, stage: 1 });
            id
        });
        Ok(doc! { "conversationId": id, "done": false, "payload": bin(first.into_bytes()), "ok": 1.0 })
    }

    pub(crate) async fn sasl_continue(&self, conn_id: i64, cmd: &Document) -> R<Document> {
        let payload = Self::payload_of(cmd)?;
        let cid = cmd.get_i32("conversationId").unwrap_or(-1);
        enum Next {
            Final(ConvState, String),
            Empty(ConvState),
            None,
        }
        let next = self.with_conn(conn_id, |c| match c.conv.take() {
            Some(cs) if cs.id == cid => {
                if cs.stage == 1 {
                    Next::Final(cs, String::from_utf8_lossy(&payload).to_string())
                } else {
                    Next::Empty(cs)
                }
            }
            other => {
                c.conv = other;
                Next::None
            }
        });
        match next {
            Next::None => Err(MongoError::new(17, "ProtocolError", "sasl: no conversation in progress".into())),
            Next::Empty(cs) => {
                self.sasl_success(conn_id, &cs);
                Ok(doc! { "conversationId": cid, "done": true, "payload": bin(vec![]), "ok": 1.0 })
            }
            Next::Final(mut cs, msg) => {
                let Some(v) = scram::server_final(&cs.conv, &msg) else {
                    return Err(auth_failed());
                };
                if cs.skip_empty {
                    self.sasl_success(conn_id, &cs);
                    return Ok(doc! { "conversationId": cid, "done": true, "payload": bin(v.into_bytes()), "ok": 1.0 });
                }
                cs.stage = 2;
                self.with_conn(conn_id, |c| c.conv = Some(cs));
                Ok(doc! { "conversationId": cid, "done": false, "payload": bin(v.into_bytes()), "ok": 1.0 })
            }
        }
    }

    fn sasl_success(&self, conn_id: i64, cs: &ConvState) {
        self.with_conn(conn_id, |c| {
            c.users.retain(|(u, d)| !(d == &cs.db && u == &cs.conv.user));
            c.users.push((cs.conv.user.clone(), cs.db.clone()));
            c.privs = None;
        });
    }

    /// `authenticationRestrictions` of the user (and of its roles): one of the entries must match the connection.
    fn check_restrictions(&self, conn_id: i64, udoc: &Document) -> R<()> {
        let Ok(list) = udoc.get_array("authenticationRestrictions") else { return Ok(()) };
        if list.is_empty() {
            return Ok(());
        }
        let (peer, server) = self.with_conn(conn_id, |c| (c.peer, c.server));
        let in_any = |v: Option<&Bson>, ip: Option<std::net::IpAddr>| -> bool {
            match v {
                Some(Bson::Array(a)) => a.iter().filter_map(|x| x.as_str()).any(|c| ip.map(|ip| cidr_contains(c, ip)).unwrap_or(false)),
                _ => true,
            }
        };
        if list.iter().filter_map(|r| r.as_document()).any(|r| in_any(r.get("clientSource"), peer) && in_any(r.get("serverAddress"), server)) {
            Ok(())
        } else {
            Err(auth_failed())
        }
    }

    /// `authenticate` (MONGODB-X509): the user is the subject of the TLS client certificate, defined in `$external`.
    pub(crate) async fn authenticate(&self, conn_id: i64, db: &str, cmd: &Document) -> R<Document> {
        let mech = cmd.get_str("mechanism").map_err(|_| MongoError::new(2, "BadValue", "BSON field 'mechanism' is missing but a required field".into()))?;
        if mech != "MONGODB-X509" {
            return Err(MongoError::new(2, "BadValue", format!("Unsupported mechanism {mech}")));
        }
        if db != "$external" {
            return Err(MongoError::new(2, "BadValue", "MONGODB-X509 authentication must be done against $external".into()));
        }
        let Some(subject) = self.with_conn(conn_id, |c| c.x509.clone()) else {
            return Err(auth_failed());
        };
        if let Ok(user) = cmd.get_str("user") {
            if user != subject {
                return Err(auth_failed());
            }
        }
        if self.find_user("$external", &subject).await?.is_none() {
            return Err(MongoError::new(11, "UserNotFound", format!("Could not find user \"{subject}\" for db \"$external\"")));
        }
        self.with_conn(conn_id, |c| {
            c.users.retain(|(u, d)| !(d == "$external" && u == &subject));
            c.users.push((subject.clone(), "$external".to_string()));
            c.privs = None;
        });
        Ok(doc! { "dbname": "$external", "user": subject, "ok": 1.0 })
    }

    // ---- user commands ---------------------------------------------------------------------------

    fn name_arg<'a>(cmd: &'a Document, key: &str, what: &str) -> R<&'a str> {
        match cmd.get(key) {
            Some(Bson::String(s)) if !s.is_empty() => Ok(s),
            Some(Bson::String(_)) if what == "User" => Err(MongoError::new(2, "BadValue", "User document needs 'user' field to be non-empty".into())),
            Some(Bson::String(_)) => Err(MongoError::new(2, "BadValue", "Role name must not be empty".into())),
            _ => Err(MongoError::new(14, "TypeMismatch", format!("BSON field '{key}' is the wrong type, expected string"))),
        }
    }

    async fn check_roles(&self, roles: &[(String, String)]) -> R<()> {
        for (r, d) in roles {
            if !self.role_exists(r, d).await? {
                return Err(MongoError::new(31, "RoleNotFound", format!("Could not find role: {r}@{d}")));
            }
        }
        Ok(())
    }

    fn roles_arg(cmd: &Document, key: &str, db: &str) -> R<Vec<(String, String)>> {
        match cmd.get(key) {
            Some(Bson::Array(a)) => a.iter().map(|b| role_pair(b, db)).collect(),
            Some(_) => Err(MongoError::new(14, "TypeMismatch", format!("BSON field '{key}' is the wrong type, expected array"))),
            None => Ok(vec![]),
        }
    }

    pub(crate) async fn create_user(&self, db: &str, cmd: &Document) -> R<Document> {
        let user = Self::name_arg(cmd, "createUser", "User")?;
        let external = db == "$external";
        let mut mechs: Vec<Mech> = vec![Mech::Sha1, Mech::Sha256];
        if let Ok(a) = cmd.get_array("mechanisms") {
            mechs = Vec::new();
            for m in a {
                match m.as_str().and_then(Mech::parse) {
                    Some(x) => mechs.push(x),
                    None => return Err(MongoError::new(2, "BadValue", format!("Unknown auth mechanism '{}'", m.as_str().unwrap_or("")))),
                }
            }
        }
        let mut cred = Document::new();
        if external {
            if cmd.contains_key("pwd") {
                return Err(MongoError::new(2, "BadValue", "Cannot set the password for users defined on the '$external' database".into()));
            }
        } else {
            let pwd = match cmd.get("pwd") {
                Some(Bson::String(s)) => s.clone(),
                Some(v) => return Err(MongoError::new(14, "TypeMismatch", format!("BSON field 'createUser.pwd' is the wrong type '{}', expected type 'string'", crate::update::type_name(v)))),
                None => return Err(MongoError::new(2, "BadValue", "Must provide a 'pwd' field for all user documents, except those with '$external' as the user's source db".into())),
            };
            if pwd.is_empty() {
                return Err(MongoError::new(50687, "Location50687", "Error preflighting UTF-8 conversion: U_STRING_NOT_TERMINATED_WARNING".into()));
            }
            let digest = bool_arg(cmd.get("digestPassword"), true);
            if !digest {
                return Err(MongoError::new(2, "BadValue", "Use of SCRAM-SHA-256 requires undigested passwords".into()));
            }
            for m in &mechs {
                cred.insert(m.name(), scram::make_credentials(*m, user, &pwd));
            }
        }
        let roles = Self::roles_arg(cmd, "roles", db)?;
        if !cmd.contains_key("roles") {
            return Err(MongoError::new(40414, "IDLFailedToParse", "BSON field 'createUser.roles' is missing but a required field".into()));
        }
        self.check_roles(&roles).await?;
        let id = format!("{db}.{user}");
        if self.find_user(db, user).await?.is_some() {
            return Err(MongoError::new(51003, "Location51003", format!("User \"{user}@{db}\" already exists")));
        }
        let uid: [u8; 16] = rand::random();
        let mut d = doc! { "_id": id, "userId": Bson::Binary(Binary { subtype: BinarySubtype::Uuid, bytes: uid.to_vec() }), "user": user, "db": db };
        d.insert("credentials", cred);
        d.insert("roles", roles.iter().map(role_doc).collect::<Vec<_>>());
        if let Some(cd) = cmd.get("customData") {
            d.insert("customData", cd.clone());
        }
        if let Some(ar) = cmd.get("authenticationRestrictions") {
            validate_restrictions(ar)?;
            d.insert("authenticationRestrictions", ar.clone());
        }
        self.insert_auth_doc(USERS, d).await?;
        Ok(ok())
    }

    pub(crate) async fn update_user(&self, db: &str, cmd: &Document) -> R<Document> {
        let user = Self::name_arg(cmd, "updateUser", "User")?;
        let Some(mut d) = self.find_user(db, user).await? else {
            return Err(MongoError::new(11, "UserNotFound", format!("User {user}@{db} not found")));
        };
        if let Some(pwd) = cmd.get("pwd") {
            let Bson::String(pwd) = pwd else { return Err(MongoError::new(14, "TypeMismatch", "BSON field 'updateUser.pwd' is the wrong type, expected string".into())) };
            if pwd.is_empty() {
                return Err(MongoError::new(2, "BadValue", "User passwords must not be empty".into()));
            }
            let existing = d.get_document("credentials").cloned().unwrap_or_default();
            let mut mechs: Vec<Mech> = existing.keys().filter_map(|k| Mech::parse(k)).collect();
            if let Ok(a) = cmd.get_array("mechanisms") {
                mechs = a.iter().filter_map(|m| m.as_str().and_then(Mech::parse)).collect();
            }
            let mut cred = Document::new();
            for m in mechs {
                cred.insert(m.name(), scram::make_credentials(m, user, pwd));
            }
            d.insert("credentials", cred);
        }
        if cmd.contains_key("roles") {
            let roles = Self::roles_arg(cmd, "roles", db)?;
            self.check_roles(&roles).await?;
            d.insert("roles", roles.iter().map(role_doc).collect::<Vec<_>>());
        }
        if let Some(cd) = cmd.get("customData") {
            d.insert("customData", cd.clone());
        }
        if let Some(ar) = cmd.get("authenticationRestrictions") {
            validate_restrictions(ar)?;
            d.insert("authenticationRestrictions", ar.clone());
        }
        self.replace_auth_doc(USERS, d).await?;
        Ok(ok())
    }

    pub(crate) async fn drop_user(&self, db: &str, cmd: &Document) -> R<Document> {
        let user = Self::name_arg(cmd, "dropUser", "User")?;
        let n = self.delete_auth_docs(USERS, doc! { "_id": format!("{db}.{user}") }).await?;
        if n == 0 {
            return Err(MongoError::new(11, "UserNotFound", format!("User '{user}@{db}' not found")));
        }
        Ok(ok())
    }

    pub(crate) async fn drop_all_users(&self, db: &str) -> R<Document> {
        let n = self.delete_auth_docs(USERS, doc! { "db": db }).await?;
        Ok(doc! { "n": n as i32, "ok": 1.0 })
    }

    pub(crate) async fn grant_roles_to_user(&self, db: &str, cmd: &Document, revoke: bool) -> R<Document> {
        let key = if revoke { "revokeRolesFromUser" } else { "grantRolesToUser" };
        let user = Self::name_arg(cmd, key, "User")?;
        let Some(mut d) = self.find_user(db, user).await? else {
            return Err(MongoError::new(11, "UserNotFound", format!("Could not find user \"{user}\" for db \"{db}\"")));
        };
        if !cmd.contains_key("roles") {
            return Err(MongoError::new(40414, "IDLFailedToParse", format!("BSON field '{key}.roles' is missing but a required field")));
        }
        let delta = Self::roles_arg(cmd, "roles", db)?;
        let mut roles: Vec<(String, String)> = d.get_array("roles").into_iter().flatten().filter_map(|b| role_pair(b, db).ok()).collect();
        if revoke {
            roles.retain(|r| !delta.contains(r));
        } else {
            self.check_roles(&delta).await?;
            for r in delta {
                if !roles.contains(&r) {
                    roles.push(r);
                }
            }
        }
        d.insert("roles", roles.iter().map(role_doc).collect::<Vec<_>>());
        self.replace_auth_doc(USERS, d).await?;
        Ok(ok())
    }

    async fn user_info_doc(&self, u: &Document, cmd: &Document) -> R<Document> {
        let db = u.get_str("db").unwrap_or("");
        let mut out = doc! { "_id": u.get("_id").cloned().unwrap_or(Bson::Null), "userId": u.get("userId").cloned().unwrap_or(Bson::Null), "user": u.get_str("user").unwrap_or(""), "db": db };
        if let Some(cd) = u.get("customData") {
            out.insert("customData", cd.clone());
        }
        let roles: Vec<(String, String)> = u.get_array("roles").into_iter().flatten().filter_map(|b| role_pair(b, db).ok()).collect();
        out.insert("roles", roles.iter().map(role_doc).collect::<Vec<_>>());
        let creds = u.get_document("credentials").cloned().unwrap_or_default();
        out.insert("mechanisms", creds.keys().map(|k| Bson::String(k.clone())).collect::<Vec<_>>());
        if bool_arg(cmd.get("showCredentials"), false) {
            out.insert("credentials", creds);
        }
        if bool_arg(cmd.get("showAuthenticationRestrictions"), false) {
            out.insert("authenticationRestrictions", u.get("authenticationRestrictions").cloned().unwrap_or(Bson::Array(vec![])));
        }
        if bool_arg(cmd.get("showPrivileges"), false) {
            let inh = self.inherited_roles(&roles).await?;
            out.insert("inheritedRoles", inh.iter().map(role_doc).collect::<Vec<_>>());
            let privs = self.role_privileges(&roles).await?;
            out.insert("inheritedPrivileges", privs.iter().map(|p| Bson::Document(p.to_doc())).collect::<Vec<_>>());
        }
        Ok(out)
    }

    pub(crate) async fn users_info(&self, db: &str, cmd: &Document) -> R<Document> {
        let spec = cmd.get("usersInfo").cloned().unwrap_or(Bson::Int32(1));
        let mut docs: Vec<Document> = Vec::new();
        let mut for_all = false;
        let mut want: Vec<(String, String)> = Vec::new();
        match &spec {
            Bson::String(u) => want.push((u.clone(), db.to_string())),
            Bson::Document(d) if d.contains_key("forAllDBs") => for_all = bool_arg(d.get("forAllDBs"), false),
            Bson::Document(d) => want.push(user_name_doc(d, db)?),
            Bson::Array(a) => {
                for b in a {
                    match b {
                        Bson::String(u) => want.push((u.clone(), db.to_string())),
                        Bson::Document(d) => want.push(user_name_doc(d, db)?),
                        _ => return Err(MongoError::new(2, "BadValue", "User names must be strings or objects".into())),
                    }
                }
            }
            Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) => {
                docs = self.auth_docs(USERS, &doc! { "db": db }).await?;
            }
            _ => return Err(MongoError::new(2, "BadValue", "Invalid usersInfo argument".into())),
        }
        if for_all {
            docs = self.auth_docs(USERS, &Document::new()).await?;
        }
        for (u, d) in want {
            if let Some(x) = self.find_user(&d, &u).await? {
                docs.push(x);
            }
        }
        let filter = cmd.get_document("filter").cloned().unwrap_or_default();
        let mut users = Vec::new();
        for u in &docs {
            let info = self.user_info_doc(u, cmd).await?;
            if matches(&info, &filter)? {
                users.push(Bson::Document(info));
            }
        }
        Ok(doc! { "users": users, "ok": 1.0 })
    }

    // ---- role commands ---------------------------------------------------------------------------

    fn privileges_arg(cmd: &Document, key: &str) -> R<Vec<Priv>> {
        match cmd.get(key) {
            None => Ok(vec![]),
            Some(Bson::Array(a)) => a
                .iter()
                .map(|b| {
                    let d = b.as_document().ok_or_else(|| MongoError::new(14, "TypeMismatch", "privilege must be an object".into()))?;
                    let r = d.get_document("resource").map_err(|_| MongoError::new(40414, "IDLFailedToParse", "BSON field 'privileges.resource' is missing but a required field".into()))?;
                    if !r.contains_key("cluster") && !r.contains_key("anyResource") && (r.contains_key("db") != r.contains_key("collection")) {
                        return Err(MongoError::new(9, "FailedToParse", "resource must set both db and collection or neither, but not exactly one.".into()));
                    }
                    let p = Priv::from_doc(d).ok_or_else(|| MongoError::new(9, "FailedToParse", "Invalid privilege specification".into()))?;
                    for a in &p.actions {
                        if !authz::action_known(a) {
                            return Err(MongoError::new(2, "BadValue", format!("Unrecognized action: {a}")));
                        }
                    }
                    Ok(p)
                })
                .collect(),
            Some(_) => Err(MongoError::new(14, "TypeMismatch", format!("BSON field '{key}' is the wrong type, expected array"))),
        }
    }

    pub(crate) async fn create_role(&self, db: &str, cmd: &Document) -> R<Document> {
        let role = Self::name_arg(cmd, "createRole", "Role")?;
        if authz::is_builtin_role(role, db) {
            return Err(MongoError::new(2, "BadValue", "Cannot create roles with the same name as a built-in role".into()));
        }
        let roles = Self::roles_arg(cmd, "roles", db)?;
        if !cmd.contains_key("roles") {
            return Err(MongoError::new(40414, "IDLFailedToParse", "BSON field 'createRole.roles' is missing but a required field".into()));
        }
        if !cmd.contains_key("privileges") {
            return Err(MongoError::new(40414, "IDLFailedToParse", "BSON field 'createRole.privileges' is missing but a required field".into()));
        }
        let privs = Self::privileges_arg(cmd, "privileges")?;
        self.check_roles(&roles).await?;
        if !self.auth_docs(ROLES, &doc! { "_id": format!("{db}.{role}") }).await?.is_empty() {
            return Err(MongoError::new(51002, "Location51002", format!("Role \"{role}@{db}\" already exists")));
        }
        let mut d = doc! { "_id": format!("{db}.{role}"), "role": role, "db": db };
        d.insert("privileges", privs.iter().map(|p| Bson::Document(p.to_doc())).collect::<Vec<_>>());
        d.insert("roles", roles.iter().map(role_doc).collect::<Vec<_>>());
        if let Some(ar) = cmd.get("authenticationRestrictions") {
            d.insert("authenticationRestrictions", ar.clone());
        }
        self.insert_auth_doc(ROLES, d).await?;
        Ok(ok())
    }

    async fn find_role(&self, role: &str, db: &str) -> R<Document> {
        self.find_role_msg(role, db, "Role {} not found").await
    }

    async fn find_role_msg(&self, role: &str, db: &str, msg: &str) -> R<Document> {
        self.auth_docs(ROLES, &doc! { "_id": format!("{db}.{role}") }).await?.into_iter().next().ok_or_else(|| MongoError::new(31, "RoleNotFound", msg.replace("{}", &format!("{role}@{db}"))))
    }

    pub(crate) async fn update_role(&self, db: &str, cmd: &Document) -> R<Document> {
        let role = Self::name_arg(cmd, "updateRole", "Role")?;
        let mut d = self.find_role(role, db).await?;
        if cmd.contains_key("privileges") {
            let privs = Self::privileges_arg(cmd, "privileges")?;
            d.insert("privileges", privs.iter().map(|p| Bson::Document(p.to_doc())).collect::<Vec<_>>());
        }
        if cmd.contains_key("roles") {
            let roles = Self::roles_arg(cmd, "roles", db)?;
            self.check_roles(&roles).await?;
            d.insert("roles", roles.iter().map(role_doc).collect::<Vec<_>>());
        }
        self.replace_auth_doc(ROLES, d).await?;
        Ok(ok())
    }

    pub(crate) async fn drop_role(&self, db: &str, cmd: &Document) -> R<Document> {
        let role = Self::name_arg(cmd, "dropRole", "Role")?;
        if authz::is_builtin_role(role, db) {
            return Err(MongoError::new(2, "BadValue", format!("{role}@{db} is a built-in role and cannot be modified.")));
        }
        if self.auth_docs(ROLES, &doc! { "_id": format!("{db}.{role}") }).await?.is_empty() {
            return Err(MongoError::new(31, "RoleNotFound", format!("Could not find role: {role}@{db}")));
        }
        self.delete_auth_docs(ROLES, doc! { "_id": format!("{db}.{role}") }).await?;
        // the role disappears from users and from other roles
        let pair = (role.to_string(), db.to_string());
        for u in self.auth_docs(USERS, &Document::new()).await? {
            let udb = u.get_str("db").unwrap_or("").to_string();
            let roles: Vec<(String, String)> = u.get_array("roles").into_iter().flatten().filter_map(|b| role_pair(b, &udb).ok()).collect();
            if roles.contains(&pair) {
                let mut u = u;
                u.insert("roles", roles.iter().filter(|r| **r != pair).map(role_doc).collect::<Vec<_>>());
                self.replace_auth_doc(USERS, u).await?;
            }
        }
        for r in self.auth_docs(ROLES, &Document::new()).await? {
            let rdb = r.get_str("db").unwrap_or("").to_string();
            let roles: Vec<(String, String)> = r.get_array("roles").into_iter().flatten().filter_map(|b| role_pair(b, &rdb).ok()).collect();
            if roles.contains(&pair) {
                let mut r = r;
                r.insert("roles", roles.iter().filter(|x| **x != pair).map(role_doc).collect::<Vec<_>>());
                self.replace_auth_doc(ROLES, r).await?;
            }
        }
        Ok(ok())
    }

    pub(crate) async fn drop_all_roles(&self, db: &str) -> R<Document> {
        let n = self.delete_auth_docs(ROLES, doc! { "db": db }).await?;
        Ok(doc! { "n": n as i32, "ok": 1.0 })
    }

    pub(crate) async fn role_delta(&self, db: &str, cmd: &Document, name: &str) -> R<Document> {
        let role = Self::name_arg(cmd, name, "Role")?;
        if authz::is_builtin_role(role, db) {
            return Err(MongoError::new(2, "BadValue", format!("{role}@{db} is a built-in role and cannot be modified.")));
        }
        let mut d = self.find_role_msg(role, db, "Role {} not found").await?;
        match name {
            "grantRolesToRole" | "revokeRolesFromRole" => {
                if !cmd.contains_key("roles") {
                    return Err(MongoError::new(40414, "IDLFailedToParse", format!("BSON field '{name}.roles' is missing but a required field")));
                }
                let delta = Self::roles_arg(cmd, "roles", db)?;
                let mut roles: Vec<(String, String)> = d.get_array("roles").into_iter().flatten().filter_map(|b| role_pair(b, db).ok()).collect();
                if name == "grantRolesToRole" {
                    self.check_roles(&delta).await?;
                    for r in delta {
                        if !roles.contains(&r) {
                            roles.push(r);
                        }
                    }
                } else {
                    roles.retain(|r| !delta.contains(r));
                }
                d.insert("roles", roles.iter().map(role_doc).collect::<Vec<_>>());
            }
            _ => {
                if !cmd.contains_key("privileges") {
                    return Err(MongoError::new(40414, "IDLFailedToParse", format!("BSON field '{name}.privileges' is missing but a required field")));
                }
                let delta = Self::privileges_arg(cmd, "privileges")?;
                let mut privs: Vec<Priv> = d.get_array("privileges").into_iter().flatten().filter_map(|p| p.as_document()).filter_map(Priv::from_doc).collect();
                for p in delta {
                    let pos = privs.iter().position(|x| x.res == p.res);
                    if name == "grantPrivilegesToRole" {
                        match pos {
                            Some(i) => {
                                for a in p.actions {
                                    if !privs[i].actions.contains(&a) {
                                        privs[i].actions.push(a);
                                    }
                                }
                            }
                            None => privs.push(p),
                        }
                    } else if let Some(i) = pos {
                        privs[i].actions.retain(|a| !p.actions.contains(a));
                        if privs[i].actions.is_empty() {
                            privs.remove(i);
                        }
                    }
                }
                d.insert("privileges", privs.iter().map(|p| Bson::Document(p.to_doc())).collect::<Vec<_>>());
            }
        }
        self.replace_auth_doc(ROLES, d).await?;
        Ok(ok())
    }

    pub(crate) async fn roles_info(&self, db: &str, cmd: &Document) -> R<Document> {
        let spec = cmd.get("rolesInfo").cloned().unwrap_or(Bson::Int32(1));
        let show_priv = match cmd.get("showPrivileges") {
            Some(Bson::Boolean(b)) => *b,
            Some(Bson::String(s)) if s == "asUserFragment" => true,
            _ => false,
        };
        let show_builtin = bool_arg(cmd.get("showBuiltinRoles"), false);
        let mut wanted: Vec<(String, String)> = Vec::new();
        let mut all_in_db = false;
        let mut all_dbs = false;
        match &spec {
            Bson::String(r) => wanted.push((r.clone(), db.to_string())),
            Bson::Document(d) if d.contains_key("forAllDBs") => all_dbs = bool_arg(d.get("forAllDBs"), false),
            Bson::Document(d) => wanted.push((d.get_str("role").unwrap_or("").to_string(), d.get_str("db").unwrap_or(db).to_string())),
            Bson::Array(a) => {
                for b in a {
                    if !matches!(b, Bson::String(_) | Bson::Document(_)) {
                        return Err(MongoError::new(2, "BadValue", "RoleName must be either a string or an object".into()));
                    }
                    wanted.push(role_pair(b, db)?);
                }
            }
            Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) => all_in_db = true,
            _ => return Err(MongoError::new(2, "BadValue", "RoleName must be either a string or an object".into())),
        }
        let mut docs: Vec<(String, String)> = wanted;
        if all_in_db || all_dbs {
            for r in self.auth_docs(ROLES, &Document::new()).await? {
                let (rn, rdb) = (r.get_str("role").unwrap_or("").to_string(), r.get_str("db").unwrap_or("").to_string());
                if all_dbs || rdb == db {
                    docs.push((rn, rdb));
                }
            }
            if show_builtin {
                for n in authz::builtin_names(db) {
                    docs.push((n, db.to_string()));
                }
            }
        }
        let mut out = Vec::new();
        for (r, rdb) in docs {
            let builtin = authz::builtin(&r, &rdb);
            let (direct, own_privs): (Vec<(String, String)>, Vec<Priv>) = if let Some((p, inh)) = &builtin {
                (inh.clone(), p.clone())
            } else if let Some(d) = self.auth_docs(ROLES, &doc! { "_id": format!("{rdb}.{r}") }).await?.into_iter().next() {
                (d.get_array("roles").into_iter().flatten().filter_map(|b| role_pair(b, &rdb).ok()).collect(), d.get_array("privileges").into_iter().flatten().filter_map(|p| p.as_document()).filter_map(Priv::from_doc).collect())
            } else {
                continue;
            };
            let mut e = if builtin.is_some() { doc! { "role": r.clone(), "db": rdb.clone() } } else { doc! { "_id": format!("{rdb}.{r}"), "role": r.clone(), "db": rdb.clone() } };
            if builtin.is_some() {
                e = doc! { "db": rdb.clone(), "role": r.clone() };
            }
            if show_priv && builtin.is_none() {
                e.insert("privileges", own_privs.iter().map(|p| Bson::Document(p.to_doc())).collect::<Vec<_>>());
            }
            e.insert("roles", direct.iter().map(role_doc).collect::<Vec<_>>());
            if show_priv && builtin.is_some() {
                e.insert("privileges", own_privs.iter().map(|p| Bson::Document(p.to_doc())).collect::<Vec<_>>());
            }
            let inh = self.inherited_roles(&[(r.clone(), rdb.clone())]).await?;
            e.insert("inheritedRoles", inh.iter().filter(|x| **x != (r.clone(), rdb.clone())).map(role_doc).collect::<Vec<_>>());
            if show_priv {
                let all = self.role_privileges(&[(r.clone(), rdb.clone())]).await?;
                e.insert("inheritedPrivileges", all.iter().map(|p| Bson::Document(p.to_doc())).collect::<Vec<_>>());
            }
            e.insert("isBuiltin", builtin.is_some());
            out.push(Bson::Document(e));
        }
        Ok(doc! { "roles": out, "ok": 1.0 })
    }
}
