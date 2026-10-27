//! Keep awake: pin a database so it never sleeps.
//!
//! The pin is the object `control/pins/{db}`: `{pinned, holder, expires_ms, seq, since_ms}`. It outlives any process.
//! One process at a time is responsible for waking the database: the holder, which renews the pin lease every third of
//! `--pin-lease-ttl-secs` with a conditional update. After a restart the pin is still there; a process with the same
//! holder id takes it back at once, any other one when the lease has expired (a clean shutdown releases it). The
//! holder wakes the database when it is asleep, and every process that has it open keeps it from going idle (touching
//! the entry is all the idle reaper looks at), so a database that is served by one process and pinned by another does
//! not flap.

use super::rec::{self, now_ms};
use super::{meta, ApiError, ApiResult, Ctl};
use futures::StreamExt;
use object_store::path::Path;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub fn pin_path(db: &str) -> Path {
    rec::path(&["control", "pins", db])
}

fn ttl_ms(ctl: &Ctl) -> i64 {
    (ctl.args.pin_lease_ttl_secs.max(1) * 1000) as i64
}

/// Take (or renew) the pin lease for this process if it is free, expired or already ours. Returns the record and
/// whether it is ours. `create`: make the pin if it does not exist (pinning); otherwise a missing pin stays missing.
async fn take(ctl: &Ctl, db: &str, create: bool) -> anyhow::Result<Option<(Value, bool)>> {
    let me = ctl.holder.clone();
    let ttl = ttl_ms(ctl);
    let mut mine = false;
    let cur = rec::update(&ctl.store, &pin_path(db), |cur| {
        let now = now_ms();
        match cur {
            None if !create => Ok(None),
            None => {
                mine = true;
                Ok(Some(json!({ "pinned": true, "holder": me, "expires_ms": now + ttl, "seq": 1, "since_ms": now })))
            }
            Some(mut c) => {
                let holder = c["holder"].as_str().unwrap_or("").to_string();
                let exp = c["expires_ms"].as_i64().unwrap_or(0);
                if holder == me || holder.is_empty() || exp + 1500 < now {
                    c["holder"] = json!(me);
                    c["expires_ms"] = json!(now + ttl);
                    c["seq"] = json!(c["seq"].as_i64().unwrap_or(0) + 1);
                    mine = true;
                    Ok(Some(c))
                } else {
                    mine = false;
                    Ok(None)
                }
            }
        }
    })
    .await?;
    Ok(cur.map(|c| (c, mine)))
}

fn lease_view(p: &Value) -> Value {
    json!({
        "holder": p["holder"], "expires_at": rec::rfc3339(p["expires_ms"].as_i64().unwrap_or(0)), "expires_at_ms": p["expires_ms"],
        "renewals": p["seq"].as_i64().unwrap_or(1) - 1, "pinned_since": rec::rfc3339(p["since_ms"].as_i64().unwrap_or(0)),
    })
}

pub async fn lease_of(ctl: &Ctl, db: &str) -> anyhow::Result<Value> {
    Ok(rec::get(&ctl.store, &pin_path(db)).await?.map(|p| lease_view(&p)).unwrap_or(Value::Null))
}

/// Pin the database and wake it.
pub async fn set(ctl: &Arc<Ctl>, db: &str) -> ApiResult<Value> {
    meta::check_name(db)?;
    let _op = ctl.ops.lock(db).await?;
    if !meta::exists(ctl, db).await? {
        return Err(ApiError::not_found(format!("database {db} does not exist")));
    }
    // The pin object is the truth; the registry copy follows it (and is repaired by the agent if a crash separates them).
    let (p, _mine) = take(ctl, db, true).await?.ok_or_else(|| ApiError::new(500, "internal", "pin vanished"))?;
    let since = p["since_ms"].as_i64().unwrap_or_else(now_ms);
    meta::modify(ctl, db, |m| {
        m["keep_awake"] = json!(true);
        m["keep_awake_since_ms"] = json!(since);
    })
    .await?;
    known_insert(db, _mine);
    drop(_op);
    wake(ctl, db).await;
    let mut d = meta::describe(ctl, db, false).await?;
    d["keep_awake_lease"] = lease_view(&p);
    Ok(d)
}

pub async fn unset(ctl: &Arc<Ctl>, db: &str) -> ApiResult<Value> {
    meta::check_name(db)?;
    let _op = ctl.ops.lock(db).await?;
    if !meta::exists(ctl, db).await? {
        return Err(ApiError::not_found(format!("database {db} does not exist")));
    }
    rec::delete(&ctl.store, &pin_path(db)).await?;
    known_remove(db);
    meta::modify(ctl, db, |m| {
        m["keep_awake"] = json!(false);
        m["keep_awake_since_ms"] = Value::Null;
    })
    .await?;
    meta::describe(ctl, db, false).await
}

/// Open the database in this process if it is not open (a reader: the first write upgrades it, as for any client).
async fn wake(ctl: &Arc<Ctl>, db: &str) {
    let h = ctl.dbs.lock_handle(db);
    let open = h.try_read().map(|g| g.is_open()).unwrap_or(true); // locked: someone is using it
    if open {
        return;
    }
    match ctl.dbs.read(db).await {
        Ok(v) => {
            drop(v);
            ctl.usage.touch(db);
        }
        Err(e) => {
            if e.downcast_ref::<crate::lease::LeaseHeld>().is_none() {
                eprintln!("keep-awake: waking {db} failed: {e:#}");
            }
        }
    }
}

fn touch(ctl: &Ctl, db: &str) -> bool {
    match ctl.dbs.lock_handle(db).try_read() {
        Ok(g) => {
            if g.is_open() {
                g.touch();
                true
            } else {
                false
            }
        }
        Err(_) => true, // in use right now
    }
}

struct Known {
    next_lease: Instant,
    mine: bool,
}

/// The pinned databases this process knows of. Shared so that pinning and unpinning through the API take effect at once
/// (the agent would otherwise go on touching or re-waking an unpinned database until its next listing).
static KNOWN: std::sync::Mutex<Option<HashMap<String, Known>>> = std::sync::Mutex::new(None);

fn known_insert(db: &str, mine: bool) {
    KNOWN.lock().unwrap().get_or_insert_with(HashMap::new).insert(db.to_string(), Known { next_lease: Instant::now() + Duration::from_millis(500), mine });
}

fn known_remove(db: &str) {
    if let Some(m) = KNOWN.lock().unwrap().as_mut() {
        m.remove(db);
    }
}

pub fn spawn_agent(ctl: &Arc<Ctl>) {
    let ctl = Arc::clone(ctl);
    tokio::spawn(async move {
        let mut next_list = Instant::now();
        let refresh = Duration::from_millis((ctl.args.pin_lease_ttl_secs.max(1) * 1000 / 3).clamp(1000, 10_000));
        let mut tick = tokio::time::interval(Duration::from_millis(500));
        loop {
            tick.tick().await;
            let now = Instant::now();
            if now >= next_list {
                let listed: Vec<Path> = ctl.store.list(Some(&rec::path(&["control", "pins"]))).filter_map(|m| async { m.ok().map(|m| m.location) }).collect().await;
                let names: Vec<String> = listed.iter().filter_map(|p| p.filename().map(|s| s.to_string())).collect();
                let mut g = KNOWN.lock().unwrap();
                let known = g.get_or_insert_with(HashMap::new);
                known.retain(|k, _| names.contains(k));
                for n in names {
                    known.entry(n).or_insert(Known { next_lease: now, mine: false });
                }
                // With no pins at all the listing is only needed to notice a pin made through another process: slower.
                next_list = now + if known.is_empty() { Duration::from_secs(30) } else { refresh };
            }
            // work on a copy of the table: nothing is held across an await
            let todo: Vec<(String, bool, Instant)> = KNOWN.lock().unwrap().as_ref().map(|m| m.iter().map(|(k, v)| (k.clone(), v.mine, v.next_lease)).collect()).unwrap_or_default();
            for (db, mine, next_lease) in todo {
                let open = touch(&ctl, &db);
                let mut mine = mine;
                if now >= next_lease {
                    let Some(_op) = ctl.ops.try_lock(&db) else { continue };
                    match take(&ctl, &db, false).await {
                        Ok(Some((p, m))) => {
                            mine = m;
                            let next = now + Duration::from_millis(if m { ttl_ms(&ctl) as u64 / 3 } else { 1000 });
                            if let Some(k) = KNOWN.lock().unwrap().as_mut().and_then(|g| g.get_mut(&db)) {
                                k.mine = m;
                                k.next_lease = next;
                            }
                            // Repair the registry copy if a crash separated it from the pin object.
                            if m {
                                let since = p["since_ms"].as_i64().unwrap_or(0);
                                let _ = meta::modify(&ctl, &db, |md| {
                                    if md["keep_awake"] != json!(true) {
                                        md["keep_awake"] = json!(true);
                                        md["keep_awake_since_ms"] = json!(since);
                                    }
                                })
                                .await;
                            }
                        }
                        Ok(None) => {
                            // the pin is gone (unpinned elsewhere) or someone else holds it
                            if let Some(k) = KNOWN.lock().unwrap().as_mut().and_then(|g| g.get_mut(&db)) {
                                k.mine = false;
                                k.next_lease = now + refresh;
                            }
                            mine = false;
                        }
                        Err(e) => {
                            eprintln!("keep-awake: lease of {db}: {e:#}");
                            if let Some(k) = KNOWN.lock().unwrap().as_mut().and_then(|g| g.get_mut(&db)) {
                                k.next_lease = now + Duration::from_secs(1);
                            }
                        }
                    }
                }
                // still pinned? (an unpin through the API removes the entry at once)
                let pinned = KNOWN.lock().unwrap().as_ref().map(|m| m.contains_key(&db)).unwrap_or(false);
                if pinned && mine && !open {
                    wake(&ctl, &db).await;
                }
            }
        }
    });
}

/// At a clean shutdown: hand the pins this process holds to whoever starts next (their lease expires now).
pub async fn release_all(ctl: &Arc<Ctl>) {
    let listed: Vec<Path> = ctl.store.list(Some(&rec::path(&["control", "pins"]))).filter_map(|m| async { m.ok().map(|m| m.location) }).collect().await;
    for p in listed {
        let me = ctl.holder.clone();
        let _ = rec::update(&ctl.store, &p, |cur| {
            Ok(cur.and_then(|mut c| {
                if c["holder"].as_str() == Some(me.as_str()) {
                    c["holder"] = json!("");
                    c["expires_ms"] = json!(0);
                    Some(c)
                } else {
                    None
                }
            }))
        })
        .await;
    }
}
