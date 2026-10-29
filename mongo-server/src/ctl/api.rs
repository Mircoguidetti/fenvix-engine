//! Routes of the admin interface. Every route is under `/v1`, takes and returns JSON (exports stream bytes), and
//! reports errors as `{"error": {"code", "message"}}` with the HTTP status in [`ApiError`].

use super::http::{Request, Response};
use super::{meta, usage, ApiError, ApiResult, Ctl};
use serde_json::{json, Value};
use std::sync::Arc;

fn body_json(req: &Request) -> ApiResult<Value> {
    if req.body.is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_slice(&req.body).map_err(|e| ApiError::bad(format!("body is not valid JSON: {e}")))
}

fn flag(req: &Request, name: &str, default: bool) -> bool {
    req.query.get(name).map(|v| matches!(v.as_str(), "1" | "true" | "yes")).unwrap_or(default)
}

pub async fn route(ctl: Arc<Ctl>, req: Request) -> Response {
    match dispatch(&ctl, &req).await {
        Ok(r) => r,
        Err(e) => Response::error(e.status, e.code, e.message),
    }
}

async fn dispatch(ctl: &Arc<Ctl>, req: &Request) -> ApiResult<Response> {
    let segs: Vec<&str> = req.path.trim_matches('/').split('/').collect();
    let m = req.method.as_str();
    match (m, segs.as_slice()) {
        ("GET", ["v1", "health"]) => Ok(Response::json(200, &json!({ "ok": true, "holder": ctl.holder, "started_at": super::rec::rfc3339(ctl.start_ms) }))),
        ("POST", ["v1", "databases"]) => {
            let b = body_json(req)?;
            let name = b["name"].as_str().ok_or_else(|| ApiError::bad("body needs a string field name"))?;
            Ok(Response::json(201, &meta::create(ctl, name).await?))
        }
        ("GET", ["v1", "databases"]) => {
            let with_storage = flag(req, "storage", false);
            let mut out = Vec::new();
            for n in meta::names(ctl).await? {
                match meta::describe(ctl, &n, with_storage).await {
                    Ok(d) => out.push(d),
                    Err(e) if e.status == 404 => {}
                    Err(e) => return Err(e),
                }
            }
            Ok(Response::json(200, &json!({ "databases": out })))
        }
        ("GET", ["v1", "databases", db]) => {
            meta::check_name(db)?;
            Ok(Response::json(200, &meta::describe(ctl, db, flag(req, "storage", true)).await?))
        }
        ("GET", ["v1", "databases", db, "usage"]) => {
            let v = usage::query(ctl, db, req.query.get("from").map(|s| s.as_str()), req.query.get("to").map(|s| s.as_str()), flag(req, "storage", true)).await?;
            Ok(Response::json(200, &v))
        }
        ("POST", ["v1", "databases", db, "fork"]) => {
            let b = body_json(req)?;
            let target = b["target"].as_str().ok_or_else(|| ApiError::bad("body needs a string field target"))?;
            let mode = b["mode"].as_str().unwrap_or("clone");
            Ok(Response::json(201, &super::fork::fork(ctl, db, target, mode).await?))
        }
        ("GET", ["v1", "databases", db, "checkpoints"]) => {
            meta::check_name(db)?;
            Ok(Response::json(200, &json!({ "checkpoints": super::purge::checkpoints(&ctl.store, db).await? })))
        }
        ("DELETE", ["v1", "databases", db]) => Ok(Response::json(200, &super::erase::erase(ctl, db).await?)),
        ("PUT", ["v1", "databases", db, "keep-awake"]) => Ok(Response::json(200, &super::pin::set(ctl, db).await?)),
        ("DELETE", ["v1", "databases", db, "keep-awake"]) => Ok(Response::json(200, &super::pin::unset(ctl, db).await?)),
        ("POST", ["v1", "databases", db, "exports"]) => Ok(Response::json(201, &super::export::create(ctl, db).await?)),
        ("GET", ["v1", "exports", id]) => Ok(Response::json(200, &super::export::get(ctl, id).await?)),
        ("DELETE", ["v1", "exports", id]) => Ok(Response::json(200, &super::export::delete(ctl, id).await?)),
        ("GET", ["v1", "exports", id, "files", rest @ ..]) => super::export::download(ctl, id, &rest.join("/"), &req.headers).await,
        ("GET", ["v1", "databases", db, "restore-points"]) => Ok(Response::json(200, &super::pitr::list_points(ctl, db).await?)),
        ("PUT", ["v1", "databases", db, "retention"]) => {
            let b = body_json(req)?;
            let w = b["window_secs"].as_u64().ok_or_else(|| ApiError::bad("body needs an integer window_secs (0 switches retention off)"))?;
            Ok(Response::json(200, &super::pitr::set_retention(ctl, db, w, b["interval_secs"].as_u64()).await?))
        }
        ("POST", ["v1", "databases", db, "restore"]) => {
            let b = body_json(req)?;
            let to = match &b["to"] {
                Value::String(s) => super::rec::parse_time(s),
                Value::Number(n) => n.as_i64(),
                _ => None,
            }
            .ok_or_else(|| ApiError::bad("body needs `to`: an RFC 3339 time or epoch milliseconds"))?;
            Ok(Response::json(200, &super::pitr::restore(ctl, db, to, b["dry_run"].as_bool().unwrap_or(false)).await?))
        }
        ("PUT", ["v1", "databases", db, "ip-allowlist"]) => Ok(Response::json(200, &super::allowlist::set(ctl, db, &body_json(req)?).await?)),
        ("GET", ["v1", "databases", db, "ip-allowlist"]) => Ok(Response::json(200, &super::allowlist::get(ctl, db).await?)),
        ("DELETE", ["v1", "databases", db, "ip-allowlist"]) => Ok(Response::json(200, &super::allowlist::clear(ctl, db).await?)),
        ("GET", ["v1", "network"]) => Ok(Response::json(200, &super::allowlist::state())),
        ("DELETE", ["v1", "network", "blocks", ip]) => Ok(Response::json(200, &super::allowlist::unblock(ip)?)),
        ("GET", ["v1", "databases", db, "manifest"]) => {
            meta::check_name(db)?;
            let id = req.query.get("id").and_then(|v| v.parse().ok());
            Ok(Response::json(200, &super::purge::manifest_summary(&ctl.store, db, id).await?.unwrap_or(Value::Null)))
        }
        (_, ["v1", ..]) => Err(ApiError::new(404, "no_route", format!("no route for {} {}", req.method, req.path))),
        _ => Err(ApiError::new(404, "no_route", "not found")),
    }
}
