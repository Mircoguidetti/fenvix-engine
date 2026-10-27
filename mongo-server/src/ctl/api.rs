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
        (_, ["v1", ..]) => Err(ApiError::new(404, "no_route", format!("no route for {} {}", req.method, req.path))),
        _ => Err(ApiError::new(404, "no_route", "not found")),
    }
}
