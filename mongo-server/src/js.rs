//! Server-side JavaScript (`$where`, `$function`, `$accumulator`, `mapReduce`) on QuickJS.
//!
//! * Sandbox: a bare QuickJS context has the language's built-ins only; there is no module loader, file system,
//!   network, process or timer API. The globals MongoDB's JS scope adds (ObjectId, NumberLong, tojson, ...) are defined
//!   by `js_prelude.js`.
//! * Isolation: every command gets its own context, so globals set by one command (or one tenant) are not seen by
//!   another; within a command they persist from one call to the next, as in MongoDB.
//! * Limits per call: `internalQueryJavaScriptFnTimeoutMillis` (default 60 s; exceeding it is error 11601
//!   `Interrupted`), `jsHeapLimitMB` (default 1100; exceeding it is error 139), a stack limit, and the 16 MB limit on a
//!   returned value (error 10334).
//! * Values cross the boundary as JS source text (`push_js`) and as a tagged JSON stream (`parse_ser`) that keeps the
//!   key order of objects and every BSON type.

use crate::error::MongoError;
use bson::{Bson, Document};
use rquickjs::context::EvalOptions;
use rquickjs::{Context, Function, Runtime};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

type R<T> = Result<T, MongoError>;

/// `internalQueryJavaScriptFnTimeoutMillis`
pub static TIMEOUT_MS: AtomicU64 = AtomicU64::new(60_000);
/// `jsHeapLimitMB`
pub static HEAP_MB: AtomicU64 = AtomicU64::new(1100);

const PRELUDE: &str = include_str!("js_prelude.js");
const MAX_BSON: usize = 16_793_600;
const MAX_CONTEXTS: usize = 16;

tokio::task_local! {
    /// Identifies the command being executed, so that it gets a JS context of its own.
    static SCOPE: u64;
}

static NEXT_SCOPE: AtomicU64 = AtomicU64::new(1);

/// Run `fut` (a command) with a JS scope of its own.
pub async fn scoped<F: std::future::Future>(fut: F) -> F::Output {
    SCOPE.scope(NEXT_SCOPE.fetch_add(1, Relaxed), fut).await
}

fn scope_id() -> u64 {
    SCOPE.try_with(|s| *s).unwrap_or(0)
}

struct Engine {
    ctx: Context,
    /// Compiled functions by source text: the global `__f{n}`.
    ids: HashMap<String, usize>,
    used: u64,
}

struct Engines {
    rt: Option<Runtime>,
    by_scope: HashMap<u64, Engine>,
    tick: u64,
}

thread_local! {
    static ENGINES: RefCell<Engines> = RefCell::new(Engines { rt: None, by_scope: HashMap::new(), tick: 0 });
    static DEADLINE: Cell<Option<Instant>> = const { Cell::new(None) };
    static TIMED_OUT: Cell<bool> = const { Cell::new(false) };
}

fn code_name(code: i32) -> &'static str {
    match code {
        2 => "BadValue",
        8 => "UnknownError",
        139 => "JSInterpreterFailure",
        11601 => "Interrupted",
        10334 => "BSONObjectTooLarge",
        31220 => "Location31220",
        _ => "UnknownError",
    }
}

fn js_err(msg: impl Into<String>) -> MongoError {
    MongoError::new(139, "JSInterpreterFailure", msg.into())
}

fn native<T>(r: rquickjs::Result<T>) -> R<T> {
    r.map_err(|e| js_err(format!("cannot start the JavaScript engine: {e}")))
}

fn start(rt: &Runtime) -> R<Context> {
    let ctx = native(Context::full(rt))?;
    ctx.with(|c| -> R<()> {
        let g = c.globals();
        native(g.set("__serverVersion", native(Function::new(c.clone(), || "7.0.14".to_string()))?))?;
        native(g.set("__bsonsize", native(Function::new(c.clone(), |s: String| -> rquickjs::Result<i32> { Ok(parse_ser(&s).ok().map(|b| bson_size(&b)).unwrap_or(0) as i32) }))?))?;
        native(g.set(
            "__bsoncmp",
            native(Function::new(c.clone(), |a: String, b: String| -> rquickjs::Result<i32> {
                match (parse_ser(&a), parse_ser(&b)) {
                    (Ok(x), Ok(y)) => Ok(match crate::query::compare(&x, &y) {
                        std::cmp::Ordering::Less => -1,
                        std::cmp::Ordering::Equal => 0,
                        std::cmp::Ordering::Greater => 1,
                    }),
                    _ => Ok(0),
                }
            }))?,
        ))?;
        native(g.set(
            "__md5",
            native(Function::new(c.clone(), |s: String| -> String {
                use md5::{Digest, Md5};
                Md5::digest(s.as_bytes()).iter().map(|b| format!("{b:02x}")).collect()
            }))?,
        ))?;
        native(g.set(
            "sleep",
            native(Function::new(c.clone(), |ms: f64| -> rquickjs::Result<()> {
                let until = Instant::now() + Duration::from_millis(ms.max(0.0) as u64);
                while Instant::now() < until {
                    if DEADLINE.with(|d| d.get().map(|t| Instant::now() > t).unwrap_or(false)) {
                        TIMED_OUT.with(|t| t.set(true));
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
                Ok(())
            }))?,
        ))?;
        c.eval::<(), _>(PRELUDE).map_err(|e| js_err(format!("prelude: {e}")))?;
        // Built-ins MongoDB's engine does not have.
        c.eval::<(), _>("for (const n of ['Reflect', 'atob', 'btoa', 'SharedArrayBuffer', 'Atomics']) delete globalThis[n];").map_err(|e| js_err(e.to_string()))?;
        Ok(())
    })?;
    Ok(ctx)
}

fn with_engine<T>(f: impl FnOnce(&mut Engine) -> R<T>) -> R<T> {
    let sid = scope_id();
    ENGINES.with(|cell| {
        let mut en = cell.borrow_mut();
        if en.rt.is_none() {
            let rt = native(Runtime::new())?;
            rt.set_max_stack_size(512 << 10);
            rt.set_interrupt_handler(Some(Box::new(|| {
                let over = DEADLINE.with(|d| d.get().map(|t| Instant::now() > t).unwrap_or(false));
                if over {
                    TIMED_OUT.with(|t| t.set(true));
                }
                over
            })));
            en.rt = Some(rt);
        }
        en.tick += 1;
        let tick = en.tick;
        let heap = (HEAP_MB.load(Relaxed) as usize).saturating_mul(1 << 20);
        en.rt.as_ref().unwrap().set_memory_limit(heap);
        if !en.by_scope.contains_key(&sid) {
            if en.by_scope.len() >= MAX_CONTEXTS {
                if let Some(old) = en.by_scope.iter().min_by_key(|(_, e)| e.used).map(|(k, _)| *k) {
                    en.by_scope.remove(&old);
                }
            }
            let ctx = start(en.rt.as_ref().unwrap())?;
            en.by_scope.insert(sid, Engine { ctx, ids: HashMap::new(), used: tick });
        }
        let e = en.by_scope.get_mut(&sid).unwrap();
        e.used = tick;
        set_deadline();
        TIMED_OUT.with(|t| t.set(false));
        let r = f(e);
        DEADLINE.with(|d| d.set(None));
        r
    })
}

fn set_deadline() {
    DEADLINE.with(|d| d.set(Some(Instant::now() + Duration::from_millis(TIMEOUT_MS.load(Relaxed)))));
}

/// Drop the context of the current command (called when it finishes, where possible).
#[allow(dead_code)]
pub fn end_scope() {
    let sid = scope_id();
    ENGINES.with(|cell| {
        if let Ok(mut en) = cell.try_borrow_mut() {
            en.by_scope.remove(&sid);
        }
    });
}

/// Function source as MongoDB wraps it: a string that is not a function is the body of one (an expression gets `return`).
fn sloppy() -> EvalOptions {
    let mut o = EvalOptions::default();
    o.strict = false;
    o.global = true;
    o
}

fn function_source(code: &str) -> String {
    let t = code.trim();
    if t.starts_with("function") || t.starts_with("(") && t.contains("=>") {
        format!("({t})")
    } else if t.contains("return") || t.contains(';') || ["throw", "if", "for", "while", "var ", "let ", "const ", "{"].iter().any(|k| t.starts_with(k)) {
        format!("(function() {{ {t} }})")
    } else {
        format!("(function() {{ return ({t}); }})")
    }
}

impl Engine {
    fn compile(&mut self, code: &str) -> R<usize> {
        if let Some(i) = self.ids.get(code) {
            return Ok(*i);
        }
        let id = self.ids.len();
        let src = format!("globalThis.__f{id} = {};", function_source(code));
        self.ctx.with(|c| c.eval_with_options::<(), _>(src, sloppy()).map_err(|x| exception(&c, x)))?;
        self.ids.insert(code.to_string(), id);
        Ok(id)
    }

    fn eval_str(&self, src: String) -> R<String> {
        self.ctx.with(|c| c.eval_with_options::<String, _>(src, sloppy()).map_err(|x| exception(&c, x)))
    }

    fn run(&self, src: String) -> R<()> {
        self.ctx.with(|c| c.eval_with_options::<(), _>(src, sloppy()).map_err(|x| exception(&c, x)))
    }
}

/// Turn a thrown JS value into a MongoDB error.
fn exception(c: &rquickjs::Ctx, e: rquickjs::Error) -> MongoError {
    if TIMED_OUT.with(|t| t.get()) {
        let _ = c.catch();
        return MongoError::new(11601, "Interrupted", "JavaScript execution interrupted".into());
    }
    match e {
        rquickjs::Error::Exception => {
            let ex = c.catch();
            if !ex.is_object() {
                let shown = if let Some(s) = ex.as_string() {
                    s.to_string().unwrap_or_default()
                } else if let Some(n) = ex.as_float() {
                    n.to_string()
                } else if let Some(n) = ex.as_int() {
                    n.to_string()
                } else {
                    format!("{ex:?}")
                };
                return MongoError::new(8, "UnknownError", format!("uncaught exception: {shown}"));
            }
            if let Some(o) = ex.as_object() {
                if let Ok(code) = o.get::<_, i32>("code") {
                    let msg: String = o.get("message").unwrap_or_default();
                    return MongoError::new(code, code_name(code), msg);
                }
            }
            let (name, msg) = match ex.as_exception() {
                Some(x) => (x.get::<_, String>("name").unwrap_or_else(|_| "Error".into()), x.message().unwrap_or_default()),
                None => ("Error".to_string(), String::new()),
            };
            js_err(format!("{name}: {msg}"))
        }
        rquickjs::Error::Allocation => js_err("Out of memory"),
        other => js_err(format!("Error: {other}")),
    }
}

// ---------------------------------------------------------------------------------------------
// Rust -> JS
// ---------------------------------------------------------------------------------------------

fn quote(s: &str) -> String {
    serde_json::to_string(s).unwrap()
}

fn push_js(b: &Bson, out: &mut String) {
    use std::fmt::Write;
    match b {
        Bson::Double(f) => {
            if f.is_nan() {
                out.push_str("NaN")
            } else if f.is_infinite() {
                out.push_str(if *f > 0.0 { "Infinity" } else { "-Infinity" })
            } else if *f == 0.0 && f.is_sign_negative() {
                out.push_str("-0")
            } else {
                let _ = write!(out, "{f}");
            }
        }
        Bson::Int32(i) => {
            let _ = write!(out, "{i}");
        }
        Bson::Int64(i) => {
            let _ = write!(out, "__nl(\"{i}\")");
        }
        Bson::Decimal128(d) => {
            let _ = write!(out, "__nd({})", quote(&d.to_string()));
        }
        Bson::String(s) | Bson::Symbol(s) => out.push_str(&quote(s)),
        Bson::Boolean(v) => out.push_str(if *v { "true" } else { "false" }),
        Bson::Null => out.push_str("null"),
        Bson::Undefined => out.push_str("undefined"),
        Bson::Document(d) => push_doc(d, out),
        Bson::Array(a) => {
            out.push('[');
            for (i, x) in a.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                push_js(x, out);
            }
            out.push(']');
        }
        Bson::ObjectId(o) => {
            let _ = write!(out, "__oidv(\"{}\")", o.to_hex());
        }
        Bson::DateTime(d) => {
            let ms = d.timestamp_millis();
            if ms.abs() <= 8_640_000_000_000_000 {
                let _ = write!(out, "new Date({ms})");
            } else {
                out.push_str("new Date(NaN)");
            }
        }
        Bson::Timestamp(t) => {
            let _ = write!(out, "__ts({},{})", t.time, t.increment);
        }
        Bson::Binary(b) => {
            let _ = write!(out, "__bd({},\"{}\")", u8::from(b.subtype), base64(&b.bytes));
        }
        Bson::RegularExpression(r) => {
            let flags: String = r.options.as_str().chars().filter(|c| matches!(c, 'i' | 'm' | 's')).collect();
            let _ = write!(out, "__re({},{})", quote(r.pattern.as_str()), quote(&flags));
        }
        Bson::MinKey => out.push_str("__minKey"),
        Bson::MaxKey => out.push_str("__maxKey"),
        Bson::JavaScriptCode(c) => {
            let _ = write!(out, "__fn({})", quote(c));
        }
        Bson::JavaScriptCodeWithScope(c) => {
            let _ = write!(out, "__fn({})", quote(&c.code));
        }
        Bson::DbPointer(_) => out.push_str("null"),
    }
}

fn push_doc(d: &Document, out: &mut String) {
    // Int32 members stay integers when the object is handed back untouched, so the object remembers which they are.
    let ints: Vec<&String> = d.iter().filter(|(_, v)| matches!(v, Bson::Int32(_))).map(|(k, _)| k).collect();
    if !ints.is_empty() {
        out.push_str("__bo(");
        push_doc_plain(d, out);
        out.push_str(",[");
        for (i, k) in ints.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            out.push_str(&quote(k));
        }
        out.push_str("])");
        return;
    }
    push_doc_plain(d, out);
}

fn push_doc_plain(d: &Document, out: &mut String) {
    if d.keys().any(|k| k == "__proto__") {
        out.push_str("__mk([");
        for (i, (k, v)) in d.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            out.push('[');
            out.push_str(&quote(k));
            out.push(',');
            push_js(v, out);
            out.push(']');
        }
        out.push_str("])");
        return;
    }
    out.push('{');
    for (i, (k, v)) in d.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&quote(k));
        out.push(':');
        push_js(v, out);
    }
    out.push('}');
}

fn js_of(b: &Bson) -> String {
    let mut s = String::new();
    push_js(b, &mut s);
    s
}

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn base64(bytes: &[u8]) -> String {
    let mut s = String::new();
    for ch in bytes.chunks(3) {
        let n = (ch[0] as u32) << 16 | (*ch.get(1).unwrap_or(&0) as u32) << 8 | *ch.get(2).unwrap_or(&0) as u32;
        s.push(B64[(n >> 18) as usize & 63] as char);
        s.push(B64[(n >> 12) as usize & 63] as char);
        s.push(if ch.len() > 1 { B64[(n >> 6) as usize & 63] as char } else { '=' });
        s.push(if ch.len() > 2 { B64[n as usize & 63] as char } else { '=' });
    }
    s
}

fn unbase64(s: &str) -> Vec<u8> {
    let mut out = Vec::new();
    let (mut acc, mut bits) = (0u32, 0);
    for c in s.bytes().filter(|c| *c != b'=') {
        let v = B64.iter().position(|x| *x == c).unwrap_or(0) as u32;
        acc = (acc << 6) | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    out
}

// ---------------------------------------------------------------------------------------------
// JS -> Rust: the tagged JSON stream written by `__ser`
// ---------------------------------------------------------------------------------------------

struct P<'a> {
    s: &'a [u8],
    i: usize,
}

impl<'a> P<'a> {
    fn ws(&mut self) {
        while self.i < self.s.len() && matches!(self.s[self.i], b' ' | b'\n' | b'\t' | b'\r') {
            self.i += 1;
        }
    }

    fn bad(&self) -> MongoError {
        js_err("cannot convert the JavaScript result to BSON")
    }

    fn string(&mut self) -> R<String> {
        self.i += 1; // opening quote
        let mut out: Vec<u16> = Vec::new();
        let mut buf = Vec::new();
        let flush = |buf: &mut Vec<u8>, out: &mut Vec<u16>| {
            if !buf.is_empty() {
                out.extend(String::from_utf8_lossy(buf).encode_utf16());
                buf.clear();
            }
        };
        loop {
            let c = *self.s.get(self.i).ok_or_else(|| self.bad())?;
            self.i += 1;
            match c {
                b'"' => break,
                b'\\' => {
                    flush(&mut buf, &mut out);
                    let e = *self.s.get(self.i).ok_or_else(|| self.bad())?;
                    self.i += 1;
                    match e {
                        b'n' => out.push(10),
                        b't' => out.push(9),
                        b'r' => out.push(13),
                        b'b' => out.push(8),
                        b'f' => out.push(12),
                        b'/' => out.push(b'/' as u16),
                        b'\\' => out.push(b'\\' as u16),
                        b'"' => out.push(b'"' as u16),
                        b'u' => {
                            let h = std::str::from_utf8(self.s.get(self.i..self.i + 4).ok_or_else(|| self.bad())?).map_err(|_| self.bad())?;
                            out.push(u16::from_str_radix(h, 16).map_err(|_| self.bad())?);
                            self.i += 4;
                        }
                        _ => return Err(self.bad()),
                    }
                }
                other => buf.push(other),
            }
        }
        flush(&mut buf, &mut out);
        Ok(String::from_utf16_lossy(&out))
    }

    fn value(&mut self) -> R<Bson> {
        self.ws();
        match *self.s.get(self.i).ok_or_else(|| self.bad())? {
            b'n' => {
                self.i += 4;
                Ok(Bson::Null)
            }
            b't' => {
                self.i += 4;
                Ok(Bson::Boolean(true))
            }
            b'f' => {
                self.i += 5;
                Ok(Bson::Boolean(false))
            }
            b'"' => Ok(Bson::String(self.string()?)),
            b'[' => {
                self.i += 1;
                let mut a = Vec::new();
                loop {
                    self.ws();
                    if self.s.get(self.i) == Some(&b']') {
                        self.i += 1;
                        break;
                    }
                    if !a.is_empty() {
                        if self.s.get(self.i) != Some(&b',') {
                            return Err(self.bad());
                        }
                        self.i += 1;
                    }
                    a.push(self.value()?);
                }
                Ok(Bson::Array(a))
            }
            b'{' => {
                self.i += 1;
                let mut entries: Vec<(String, Bson)> = Vec::new();
                loop {
                    self.ws();
                    if self.s.get(self.i) == Some(&b'}') {
                        self.i += 1;
                        break;
                    }
                    if !entries.is_empty() {
                        if self.s.get(self.i) != Some(&b',') {
                            return Err(self.bad());
                        }
                        self.i += 1;
                        self.ws();
                    }
                    let k = self.string()?;
                    self.ws();
                    if self.s.get(self.i) != Some(&b':') {
                        return Err(self.bad());
                    }
                    self.i += 1;
                    entries.push((k, self.value()?));
                }
                if entries.first().map(|(k, _)| k == "\u{1}").unwrap_or(false) {
                    return tagged(&entries).ok_or_else(|| self.bad());
                }
                let mut d = Document::new();
                for (k, v) in entries {
                    d.insert(k, v);
                }
                Ok(Bson::Document(d))
            }
            _ => {
                let st = self.i;
                while self.i < self.s.len() && !matches!(self.s[self.i], b',' | b']' | b'}' | b' ') {
                    self.i += 1;
                }
                let t = std::str::from_utf8(&self.s[st..self.i]).map_err(|_| self.bad())?;
                Ok(Bson::Double(t.parse::<f64>().map_err(|_| self.bad())?))
            }
        }
    }
}

fn tagged(e: &[(String, Bson)]) -> Option<Bson> {
    let get = |k: &str| e.iter().find(|(n, _)| n == k).map(|(_, v)| v);
    let s = |k: &str| get(k).and_then(|v| v.as_str());
    let tag = get("\u{1}")?.as_str()?;
    Some(match tag {
        "d" => Bson::Double(match s("v")? {
            "NaN" => f64::NAN,
            "Infinity" => f64::INFINITY,
            "-Infinity" => f64::NEG_INFINITY,
            "-0" => -0.0,
            other => other.parse().ok()?,
        }),
        "c" => Bson::JavaScriptCode(s("v")?.to_string()),
        "min" => Bson::MinKey,
        "max" => Bson::MaxKey,
        "t" => Bson::DateTime(bson::DateTime::from_millis(s("v")?.parse().ok()?)),
        "r" => Bson::RegularExpression(bson::Regex { pattern: s("p")?.to_string().try_into().ok()?, options: s("f")?.to_string().try_into().ok()? }),
        "o" => Bson::ObjectId(bson::oid::ObjectId::parse_str(s("v")?).ok()?),
        "l" => Bson::Int64(s("v")?.parse().ok()?),
        "i" => Bson::Int32(s("v")?.parse().ok()?),
        "D" => Bson::Decimal128(s("v")?.parse().ok()?),
        "ts" => {
            let n = |k: &str| get(k).and_then(|v| v.as_f64()).map(|f| f as u32);
            Bson::Timestamp(bson::Timestamp { time: n("t")?, increment: n("i")? })
        }
        "b" => Bson::Binary(bson::Binary { subtype: bson::spec::BinarySubtype::from(get("s").and_then(|v| v.as_f64())? as u8), bytes: unbase64(s("v")?) }),
        _ => return None,
    })
}

fn parse_ser(s: &str) -> R<Bson> {
    let mut p = P { s: s.as_bytes(), i: 0 };
    p.value()
}

fn bson_size(b: &Bson) -> usize {
    match b {
        Bson::Document(d) => { let mut v = Vec::new(); d.to_writer(&mut v).map(|_| v.len()).unwrap_or(0) }
        other => {
            let mut d = Document::new();
            d.insert("v", other.clone());
            let mut v = Vec::new();
            d.to_writer(&mut v).map(|_| v.len()).unwrap_or(0)
        }
    }
}

fn result(text: String) -> R<Bson> {
    if text.len() > MAX_BSON {
        return Err(MongoError::new(10334, "BSONObjectTooLarge", format!("BSONObj size: {} (0x{:X}) is invalid. Size must be between 0 and {}(16MB)", text.len() + 4, text.len() + 4, MAX_BSON)));
    }
    parse_ser(&text)
}

// ---------------------------------------------------------------------------------------------
// $where, $function
// ---------------------------------------------------------------------------------------------

/// The code of a `$where`: strings and code only, not empty.
pub fn where_code(v: &Bson) -> R<String> {
    let code = match v {
        Bson::String(s) => s.clone(),
        Bson::JavaScriptCode(c) => c.clone(),
        Bson::JavaScriptCodeWithScope(_) => return Err(MongoError::new(4649201, "Location4649201", "$where no longer supports deprecated BSON type CodeWScope".into())),
        _ => return Err(MongoError::new(2, "BadValue", "$where got bad type".into())),
    };
    if code.is_empty() {
        return Err(MongoError::new(2, "BadValue", "code for $where cannot be empty".into()));
    }
    Ok(code)
}

fn has_where(d: &Document) -> bool {
    d.iter().any(|(k, v)| {
        k == "$where"
            || match v {
                Bson::Document(x) => has_where(x),
                Bson::Array(a) => a.iter().any(|y| matches!(y, Bson::Document(x) if has_where(x))),
                _ => false,
            }
    })
}

/// Where a filter may use `$where`: at the top level (also inside `$and` / `$or` / `$nor`), with a non-empty string or
/// code; MongoDB rejects it on a field and inside `$elemMatch` when the query is parsed, so does this.
pub fn validate_filter(filter: &Document) -> R<()> {
    for (k, v) in filter {
        match (k.as_str(), v) {
            ("$where", code) => {
                where_code(code)?;
            }
            ("$and" | "$or" | "$nor", Bson::Array(a)) => {
                for s in a {
                    if let Bson::Document(d) = s {
                        validate_filter(d)?;
                    }
                }
            }
            (k, Bson::Document(cond)) if !k.starts_with('$') => {
                for (op, arg) in cond {
                    if op == "$where" || (op == "$not" && matches!(arg, Bson::Document(x) if has_where(x))) {
                        return Err(MongoError::new(2, "BadValue", "$where cannot be applied to a field".into()));
                    }
                    if op == "$elemMatch" && matches!(arg, Bson::Document(x) if has_where(x)) {
                        return Err(MongoError::new(2, "BadValue", "$where can only be applied to the top-level document".into()));
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// Does `this` = `doc` satisfy the `$where` code?
pub fn eval_where(code: &Bson, doc: &Document) -> R<bool> {
    let code = where_code(code)?;
    with_engine(|e| {
        let id = e.compile(&code)?;
        let mut call = String::from("globalThis.obj = ");
        push_doc(doc, &mut call);
        call.push_str(&format!(";!!__f{id}.call(obj)"));
        e.ctx.with(|c| c.eval_with_options::<bool, _>(call, sloppy()).map_err(|x| exception(&c, x)))
    })
}

/// `$function` / `$accumulator`: call `body(...args)` and convert the result.
pub fn call_function(body: &str, args: &[Bson]) -> R<Bson> {
    with_engine(|e| {
        let id = e.compile(body)?;
        let list: Vec<String> = args.iter().map(js_of).collect();
        let call = format!("__ser(__f{id}.apply({{}}, [{}]))", list.join(","));
        result(e.eval_str(call)?)
    })
}

// ---------------------------------------------------------------------------------------------
// mapReduce
// ---------------------------------------------------------------------------------------------

/// Run `map` over `docs`, group the emitted values by key, reduce (a key with one value is not reduced) and finalize.
/// The result is `[{_id: key, value}]`.
pub fn map_reduce(map: &str, reduce: &str, finalize: Option<&str>, scope: &Document, docs: &[Document]) -> R<Vec<Document>> {
    with_engine(|e| {
        for (k, v) in scope {
            if !k.is_empty() && k != "__proto__" {
                e.run(format!("globalThis[{}] = {};", quote(k), js_of(v)))?;
            }
        }
        let mapid = e.compile(map)?;
        let redid = e.compile(reduce)?;
        let finid = match finalize {
            Some(f) => Some(e.compile(f)?),
            None => None,
        };
        e.run("__mr_begin();".into())?;
        for chunk in docs.chunks(200) {
            set_deadline();
            let mut src = String::from("for (const d of [");
            for (i, d) in chunk.iter().enumerate() {
                if i > 0 {
                    src.push(',');
                }
                push_doc(d, &mut src);
            }
            src.push_str(&format!("]) __mr_map(__f{mapid}, d);"));
            e.run(src)?;
        }
        set_deadline();
        let fin = finid.map(|i| format!("__f{i}")).unwrap_or_else(|| "null".into());
        let text = e.eval_str(format!("__mr_results(__mr_reduce(__f{redid}, {fin}))"))?;
        match result(text)? {
            Bson::Array(a) => Ok(a.into_iter().filter_map(|b| if let Bson::Document(d) = b { Some(d) } else { None }).collect()),
            _ => Err(js_err("mapReduce produced no result")),
        }
    })
}

/// Reduce two values of one key (the `reduce` output action): `reduce(key, [old, new])`, then `finalize`.
pub fn reduce_pair(reduce: &str, finalize: Option<&str>, key: &Bson, old: &Bson, new: &Bson) -> R<Bson> {
    with_engine(|e| {
        let redid = e.compile(reduce)?;
        let mut src = format!("var __r = __f{redid}({}, [{}, {}]);", js_of(key), js_of(old), js_of(new));
        if let Some(f) = finalize {
            let fid = e.compile(f)?;
            src.push_str(&format!("__r = __f{fid}({}, __r);", js_of(key)));
        }
        src.push_str("__ser(__r)");
        result(e.eval_str(src)?)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    #[test]
    fn where_expression_and_function() {
        let d = doc! { "a": 5, "s": "hi", "arr": [1, 2, 3] };
        assert!(eval_where(&Bson::String("this.a > 4".into()), &d).unwrap());
        assert!(!eval_where(&Bson::String("this.a > 9".into()), &d).unwrap());
        assert!(eval_where(&Bson::String("function() { return this.arr.length == 3 && this.s == 'hi'; }".into()), &d).unwrap());
        assert!(eval_where(&Bson::String("return this.a == 5".into()), &d).unwrap());
    }

    #[test]
    fn function_returns_bson() {
        let r = call_function("function(a, b) { return a + b; }", &[Bson::Int32(1), Bson::Int32(2)]).unwrap();
        assert_eq!(r, Bson::Double(3.0));
        let r = call_function("function(x) { return {k: [x, 'y'], d: new Date(5)}; }", &[Bson::Int32(7)]).unwrap();
        assert_eq!(r.as_document().unwrap().get_array("k").unwrap()[1], Bson::String("y".into()));
        let r = call_function("function(l) { return NumberLong(l + 1); }", &[Bson::Int64(4)]).unwrap();
        assert_eq!(r, Bson::Int64(5));
    }

    #[test]
    fn key_order_is_kept() {
        let r = call_function("function(o) { return o; }", &[Bson::Document(doc! { "b": 1, "a": 2, "c": { "z": 1, "y": 2 } })]).unwrap();
        let keys: Vec<&String> = r.as_document().unwrap().keys().collect();
        assert_eq!(keys, ["b", "a", "c"]);
    }

    #[test]
    fn errors_and_limits() {
        assert!(eval_where(&Bson::String("this.a.b.c".into()), &doc! {}).is_err());
        assert!(eval_where(&Bson::String("throw new Error('x')".into()), &doc! {}).is_err());
        assert_eq!(eval_where(&Bson::String("throw 'boom'".into()), &doc! {}).unwrap_err().code, 8);
    }

    #[test]
    fn map_reduce_groups() {
        let docs: Vec<Document> = (1..=6).map(|i| doc! { "k": i % 2, "v": i }).collect();
        let r = map_reduce("function() { emit(this.k, this.v) }", "function(k, v) { return Array.sum(v) }", None, &Document::new(), &docs).unwrap();
        assert_eq!(r.len(), 2);
    }
}
