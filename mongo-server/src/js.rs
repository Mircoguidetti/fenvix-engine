//! Server-side JavaScript (`$where`, `$function`) on QuickJS. One engine per worker thread; every call has a
//! time limit. Values cross the boundary as canonical Extended JSON; 64-bit integers become JS numbers.

use crate::error::MongoError;
use bson::{Bson, Document};
use rquickjs::{Context, Runtime};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::time::{Duration, Instant};

type R<T> = Result<T, MongoError>;

const LIMIT: Duration = Duration::from_secs(10);

const PRELUDE: &str = r#"
class ObjectId { constructor(h) { this.str = h; } toString() { return 'ObjectId("' + this.str + '")'; } valueOf() { return this.str; } toHexString() { return this.str; } }
function __rev(k, v) {
  if (v && typeof v === 'object' && !Array.isArray(v)) {
    const ks = Object.keys(v);
    if (ks.length === 1) {
      switch (ks[0]) {
        case '$numberInt': case '$numberLong': case '$numberDouble': case '$numberDecimal': return Number(v[ks[0]]);
        case '$date': return new Date(Number(v.$date.$numberLong !== undefined ? v.$date.$numberLong : v.$date));
        case '$oid': return new ObjectId(v.$oid);
        case '$regularExpression': return new RegExp(v.$regularExpression.pattern, v.$regularExpression.options);
        case '$undefined': return undefined;
        case '$minKey': return null;
        case '$maxKey': return null;
      }
    }
  }
  return v;
}
function __doc(s) { return JSON.parse(s, __rev); }
function __ej(v) {
  if (v === null || v === undefined) return null;
  switch (typeof v) {
    case 'number': return { $numberDouble: Number.isNaN(v) ? 'NaN' : (v === Infinity ? 'Infinity' : (v === -Infinity ? '-Infinity' : String(v))) };
    case 'string': case 'boolean': return v;
    case 'function': return null;
  }
  if (v instanceof Date) return { $date: { $numberLong: String(v.getTime()) } };
  if (v instanceof RegExp) return { $regularExpression: { pattern: v.source, options: v.flags } };
  if (v instanceof ObjectId) return { $oid: v.str };
  if (Array.isArray(v)) return v.map(__ej);
  const o = {};
  for (const k of Object.keys(v)) o[k] = __ej(v[k]);
  return o;
}
"#;

struct Engine {
    _rt: Runtime,
    ctx: Context,
    ids: HashMap<String, usize>,
}

thread_local! {
    static ENGINE: RefCell<Option<Engine>> = const { RefCell::new(None) };
    static DEADLINE: Cell<Option<Instant>> = const { Cell::new(None) };
}

fn js_err(msg: String) -> MongoError {
    MongoError::new(139, "JSInterpreterFailure", msg)
}

fn engine<T>(f: impl FnOnce(&mut Engine) -> R<T>) -> R<T> {
    ENGINE.with(|cell| {
        let mut slot = cell.borrow_mut();
        if slot.is_none() {
            let rt = Runtime::new().map_err(|e| js_err(format!("cannot start the JavaScript engine: {e}")))?;
            rt.set_memory_limit(256 << 20);
            rt.set_max_stack_size(512 << 10);
            rt.set_interrupt_handler(Some(Box::new(|| DEADLINE.with(|d| d.get().map(|t| Instant::now() > t).unwrap_or(false)))));
            let ctx = Context::full(&rt).map_err(|e| js_err(format!("cannot start the JavaScript engine: {e}")))?;
            ctx.with(|c| c.eval::<(), _>(PRELUDE)).map_err(|e| js_err(format!("prelude: {e}")))?;
            *slot = Some(Engine { _rt: rt, ctx, ids: HashMap::new() });
        }
        DEADLINE.with(|d| d.set(Some(Instant::now() + LIMIT)));
        let r = f(slot.as_mut().unwrap());
        DEADLINE.with(|d| d.set(None));
        r
    })
}

/// Function source as MongoDB wraps it: a string that is not a function is the body of one (an expression gets `return`).
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
        self.ctx.with(|c| c.eval::<(), _>(src)).map_err(|e| js_err(format!("SyntaxError: {e}")))?;
        self.ids.insert(code.to_string(), id);
        Ok(id)
    }
}

fn quote(s: &str) -> String {
    serde_json::to_string(s).unwrap()
}

fn ext(b: &Bson) -> String {
    serde_json::to_string(&b.clone().into_canonical_extjson()).unwrap()
}

fn exception(c: &rquickjs::Ctx, e: rquickjs::Error) -> MongoError {
    if let rquickjs::Error::Exception = e {
        let ex = c.catch();
        if !ex.is_object() {
            // `throw 'boom'`: not an Error object
            return MongoError::new(8, "UnknownError", format!("uncaught exception: {ex:?}"));
        }
        let msg = ex.as_exception().map(|x| x.message().unwrap_or_default()).unwrap_or_default();
        return js_err(format!("Error: {msg}"));
    }
    js_err(format!("Error: {e}"))
}

fn code_of(v: &Bson) -> R<String> {
    match v {
        Bson::String(s) => Ok(s.clone()),
        Bson::JavaScriptCode(c) => Ok(c.clone()),
        Bson::JavaScriptCodeWithScope(c) => Ok(c.code.clone()),
        _ => Err(MongoError::new(2, "BadValue", "$where got bad type".into())),
    }
}

/// Does `this` = `doc` satisfy the `$where` code?
pub fn eval_where(code: &Bson, doc: &Document) -> R<bool> {
    let code = code_of(code)?;
    engine(|e| {
        let id = e.compile(&code)?;
        let call = format!("!!__f{id}.call(__doc({}))", quote(&ext(&Bson::Document(doc.clone()))));
        e.ctx.with(|c| c.eval::<bool, _>(call).map_err(|x| exception(&c, x)))
    })
}

/// `$function`: call `body(...args)` and convert the result.
pub fn call_function(body: &str, args: &[Bson]) -> R<Bson> {
    engine(|e| {
        let id = e.compile(body)?;
        let args_json = ext(&Bson::Array(args.to_vec()));
        let call = format!("JSON.stringify(__ej(__f{id}.apply(null, __doc({}))))", quote(&args_json));
        let out: String = e.ctx.with(|c| c.eval::<String, _>(call).map_err(|x| exception(&c, x)))?;
        let v: serde_json::Value = serde_json::from_str(&out).map_err(|x| js_err(format!("bad result: {x}")))?;
        Bson::try_from(v).map_err(|x| js_err(format!("bad result: {x}")))
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
    }

    #[test]
    fn errors_and_limits() {
        assert!(eval_where(&Bson::String("this.a.b.c".into()), &doc! {}).is_err());
        assert!(eval_where(&Bson::String("throw new Error('x')".into()), &doc! {}).is_err());
    }
}
