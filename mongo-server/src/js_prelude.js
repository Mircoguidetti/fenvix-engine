// Global environment of server-side JavaScript: the types and helpers MongoDB's JS scope offers (ObjectId, NumberLong,
// tojson, Array.sum, ...) on top of a bare QuickJS context. No file system, network or process access exists: the
// context has only the language's own built-ins.
(function (g) {
  'use strict';
  const NativeMap = g.Map;
  const Refl = g.Reflect;
  const hide = (o, k, v) => Object.defineProperty(o, k, { value: v, writable: true, configurable: true, enumerable: false });
  const show = (o, k, v) => Object.defineProperty(o, k, { value: v, writable: true, configurable: true, enumerable: true });

  // Errors that carry a MongoDB error code back to the server.
  class MongoJsError extends Error {
    constructor(code, msg) { super(msg); this.code = code; }
  }
  hide(g, '__MongoJsError', MongoJsError);

  // ---------------------------------------------------------------------------------------------------------------
  // BSON types
  // ---------------------------------------------------------------------------------------------------------------
  const longs = new WeakMap();
  const MAXL = (1n << 63n) - 1n, MINL = -(1n << 63n);
  const bad = (msg) => new MongoJsError(2, msg);
  function NumberLong(a, b, c) {
    if (!(this instanceof NumberLong)) return new NumberLong(...arguments);
    const n = arguments.length;
    let v;
    if (n === 0) v = 0n;
    else if (n === 1) {
      if (typeof a === 'string') {
        if (!/^-?\d+$/.test(a)) throw bad('could not convert string to long long');
        v = BigInt(a);
        if (v > MAXL || v < MINL) throw bad('could not convert string to long long');
      } else if (typeof a === 'number') {
        if (!Number.isInteger(a) || a >= 9223372036854775808 || a < -9223372036854775808) throw bad('number passed to NumberLong must be representable as an int64_t');
        v = BigInt(a);
      } else if (a instanceof NumberLong) v = longs.get(a);
      else throw bad('NumberLong needs a number or string');
    } else if (n === 3) {
      v = (BigInt.asIntN(32, BigInt(Math.trunc(Number(b)))) << 32n) | BigInt.asUintN(32, BigInt(Math.trunc(Number(c))));
    } else throw bad('NumberLong needs 0, 1 or 3 arguments');
    longs.set(this, v);
  }
  const LP = NumberLong.prototype;
  hide(LP, 'toNumber', function () { return Number(longs.get(this)); });
  hide(LP, 'valueOf', function () { return Number(longs.get(this)); });
  hide(LP, 'toString', function () {
    const v = longs.get(this);
    return 'NumberLong(' + (v > 9007199254740991n || v < -9007199254740991n ? '"' + v + '"' : String(v)) + ')';
  });
  hide(LP, 'tojson', function () { return this.toString(); });
  hide(LP, 'toJSON', function () { return { $numberLong: String(longs.get(this)) }; });
  hide(LP, 'compare', function (o) { const a = longs.get(this), b = longs.get(o); return a < b ? -1 : a > b ? 1 : 0; });
  hide(LP, 'floatApprox', 0);
  hide(LP, 'top', 0);
  hide(LP, 'bottom', 0);
  Object.defineProperty(LP, 'floatApprox', { get() { return Number(longs.get(this)); }, configurable: true });
  Object.defineProperty(LP, 'top', { get() { return Number(longs.get(this) >> 32n); }, configurable: true });
  Object.defineProperty(LP, 'bottom', { get() { return Number(BigInt.asUintN(32, longs.get(this))); }, configurable: true });
  hide(g, 'NumberLong', NumberLong);
  hide(g, '__longValue', (o) => longs.get(o));

  const ints = new WeakMap();
  function NumberInt(v) {
    if (!(this instanceof NumberInt)) return new NumberInt(v);
    ints.set(this, Number(v) | 0);
  }
  hide(NumberInt.prototype, 'toJSON', function () { return ints.get(this); });
  hide(NumberInt.prototype, 'valueOf', function () { return ints.get(this); });
  hide(NumberInt.prototype, 'toNumber', function () { return ints.get(this); });
  hide(NumberInt.prototype, 'toString', function () { return 'NumberInt(' + ints.get(this) + ')'; });
  hide(NumberInt.prototype, 'tojson', function () { return this.toString(); });
  hide(g, 'NumberInt', NumberInt);
  hide(g, '__intValue', (o) => ints.get(o));

  const decs = new WeakMap();
  // a double as MongoDB converts it to Decimal128: 15 significant digits
  function dec15(x) {
    if (x === 0) return '0';
    if (!Number.isFinite(x)) return x > 0 ? 'Infinity' : x < 0 ? '-Infinity' : 'NaN';
    const m = /^(-?)(\d)\.(\d+)e([+-]\d+)$/.exec(x.toExponential(14));
    const coef = m[2] + m[3], e = parseInt(m[4], 10);
    const exp10 = e - 14;
    let s;
    if (exp10 <= 0 && e >= -6) {
      const point = coef.length + exp10;
      s = point > 0 ? (exp10 === 0 ? coef : coef.slice(0, point) + '.' + coef.slice(point)) : '0.' + '0'.repeat(-point) + coef;
    } else s = coef[0] + '.' + coef.slice(1) + 'E' + (e >= 0 ? '+' : '') + e;
    return m[1] + s;
  }
  function NumberDecimal(s) {
    if (!(this instanceof NumberDecimal)) return new NumberDecimal(s);
    if (s === undefined) s = '0';
    else if (typeof s === 'number') s = dec15(s);
    else if (typeof s !== 'string' || !/^[+-]?((\d+\.?\d*|\.\d+)([eE][+-]?\d+)?|inf(inity)?|nan)$/i.test(s)) throw bad('Input is not a valid Decimal128 value.');
    decs.set(this, String(s));
  }
  hide(NumberDecimal.prototype, 'toString', function () { return 'NumberDecimal("' + decs.get(this) + '")'; });
  hide(NumberDecimal.prototype, 'tojson', function () { return this.toString(); });
  hide(NumberDecimal.prototype, 'toJSON', function () { return { $numberDecimal: decs.get(this) }; });
  hide(g, 'NumberDecimal', NumberDecimal);
  hide(g, '__decValue', (o) => decs.get(o));

  let oidCounter = Math.floor(Math.random() * 0xffffff);
  const oidRand = Array.from({ length: 5 }, () => Math.floor(Math.random() * 256).toString(16).padStart(2, '0')).join('');
  function newOid() {
    const t = Math.floor(Date.now() / 1000).toString(16).padStart(8, '0');
    oidCounter = (oidCounter + 1) & 0xffffff;
    return t + oidRand + oidCounter.toString(16).padStart(6, '0');
  }
  function ObjectId(s) {
    if (!(this instanceof ObjectId)) return new ObjectId(s);
    if (s === undefined) s = newOid();
    if (typeof s !== 'string') s = String(s);
    if (s.length !== 24) throw new Error('invalid object id: length');
    if (!/^[0-9a-fA-F]{24}$/.test(s)) throw new Error('invalid object id: not hex');
    show(this, 'str', s.toLowerCase());
  }
  hide(ObjectId.prototype, 'toString', function () { return 'ObjectId("' + this.str + '")'; });
  hide(ObjectId.prototype, 'tojson', function () { return this.toString(); });
  hide(ObjectId.prototype, 'valueOf', function () { return this.str; });
  hide(ObjectId.prototype, 'toJSON', function () { return { $oid: this.str }; });
  hide(ObjectId.prototype, 'equals', function (o) { return this.str == o.str; });
  hide(ObjectId.prototype, 'getTimestamp', function () { return new Date(parseInt(this.str.substr(0, 8), 16) * 1000); });
  hide(ObjectId, 'fromDate', function (d) { return new ObjectId(Math.floor(d.getTime() / 1000).toString(16).padStart(8, '0') + '0000000000000000'); });
  hide(g, 'ObjectId', ObjectId);

  function Timestamp(t, i) {
    if (!(this instanceof Timestamp)) return new Timestamp(t, i);
    if (arguments.length !== 0 && arguments.length !== 2) throw bad('Timestamp needs 0 or 2 arguments');
    t = t === undefined ? 0 : Math.trunc(Number(t));
    i = i === undefined ? 0 : Math.trunc(Number(i));
    if (!(t >= 0 && t <= 4294967295)) throw bad('Timestamp time (seconds) must be non-negative and not greater than 4294967295, got ' + t);
    if (!(i >= 0 && i <= 4294967295)) throw bad('Timestamp increment must be non-negative and not greater than 4294967295, got ' + i);
    show(this, 't', t);
    show(this, 'i', i);
  }
  hide(Timestamp.prototype, 'getTime', function () { return this.t; });
  hide(Timestamp.prototype, 'getInc', function () { return this.i; });
  hide(Timestamp.prototype, 'toString', function () { return 'Timestamp(' + this.t + ', ' + this.i + ')'; });
  hide(Timestamp.prototype, 'tojson', function () { return this.toString(); });
  hide(Timestamp.prototype, 'toJSON', function () { return { $timestamp: { t: this.t, i: this.i } }; });
  hide(g, 'Timestamp', Timestamp);

  const bins = new WeakMap();
  const b64chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/';
  function b64decode(s) {
    const out = [];
    let acc = 0, bits = 0;
    for (const c of s.replace(/=+$/, '')) {
      const v = b64chars.indexOf(c);
      if (v < 0) throw new Error('invalid base64 character');
      acc = (acc << 6) | v; bits += 6;
      if (bits >= 8) { bits -= 8; out.push((acc >> bits) & 255); }
    }
    return out;
  }
  function b64encode(bytes) {
    let s = '';
    for (let i = 0; i < bytes.length; i += 3) {
      const n = (bytes[i] << 16) | ((bytes[i + 1] || 0) << 8) | (bytes[i + 2] || 0);
      s += b64chars[(n >> 18) & 63] + b64chars[(n >> 12) & 63] + (i + 1 < bytes.length ? b64chars[(n >> 6) & 63] : '=') + (i + 2 < bytes.length ? b64chars[n & 63] : '=');
    }
    return s;
  }
  const hexOf = (bytes) => bytes.map((b) => b.toString(16).padStart(2, '0')).join('');
  function BinData(subtype, b64) {
    if (!(this instanceof BinData)) return new BinData(subtype, b64);
    if (arguments.length !== 2) throw bad('BinData takes 2 arguments -- BinData(subtype,data)');
    if (typeof subtype !== 'number' || !(subtype >= 0 && subtype <= 255)) throw bad('BinData subtype must be a Number between 0 and 255 inclusive');
    if (typeof b64 !== 'string' || !/^[A-Za-z0-9+/]*={0,2}$/.test(b64) || b64.length % 4 === 1) throw bad('invalid base64');
    bins.set(this, { subtype: Math.trunc(subtype), bytes: b64decode(b64) });
  }
  hide(BinData.prototype, 'subtype', function () { return bins.get(this).subtype; });
  hide(BinData.prototype, 'length', function () { return bins.get(this).bytes.length; });
  hide(BinData.prototype, 'base64', function () { return b64encode(bins.get(this).bytes); });
  hide(BinData.prototype, 'hex', function () { return hexOf(bins.get(this).bytes); });
  hide(BinData.prototype, 'toString', function () { return 'BinData(' + this.subtype() + ',"' + this.base64() + '")'; });
  hide(BinData.prototype, 'tojson', function () { return this.toString(); });
  hide(BinData.prototype, 'toJSON', function () { return { $binary: this.base64(), $type: this.subtype().toString(16).padStart(2, '0') }; });
  hide(g, 'BinData', BinData);
  hide(g, '__binValue', (o) => bins.get(o));
  hide(g, 'HexData', function (subtype, hex) {
    if (typeof hex !== 'string' || hex.length % 2 !== 0) throw bad('Hex string must have even length');
    const bad1 = hex.match(/[^0-9a-fA-F]/);
    if (bad1) throw bad('The character \\x' + bad1[0].charCodeAt(0).toString(16) + ' failed to parse from hex.');
    const bytes = [];
    for (let i = 0; i < hex.length; i += 2) bytes.push(parseInt(hex.substr(i, 2), 16));
    return new BinData(subtype, b64encode(bytes));
  });
  hide(g, 'UUID', function (s) {
    if (s === undefined) {
      const r = Array.from({ length: 16 }, () => Math.floor(Math.random() * 256));
      r[6] = (r[6] & 0x0f) | 0x40; r[8] = (r[8] & 0x3f) | 0x80;
      s = hexOf(r);
    }
    const hex = String(s).replace(/-/g, '');
    if (!/^[0-9a-fA-F]{32}$/.test(hex) || (String(s).includes('-') && !/^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/.test(String(s)))) throw bad('Invalid UUID string: ' + s);
    const bytes = [];
    for (let i = 0; i < 32; i += 2) bytes.push(parseInt(hex.substr(i, 2), 16));
    const b = new BinData(4, b64encode(bytes));
    const h = hexOf(bytes);
    hide(b, 'toString', function () { return 'UUID("' + h.slice(0, 8) + '-' + h.slice(8, 12) + '-' + h.slice(12, 16) + '-' + h.slice(16, 20) + '-' + h.slice(20) + '")'; });
    hide(b, 'tojson', b.toString);
    return b;
  });
  hide(g, 'MD5', function () { throw bad('MD5 is not supported'); });

  function MinKey() { return MinKey; }
  function MaxKey() { return MaxKey; }
  hide(MinKey, 'toString', function () { return '[object MinKey]'; });
  hide(MaxKey, 'toString', function () { return '[object MaxKey]'; });
  hide(MinKey, 'toJSON', function () { return { $minKey: 1 }; });
  hide(MaxKey, 'toJSON', function () { return { $maxKey: 1 }; });
  hide(MinKey, 'tojson', function () { return '{ "$minKey" : 1 }'; });
  hide(MaxKey, 'tojson', function () { return '{ "$maxKey" : 1 }'; });
  const minKey = MinKey, maxKey = MaxKey;
  hide(g, 'MinKey', MinKey);
  hide(g, 'MaxKey', MaxKey);

  function DBRef(ns, id, db) {
    if (!(this instanceof DBRef)) return new DBRef(ns, id, db);
    show(this, '$ref', ns);
    show(this, '$id', id);
    if (db !== undefined) show(this, '$db', db);
  }
  hide(DBRef.prototype, 'getRef', function () { return this.$ref; });
  hide(DBRef.prototype, 'getId', function () { return this.$id; });
  hide(DBRef.prototype, 'getDb', function () { return this.$db; });
  hide(DBRef.prototype, 'getCollection', function () { return this.$ref; });
  hide(DBRef.prototype, 'toString', function () { return 'DBRef(' + tojson(this.$ref) + ', ' + tojson(this.$id) + (this.$db !== undefined ? ', ' + tojson(this.$db) : '') + ')'; });
  hide(DBRef.prototype, 'tojson', function () { return this.toString(); });
  hide(g, 'DBRef', DBRef);
  hide(g, 'DBPointer', function DBPointer(ns, id) { if (!(this instanceof DBPointer)) return new DBPointer(ns, id); show(this, 'ns', ns); show(this, 'id', id); });
  hide(g, 'Code', function Code(code, scope) { if (!(this instanceof Code)) return new Code(code, scope); show(this, 'code', code); if (scope !== undefined) show(this, 'scope', scope); });

  hide(g, 'ISODate', function ISODate(s) {
    if (s === undefined) return new Date();
    const m = /^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,3}))?)?)?(Z|[+-]\d{2}:?\d{2})?$/.exec(String(s));
    if (!m) throw new Error('invalid ISO date: ' + s);
    const d = new Date(Date.UTC(+m[1], +m[2] - 1, +m[3], +(m[4] || 0), +(m[5] || 0), +(m[6] || 0), +((m[7] || '0') + '00').slice(0, 3)));
    if (m[8] && m[8] !== 'Z') {
      const sign = m[8][0] === '-' ? -1 : 1, hh = +m[8].substr(1, 2), mm = +m[8].slice(-2);
      d.setTime(d.getTime() - sign * (hh * 60 + mm) * 60000);
    }
    return d;
  });

  // ---------------------------------------------------------------------------------------------------------------
  // tojson and printing
  // ---------------------------------------------------------------------------------------------------------------
  function quote(s) {
    return '"' + s.replace(/[\\"\u0000-\u001f]/g, (c) => ({ '"': '\\"', '\\': '\\\\', '\b': '\\b', '\f': '\\f', '\n': '\\n', '\r': '\\r', '\t': '\\t' }[c] || '\\u' + c.charCodeAt(0).toString(16).padStart(4, '0'))) + '"';
  }
  function tojsonObject(x, indent, nolint, depth) {
    const le = nolint ? ' ' : '\n', tab = nolint ? '' : '\t';
    if (typeof x.tojson === 'function' && x.tojson !== tojson) return x.tojson(indent, nolint);
    if (x instanceof Error) return x.toString();
    const keys = Object.keys(x);
    if (keys.length === 0) return '{ }';
    let s = '{' + le;
    indent += tab;
    let n = 1;
    for (const k of keys) {
      s += indent + '"' + k + '" : ' + tojson(x[k], indent, nolint, depth + 1);
      if (n !== keys.length) { s += ','; n++; }
      s += le;
    }
    indent = indent.substring(1);
    return s + indent + '}';
  }
  function tojsonArray(a, indent, nolint, depth) {
    if (a.length === 0) return '[ ]';
    const le = nolint ? ' ' : '\n';
    const tab = nolint ? '' : '\t';
    let s = '[' + le;
    indent += tab;
    for (let i = 0; i < a.length; i++) {
      s += indent + tojson(a[i], indent, nolint, depth + 1);
      if (i < a.length - 1) s += ',';
      s += le;
    }
    indent = indent.substring(1);
    return s + indent + ']';
  }
  function tojson(x, indent, nolint, depth) {
    if (x === null) return 'null';
    if (x === undefined) return 'undefined';
    depth = depth || 0;
    const top = indent === undefined;
    if (!indent) indent = '';
    if (nolint === undefined && top && depth === 0) nolint = true;
    switch (typeof x) {
      case 'string': return quote(x);
      case 'number': case 'boolean': return '' + x;
      case 'function': return x === minKey ? '{ "$minKey" : 1 }' : x === maxKey ? '{ "$maxKey" : 1 }' : x.toString();
      case 'object': {
        if (x instanceof Date) return isNaN(x.getTime()) ? 'new Date(NaN)' : 'ISODate("' + x.toISOString().replace('.000Z', 'Z') + '")';
        if (x instanceof RegExp) return x.toString();
        if (Array.isArray(x)) {
          const s = tojsonArray(x, indent, nolint, depth);
          return nolint && depth === 0 ? s.replace(/\s+/g, ' ') : s;
        }
        const s = tojsonObject(x, indent, nolint, depth);
        return s;
      }
      default: return String(x);
    }
  }
  hide(g, 'tojson', function (x, indent, nolint) {
    const r = tojson(x, indent, nolint, 0);
    return r;
  });
  hide(g, 'tojsonObject', function (x, indent, nolint) { return tojsonObject(x, indent || '', nolint, 0); });
  hide(g, 'tostrictjson', function (x) { return JSON.stringify(x); });
  hide(g, 'print', function () { /* goes nowhere: there is no console on the server side */ });
  hide(g, 'printjson', function () {});
  hide(g, 'printjsononeline', function () {});
  hide(g, 'gc', function () {});
  hide(g, 'sleep', function (ms) { const until = Date.now() + ms; while (Date.now() < until) __nap(1); });
  hide(g, 'assert', function (cond, msg) {
    if (!cond) throw new Error('assert failed' + (msg !== undefined ? ' : ' + (typeof msg === 'function' ? msg() : msg) : ''));
  });
  hide(g, 'isString', (x) => typeof x === 'string');
  hide(g, 'isNumber', (x) => typeof x === 'number');
  hide(g, 'isObject', (x) => typeof x === 'object');
  hide(g, 'version', () => __serverVersion());

  // ---------------------------------------------------------------------------------------------------------------
  // Shell conveniences on built-ins
  // ---------------------------------------------------------------------------------------------------------------
  // the shell's Map (it replaces the built-in one; `get` after `put` fails because `friendlyEqual` is not defined here)
  function Map() { this._data = {}; }
  hide(Map, 'hash', function (val) {
    if (!val) return val;
    switch (typeof val) {
      case 'string': case 'number': case 'date': return val.toString();
      case 'object': case 'array': { let s = ''; for (const k in val) s += k + val[k]; return s; }
    }
    throw Error("can't hash : " + typeof val);
  });
  hide(Map.prototype, 'put', function (key, value) { const o = this._get(key); const old = o.value; o.value = value; return old; });
  hide(Map.prototype, 'get', function (key) { return this._get(key).value; });
  hide(Map.prototype, '_get', function (key) {
    const h = Map.hash(key);
    let a = this._data[h];
    if (!a) { a = []; this._data[h] = a; }
    for (let i = 0; i < a.length; i++) { if (friendlyEqual(key, a[i].key)) return a[i]; }
    const o = { key: key, value: null };
    a.push(o);
    return o;
  });
  hide(Map.prototype, 'values', function () { const all = []; for (const k in this._data) this._data[k].forEach(function (z) { all.push(z.value); }); return all; });
  hide(g, 'Map', Map);
  hide(Array, 'contains', function (a, x) { for (let i = 0; i < a.length; i++) if (a[i] == x) return true; return false; });
  hide(Array, 'unique', function (a) { const u = []; for (let i = 0; i < a.length; i++) if (!Array.contains(u, a[i])) u.push(a[i]); return u; });
  hide(Array, 'shuffle', function (a) { for (let i = a.length - 1; i > 0; i--) { const j = Math.floor(Math.random() * (i + 1)); const t = a[i]; a[i] = a[j]; a[j] = t; } return a; });
  hide(Array, 'tojson', function (a, indent, nolint) { return tojson(a, indent, nolint); });
  hide(Object, 'deepMerge', function (dst, src) { for (const k in src) { if (typeof src[k] === 'object' && src[k] !== null && typeof dst[k] === 'object' && dst[k] !== null) Object.deepMerge(dst[k], src[k]); else dst[k] = src[k]; } return dst; });
  hide(Number.prototype, 'toPercentStr', function () { return (this * 100).toFixed(2) + '%'; });
  hide(RegExp.prototype, 'toJSON', function () { return { $regex: this.source, $options: this.flags }; });
  hide(Array, 'fetchRefs', function (a) { return a; });
  hide(Array, 'sum', function (a) { if (a.length === 0) return null; let s = a[0]; for (let i = 1; i < a.length; i++) s += a[i]; return s; });
  hide(Array, 'avg', function (a) { if (a.length === 0) return null; return Array.sum(a) / a.length; });
  hide(Array, 'stdDev', function (a) { const m = Array.avg(a); let s = 0; for (let i = 0; i < a.length; i++) s += Math.pow(a[i] - m, 2); return Math.sqrt(s / a.length); });
  hide(Object, 'extend', function (dst, src) { for (const k in src) dst[k] = src[k]; return dst; });
  hide(Object, 'merge', function (dst, src) { return Object.extend(Object.extend({}, dst), src); });
  hide(Object, 'keySet', function (o) { return Object.keys(o); });
  hide(Object, 'bsonsize', function (o) { return __bsonsize(g.__ser(o)); });
  hide(g, 'bsonsize', Object.bsonsize);
  hide(g, 'buildInfo', function () { return { version: __serverVersion() }; });
  hide(g, 'bsonWoCompare', function (a, b) { return __bsoncmp(g.__ser(a), g.__ser(b)); });
  hide(g, 'hex_md5', function (s) { return __md5(String(s)); });
  hide(Number.prototype, 'zeroPad', function (w) { w = w || 2; let s = '' + this; while (s.length < w) s = '0' + s; return s; });
  hide(String.prototype, 'ltrim', function () { return this.replace(/^\s+/, ''); });
  hide(String.prototype, 'rtrim', function () { return this.replace(/\s+$/, ''); });
  hide(Date, 'timeFunc', function (theFunc, numTimes) { const start = new Date(); numTimes = numTimes || 1; for (let i = 0; i < numTimes; i++) theFunc.apply(null, Array.prototype.slice.call(arguments, 2)); return new Date() - start; });
  hide(Date.prototype, 'tojson', function () { return tojson(this); });
  const dateParts = (d) => {
    const days = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
    const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    const p2 = (n) => String(n).padStart(2, '0');
    return { wd: days[d.getUTCDay()], mon: months[d.getUTCMonth()], day: p2(d.getUTCDate()), dayNoPad: String(d.getUTCDate()), year: String(d.getUTCFullYear()).padStart(4, '0'), time: p2(d.getUTCHours()) + ':' + p2(d.getUTCMinutes()) + ':' + p2(d.getUTCSeconds()) };
  };
  hide(Date.prototype, 'toString', function () { if (isNaN(this.getTime())) return 'Invalid Date'; const p = dateParts(this); return p.wd + ' ' + p.mon + ' ' + p.day + ' ' + p.year + ' ' + p.time + ' GMT+0000 (UTC)'; });
  hide(Date.prototype, 'toLocaleString', function () { if (isNaN(this.getTime())) return 'Invalid Date'; const p = dateParts(this); return p.wd + ' ' + p.mon + ' ' + p.dayNoPad.padStart(2, ' ') + ' ' + p.time + ' ' + p.year; });

  // ---------------------------------------------------------------------------------------------------------------
  // Values crossing the boundary
  // ---------------------------------------------------------------------------------------------------------------
  hide(g, '__mk', function (entries) { const o = Object.create(BP); for (const [k, v] of entries) Object.defineProperty(o, k, { value: v, writable: true, configurable: true, enumerable: true }); return o; });
  const BP = Object.create(Object.prototype, { [Symbol.toStringTag]: { value: 'BSON', configurable: true } });
  hide(g, '__BP', BP);
  const intKeys = new WeakMap();
  const pristine = new WeakMap();
  // a document argument: a proxy that notices modification, remembering the original BSON for an unmodified hand-back
  hide(g, '__bt', function (o, text) {
    const st = { dirty: false, text };
    const p = new Proxy(o, {
      set(t, k, v, r) { st.dirty = true; return Refl.set(t, k, v); },
      defineProperty(t, k, d) { st.dirty = true; return Refl.defineProperty(t, k, d); },
      deleteProperty(t, k) { st.dirty = true; return Refl.deleteProperty(t, k); },
      setPrototypeOf(t, pr) { st.dirty = true; return Refl.setPrototypeOf(t, pr); },
    });
    pristine.set(p, st);
    const tk = intKeys.get(o);
    if (tk) intKeys.set(p, tk);
    return p;
  });
  hide(g, '__bo', function (o, keys) { const m = new NativeMap(); for (const k of keys) m.set(k, o[k]); intKeys.set(o, m); return o; });
  hide(g, '__oidv', (h) => new ObjectId(h));
  hide(g, '__nl', (s) => new NumberLong(s));
  hide(g, '__nd', (s) => new NumberDecimal(s));
  hide(g, '__ts', (t, i) => new Timestamp(t, i));
  hide(g, '__bd', (st, b) => new BinData(st, b));
  const fromBson = new WeakSet();
  hide(g, '__re', (p, f) => { let r; try { r = new RegExp(p, f); } catch (e) { r = new RegExp(''); } fromBson.add(r); return r; });
  hide(g, '__fn', (code) => { try { return (0, eval)('(' + code + ')'); } catch (e) { return code; } });

  // Serialisation of a JS value to the server's wire text: a JSON-like stream in which BSON types that JSON lacks are
  // tagged objects {"\u0001": "<tag>", "v": ...}. Key order of objects is preserved.
  let regexSlash = true;
  function ser(v, out) {
    if (v === null || v === undefined) { out.push('null'); return; }
    if (v === minKey || v === g.MinKey) { out.push('{"\u0001":"min"}'); return; }
    if (v === maxKey || v === g.MaxKey) { out.push('{"\u0001":"max"}'); return; }
    switch (typeof v) {
      case 'number':
        if (v !== v) out.push('{"\u0001":"d","v":"NaN"}');
        else if (v === Infinity) out.push('{"\u0001":"d","v":"Infinity"}');
        else if (v === -Infinity) out.push('{"\u0001":"d","v":"-Infinity"}');
        else if (v === 0 && 1 / v < 0) out.push('{"\u0001":"d","v":"-0"}');
        else out.push(String(v));
        return;
      case 'string': out.push(JSON.stringify(v)); return;
      case 'boolean': out.push(v ? 'true' : 'false'); return;
      case 'function': out.push('{"\u0001":"c","v":' + JSON.stringify(v.toString()) + '}'); return;
      case 'symbol': throw new MongoJsError(2, 'cannot convert a Symbol to BSON');
      case 'bigint': throw new MongoJsError(2, 'cannot convert a BigInt to BSON');
    }
    if (v instanceof Date) { out.push('{"\u0001":"t","v":"' + (isNaN(v.getTime()) ? 0 : v.getTime()) + '"}'); return; }
    if (v instanceof RegExp) { out.push('{"\u0001":"r","p":' + JSON.stringify(regexSlash && !fromBson.has(v) ? v.source + '/' : v.source) + ',"f":' + JSON.stringify(v.flags.replace(/[^imsx]/g, '')) + '}'); return; }
    if (v instanceof ObjectId) { out.push('{"\u0001":"o","v":"' + v.str + '"}'); return; }
    if (v instanceof NumberLong) { out.push('{"\u0001":"l","v":"' + longs.get(v) + '"}'); return; }
    if (v instanceof NumberInt) { out.push('{"\u0001":"i","v":"' + ints.get(v) + '"}'); return; }
    if (v instanceof NumberDecimal) { out.push('{"\u0001":"D","v":' + JSON.stringify(decs.get(v)) + '}'); return; }
    if (v instanceof Timestamp) { out.push('{"\u0001":"ts","t":' + v.t + ',"i":' + v.i + '}'); return; }
    if (v instanceof BinData) { const b = bins.get(v); out.push('{"\u0001":"b","s":' + b.subtype + ',"v":"' + b64encode(b.bytes) + '"}'); return; }
    if (Array.isArray(v)) {
      out.push('[');
      let first = true;
      v.forEach((x) => { if (!first) out.push(','); first = false; ser(x, out); });
      out.push(']');
      return;
    }
    const pr = pristine.get(v);
    if (pr !== undefined && !pr.dirty) { out.push(pr.text); return; }
    out.push('{');
    let first = true;
    const tracked = intKeys.get(v);
    for (const k of Object.keys(v)) {
      if (!first) out.push(',');
      first = false;
      out.push(JSON.stringify(k), ':');
      const x = v[k];
      if (tracked !== undefined && typeof x === 'number' && tracked.get(k) === x) out.push('{"\u0001":"i","v":"' + x + '"}');
      else ser(x, out);
    }
    out.push('}');
  }
  hide(g, '__ser', function (v) { const out = []; ser(v, out); return out.join(''); });

  // ---------------------------------------------------------------------------------------------------------------
  // mapReduce
  // ---------------------------------------------------------------------------------------------------------------
  function gkey(k) {
    if (k === null || k === undefined) return 'z';
    switch (typeof k) {
      case 'number': return 'n' + (k === 0 ? 0 : k);
      case 'string': return 's' + JSON.stringify(__collkey(k));
      case 'boolean': return k ? 'T' : 'F';
      case 'function': return 'f' + k.toString();
    }
    if (k instanceof NumberLong) return 'n' + Number(longs.get(k));
    if (k instanceof NumberInt) return 'n' + ints.get(k);
    if (k instanceof Date) return 'd' + k.getTime();
    if (k instanceof ObjectId) return 'o' + k.str;
    if (k instanceof RegExp) return 'r' + k.toString();
    if (Array.isArray(k)) return '[' + k.map(gkey).join(',') + ']';
    if (k instanceof NumberDecimal || k instanceof Timestamp || k instanceof BinData || k === minKey || k === maxKey) { const o = []; ser(k, o); return 'x' + o.join(''); }
    return '{' + Object.keys(k).map((x) => JSON.stringify(x) + ':' + gkey(k[x])).join(',') + '}';
  }
  let groups = null, order = null, emitted = 0;
  hide(g, '__mr_begin', function () { groups = new NativeMap(); order = []; emitted = 0; });
  hide(g, 'emit', function (k, v) {
    if (arguments.length !== 2) throw new MongoJsError(31220, 'emit takes 2 args');
    const id = gkey(k);
    let e = groups.get(id);
    if (!e) { e = { key: k, values: [] }; groups.set(id, e); order.push(e); }
    e.values.push(v);
    emitted++;
  });
  hide(g, '__mr_map', function (fn, doc) { fn.call(doc); });
  // reduce every key (values arrive newest first), then finalize
  hide(g, '__mr_reduce', function (reduce, finalize) {
    const res = [];
    for (const e of order) {
      const vals = e.values.slice().reverse();
      let r = reduce(e.key, vals);
      if (finalize) r = finalize(e.key, r);
      res.push({ _id: e.key, value: r });
    }
    return res;
  });
  hide(g, '__mr_count', () => emitted);
  hide(g, '__mr_results', function (res) { const out = []; regexSlash = false; try { ser(res, out); } finally { regexSlash = true; } return out.join(''); });
})(globalThis);
