// Item 4 (Node driver): find projections — same scenarios as g04_projection.py plus generated specifications.
module.exports = ({ C }) => {
  const DOCS = [
    { _id: 1, a: [{ x: 1, y: 2 }, { x: 2, y: 3 }, { x: 2, y: 4 }], b: 5, c: { d: 1, e: [1, 2, 3], f: { g: 1, h: 2 } }, s: 'str', n: null, arr: [1, 2, 3, 4, 5], mixed: [1, { k: 1 }, [2, 3]], empty: [] },
    { _id: 2, a: [{ x: 3, y: 1 }], b: 6, c: { d: 2 }, s: 'other', arr: [9, 8], mixed: 'scalar' },
    { _id: 3, a: { x: 7 }, b: null, arr: 5 },
    { _id: 4, a: 5, c: [{ d: 1 }, { d: 2, e: 3 }] },
    { _id: 5, nested: { arr: [1, [2, 3], { k: 4 }], docs: [{ v: 1, w: [1, 2] }, { v: 2 }, 3, [{ v: 5 }]] }, a: [[{ x: 1 }], { x: 2 }], c: 7, zz: [1, 2, { y: 1 }] },
    { _id: 6, b: 1, arr: [], mixed: [[], [[1]]], c: { e: [[1, 2], [3]], f: [{ g: 1 }, { g: 2, h: 3 }] } },
  ];
  const rng = (seed) => () => { seed |= 0; seed = (seed + 0x6D2B79F5) | 0; let t = Math.imul(seed ^ (seed >>> 15), 1 | seed); t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t; return ((t ^ (t >>> 14)) >>> 0) / 4294967296; };
  const pick = (r, a) => a[Math.floor(r() * a.length)];
  const find = async (db, spec, flt = {}) => { try { return await db.collection('t').find(flt, { projection: spec }).sort({ _id: 1 }).toArray(); } catch (e) { return { err: [e.code, e.codeName] }; } };
  const group = (name, specs) => C(name, async (c, db) => { await db.collection('t').insertMany(DOCS); const out = {}; let i = 0; for (const s of specs) { const [spec, flt] = Array.isArray(s) ? s : [s, {}]; out[`${i++}:${JSON.stringify(spec)}|${JSON.stringify(flt)}`] = await find(db, spec, flt); } return out; });

  group('g04.proj.inclusion_exclusion', [{ b: 1 }, { b: 0 }, { b: 1, s: 1 }, { _id: 0 }, { _id: 0, b: 1 }, { _id: 1, b: 0 }, { b: 2 }, { b: 1.5 }, { 'c.d': 1 }, { 'c.d': 0 }, { c: { d: 1 } }, { c: { f: { g: 1 } } }, { 'c.d': 1, c: 1 }, { c: 1, 'c.d': 1 }, { 'a.x': 1 }, { 'mixed.k': 1 }, { 'mixed.k': 0 }, { 'arr.x': 1 }, { 'a.b.c': 1 }, { '': 1 }, { 'a..b': 1 }, { 'a.': 1 }, { $b: 1 }, { b: null }, { b: 'x' }, { b: 1, s: 0 }]);
  group('g04.proj.expressions', [{ t: '$b' }, { t: '$s', b: 1 }, { t: { $add: ['$b', 1] } }, { t: { $size: '$arr' } }, { t: { $literal: '$b' } }, { t: 'abc' }, { t: [1, 2] }, { 'x.y': '$b' }, { 'c.d': '$b', 'c.e': 1 }, { _id: '$b' }, { t: '$$ROOT' }, { t: '$$REMOVE' }, { t: { $bogus: 1 } }, { t: { $add: 1, $sub: 2 } }, { t: '$' }, { t: '$$nope' }, { b: 0, t: '$s' }, { b: 1, s: 0, t: 1 }, { t: { $map: { input: '$arr', as: 'v', in: { $multiply: ['$$v', 2] } } } }, { t: { $filter: { input: '$arr', as: 'v', cond: { $gt: ['$$v', 2] } } } }, { 'nested.docs.v': '$b' }, { 'mixed.k': '$b' }]);
  group('g04.proj.slice', [{ arr: { $slice: 2 } }, { arr: { $slice: -2 } }, { arr: { $slice: 0 } }, { arr: { $slice: [1, 2] } }, { arr: { $slice: [-2, 1] } }, { arr: { $slice: [1, -1] } }, { arr: { $slice: [1] } }, { arr: { $slice: 'x' } }, { arr: { $slice: 1.5 } }, { b: { $slice: 1 } }, { 'c.e': { $slice: 2 } }, { c: { e: { $slice: 1 } } }, { arr: { $slice: 2 }, b: 1 }, { arr: { $slice: 2 }, b: 0 }, { arr: { $slice: 2 }, s: 1, _id: 0 }, { arr: { $slice: 2, $foo: 1 } }, { mixed: { $slice: 2 } }]);
  group('g04.proj.elemMatch', [{ a: { $elemMatch: { x: 2 } } }, { a: { $elemMatch: { x: 2, y: { $gt: 3 } } } }, { arr: { $elemMatch: { $gt: 3 } } }, { b: { $elemMatch: { x: 1 } } }, { 'c.e': { $elemMatch: { $gt: 1 } } }, { a: { $elemMatch: { x: 2 } }, b: 1 }, { a: { $elemMatch: { x: 2 } }, b: 0 }, { a: { $elemMatch: { x: 2 } }, _id: 0 }, { a: { $elemMatch: { x: 2 } }, arr: { $slice: 2 } }, { a: { $elemMatch: { x: 2 } }, t: '$b' }, { a: { $elemMatch: 5 } }, { a: { $elemMatch: {} } }, { mixed: { $elemMatch: { $type: 'array' } } }, { a: { $elemMatch: { $where: '1' } } }]);
  group('g04.proj.positional', [[{ 'arr.$': 1 }, { arr: 4 }], [{ 'a.$': 1 }, { 'a.x': 2 }], [{ 'arr.$': 1 }, { arr: { $gt: 2 } }], [{ 'arr.$': 1 }, {}], [{ 'arr.$': 1 }, { arr: { $ne: 1 } }], [{ 'arr.$': 1, b: 1 }, { arr: 4 }], [{ 'arr.$': 1, b: 0 }, { arr: 4 }], [{ 'arr.$': 0 }, { arr: 4 }], [{ 'arr.$': 1, 'a.$': 1 }, { arr: 4, 'a.x': 2 }], [{ 'c.e.$': 1 }, { 'c.e': 2 }], [{ 'c.$': 1 }, { 'c.d': 2 }], [{ 'a.$.x': 1 }, { 'a.x': 2 }], [{ 'arr.$': 1 }, { $or: [{ arr: 4 }, { b: 6 }] }], [{ 'a.x.$': 1 }, { arr: 4 }], [{ 'mixed.$': 1 }, { mixed: { $type: 'array' } }]]);
  C('g04.proj.findAndModify_and_findOneAnd', async (c, db) => {
    const t = db.collection('t'); await t.insertMany(DOCS); const out = {};
    const run = async (k, f) => { try { out[k] = await f(); } catch (e) { out[k] = { err: [e.code, e.codeName] }; } };
    await run('findOne', () => t.findOne({ arr: 4 }, { projection: { 'arr.$': 1 } }));
    await run('fau_after_pos', () => t.findOneAndUpdate({ _id: 1, arr: 4 }, { $set: { z: 1 } }, { projection: { 'arr.$': 1 }, returnDocument: 'after' }));
    await run('fau_before_pos', () => t.findOneAndUpdate({ _id: 1, arr: 4 }, { $set: { z: 2 } }, { projection: { 'arr.$': 1 }, returnDocument: 'before' }));
    await run('delete_slice', () => t.findOneAndDelete({ _id: 2 }, { projection: { arr: { $slice: 1 }, b: 1 } }));
    await run('replace_expr', () => t.findOneAndReplace({ _id: 3 }, { q: 1 }, { projection: { t: '$q' }, returnDocument: 'after' }));
    return out;
  });
  const PATHS = ['_id', 'a', 'a.x', 'a.y', 'b', 'c', 'c.d', 'c.e', 'c.f', 'c.f.g', 's', 'n', 'arr', 'mixed', 'mixed.k', 'empty', 'nested', 'nested.arr', 'nested.docs', 'nested.docs.v', 'zz', 'zz.y'];
  const VALUES = [1, 1, 1, 0, 0, true, false, '$b', '$c.d', '$a.x', { $add: [1, 2] }, { $slice: 2 }, { $slice: -1 }, { $slice: [1, 2] }, { $elemMatch: { x: 2 } }, { $elemMatch: { $gt: 2 } }, { d: 1 }, { g: 0 }, 'lit', null, 7, { $literal: 1 }];
  const FILTERS = [{ arr: { $gt: 2 } }, { arr: 4 }, { 'a.x': 2 }, { a: { $elemMatch: { x: 2 } } }, { 'c.e': 2 }, { mixed: 1 }, { arr: { $in: [8, 2] } }, { arr: { $ne: 1 } }, { $or: [{ arr: 4 }, { b: 6 }] }, { arr: { $exists: true } }, { b: 5 }, { 'nested.docs.v': 2 }, { arr: { $all: [2, 3] } }, { 'a.x': { $in: [1, 3] }, arr: { $lt: 9 } }, { mixed: { $type: 'array' } }, {}, { arr: { $gte: 2, $lte: 3 } }];
  const POS = ['arr.$', 'a.$', 'c.$', 'c.e.$', 'mixed.$', 'nested.arr.$', 'nested.docs.$', 'zz.$', 'b.$', 'a.x.$'];
  for (const seed of [11, 12, 13]) {
    C(`g04.proj.fuzz_${seed}`, async (c, db) => {
      const r = rng(seed); await db.collection('t').insertMany(DOCS); const out = {};
      for (let i = 0; i < 250; i++) {
        const spec = {}; const n = pick(r, [1, 1, 2, 2, 3, 4]); for (let j = 0; j < n; j++) spec[pick(r, PATHS)] = pick(r, VALUES);
        const special = Object.keys(spec).filter((k) => spec[k] && typeof spec[k] === 'object' && ('$slice' in spec[k] || '$elemMatch' in spec[k]));
        if (special.some((k) => Object.keys(spec).some((o) => o !== k && (o.startsWith(k + '.') || k.startsWith(o + '.'))))) continue;
        out[`${i}:${JSON.stringify(spec)}`] = await find(db, spec);
      }
      return out;
    });
    C(`g04.proj.positional_fuzz_${seed}`, async (c, db) => {
      const r = rng(seed * 7); await db.collection('t').insertMany(DOCS); const out = {};
      for (let i = 0; i < 250; i++) { const flt = pick(r, FILTERS); const spec = { [pick(r, POS)]: 1 }; if (r() < 0.3) spec[pick(r, ['b', 's', '_id', 'arr2'])] = pick(r, [1, 0]); out[`${i}:${JSON.stringify(spec)}|${JSON.stringify(flt)}`] = await find(db, spec, flt); }
      return out;
    });
  }
};
