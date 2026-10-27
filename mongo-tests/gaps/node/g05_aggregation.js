// Item 5 (Node driver): trigonometric / bitwise / standard-deviation expressions, exact $sum/$avg, $lookup let, $changeStreamSplitLargeEvent.
module.exports = ({ C, Long, EJSON, bson }) => {
  const { Decimal128 } = bson;
  const evAll = async (db, exprs) => {
    const t = db.collection('t'); await t.insertOne({ _id: 1, x: 0.5, n: 3, s: 'str', arr: [1, 2, 3, 4], nul: null, big: Long.fromNumber(2 ** 40), dec: Decimal128.fromString('0.5') });
    const out = {}; let i = 0;
    for (const e of exprs) { try { const r = await t.aggregate([{ $project: { _id: 0, r: e } }]).toArray(); out[`${i++}:${JSON.stringify(e)}`] = r[0].r === undefined ? '<missing>' : r[0].r; } catch (er) { out[`${i++}:${JSON.stringify(e)}`] = { err: [er.code, er.codeName] }; } }
    return out;
  };
  const VALS = [0.5, 2, -2, 1, -1, 0, 10, 1e300, Infinity, -Infinity, NaN, 'x', null, true, Long.fromNumber(3), [0.5], [0.5, 1], [], { a: 1 }, '$x', '$n', '$s', '$nul', '$nope', '$big', '$arr'];
  for (const op of ['$sin', '$cos', '$tan', '$asin', '$acos', '$atan', '$sinh', '$cosh', '$tanh', '$asinh', '$acosh', '$atanh', '$degreesToRadians', '$radiansToDegrees']) C(`g05.expr.${op.slice(1)}`, async (c, db) => evAll(db, VALS.map((v) => ({ [op]: v }))));
  C('g05.expr.atan2', async (c, db) => evAll(db, [[1, 1], [0, 0], [-1, -1], [1, 0], [Infinity, 1], [NaN, 1], ['x', 1], [null, 1], [1], [1, 2, 3], '$arr', ['$x', '$n'], [Long.fromNumber(5), 2], [true, 1]].map((p) => ({ $atan2: p }))));
  C('g05.expr.bitwise', async (c, db) => {
    const L = (n) => Long.fromNumber(n);
    const vals = [[5, 3], [5, L(3)], [], [5], 5, [5, 1.5], [null, 1], ['a', 1], [1, 2, 4], [L(2 ** 40), L(2 ** 40 + 1)], [-1, 255], [2147483647, 1], [1, '$nope'], [1, '$nul'], [Decimal128.fromString('3'), 1], [true, 1], ['$n', '$n'], ['$big', 1]];
    const ex = ['$bitAnd', '$bitOr', '$bitXor'].flatMap((op) => vals.map((v) => ({ [op]: v })));
    return evAll(db, ex.concat([5, [5], L(5), null, 'a', [1, 2], 2.0, [], 0, -1, -2147483648, L(2 ** 40), '$nope', '$n', '$big'].map((v) => ({ $bitNot: v }))));
  });
  C('g05.expr.stdDev_as_expression', async (c, db) => evAll(db, ['$stdDevPop', '$stdDevSamp'].flatMap((op) => [[1, 2, 3, 4], [1], [], ['a', 1, 2], 1, [1, 2, null], null, '$nope', 'x', '$arr', ['$n', '$n', 1], [[1, 2], 3]].map((v) => ({ [op]: v })))));
  C('g05.sum_avg.double_accumulation', async (c, db) => {
    let seed = 5; const r = () => { seed = (seed * 1103515245 + 12345) & 0x7fffffff; return seed / 0x7fffffff; };
    const docs = Array.from({ length: 400 }, (_, i) => ({ _id: i, g: i % 3, d: (r() * 2 - 1) * 1e6 * [1, 1e-3, 1e-9][i % 3], i: Math.floor(r() * 11) - 5 }));
    docs.push(...Array.from({ length: 100 }, (_, i) => ({ _id: 1000 + i, g: 9, d: 0.1 })), ...Array.from({ length: 40 }, (_, i) => ({ _id: 2000 + i, g: 8, d: i % 2 ? 1e16 : -1e16 + 1.0 })));
    await db.collection('t').insertMany(docs);
    return { group: await db.collection('t').aggregate([{ $group: { _id: '$g', s: { $sum: '$d' }, a: { $avg: '$d' }, si: { $sum: '$i' }, ai: { $avg: '$i' }, sd: { $stdDevPop: '$d' }, ss: { $stdDevSamp: '$d' } } }, { $sort: { _id: 1 } }]).toArray(),
      expr: await db.collection('t').aggregate([{ $group: { _id: '$g', arr: { $push: '$d' } } }, { $project: { s: { $sum: '$arr' }, a: { $avg: '$arr' } } }, { $sort: { _id: 1 } }]).toArray() };
  });
  C('g05.lookup.let_variables', async (c, db) => {
    await db.collection('a').insertMany([{ _id: 1, k: 1, m: null, arr: [1, 2] }, { _id: 2, k: 2 }, { _id: 3, k: null }, { _id: 4 }]);
    await db.collection('b').insertMany([{ _id: 10, x: 1 }, { _id: 11, x: 2 }, { _id: 12, x: null }, { _id: 13 }, { _id: 14, x: [1, 2] }]);
    const L = (let_, pipe, extra = {}) => [{ $lookup: { from: 'b', let: let_, pipeline: pipe, as: 'j', ...extra } }, { $project: { 'j._id': 1 } }, { $sort: { _id: 1 } }];
    const pipes = [L({ v: '$k' }, [{ $match: { $expr: { $eq: ['$x', '$$v'] } } }]), L({ v: '$m' }, [{ $match: { $expr: { $eq: ['$x', '$$v'] } } }]), L({ v: '$nope' }, [{ $match: { $expr: { $eq: ['$x', '$$v'] } } }]), L({ v: '$nope' }, [{ $match: { $expr: { $eq: ['$$v', null] } } }]),
      L({ v: '$nope' }, [{ $match: { $expr: { $eq: [{ $type: '$$v' }, 'missing'] } } }]), L({ v: '$nope' }, [{ $project: { t: { $type: '$$v' }, w: '$$v' } }]), L({ v: '$arr' }, [{ $match: { $expr: { $in: ['$x', '$$v'] } } }]), L({ V: '$k' }, [{ $match: {} }]), L({ v: '$k' }, [{ $match: { $expr: { $eq: ['$x', '$$undefinedvar'] } } }]),
      [{ $lookup: { from: 'b', let: { v: '$k' }, pipeline: 5, as: 'j' } }], [{ $lookup: { from: 'b', let: { v: '$k' }, pipeline: [{ $out: 'zz' }], as: 'j' } }], [{ $lookup: { from: 'b', let: { v: '$k' }, pipeline: [{ $documents: [{ x: 1 }] }], as: 'j' } }, { $sort: { _id: 1 } }], [{ $lookup: { from: 'b', let: { v: '$k' }, pipeline: [{ $group: { _id: '$$v', n: { $sum: 1 } } }], as: 'j' } }, { $project: { j: 1 } }, { $sort: { _id: 1 } }]];
    const out = {}; let i = 0; for (const p of pipes) { try { out[`${i++}`] = await db.collection('a').aggregate(p).toArray(); } catch (e) { out[`${i++}`] = { err: [e.code, e.codeName] }; } } return out;
  });
  C('g05.stage.changeStreamSplitLargeEvent', async (c, db) => {
    await db.collection('t').insertOne({ _id: 1 }); const out = {};
    for (const [k, p] of Object.entries({ plain: [{ $changeStreamSplitLargeEvent: {} }], badspec: [{ $changeStreamSplitLargeEvent: 1 }] })) { try { await db.collection('t').aggregate(p).toArray(); out[k] = 'ok'; } catch (e) { out[k] = { err: [e.code, e.codeName] }; } }
    for (const [k, p] of Object.entries({ first: [{ $changeStreamSplitLargeEvent: {} }, { $match: {} }], last: [{ $match: {} }, { $changeStreamSplitLargeEvent: {} }], twice: [{ $changeStreamSplitLargeEvent: {} }, { $changeStreamSplitLargeEvent: {} }], bad: [{ $changeStreamSplitLargeEvent: { a: 1 } }] })) { try { const s = db.collection('t').watch(p); await s.close(); out['cs_' + k] = 'ok'; } catch (e) { out['cs_' + k] = { err: [e.code, e.codeName] }; } }
    return out;
  }, { rs: true });
};
