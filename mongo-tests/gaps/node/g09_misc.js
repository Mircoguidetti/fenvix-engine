// Item 9 (Node driver): window operators $top / $bottom / $topN / $bottomN / $firstN / $lastN / $maxN / $minN in $setWindowFields and
// the same accumulators in $group.
module.exports = ({ C }) => {
  const PLAN = 'PlanExecutor error during aggregation :: caused by :: ';
  const tr = async (f) => { try { return await f(); } catch (e) { const m = e.errmsg || e.message || ''; return { err: [e.code, e.codeName, m.startsWith(PLAN) ? m.slice(PLAN.length) : m] }; } };
  const seed = (db) => db.collection('t').insertMany([
    { _id: 1, p: 'a', x: 1, v: 10, w: 'w1' }, { _id: 2, p: 'a', x: 2, v: 30, w: 'w2' }, { _id: 3, p: 'a', x: 3, v: 20 }, { _id: 4, p: 'a', x: 4, v: null, w: 'w4' },
    { _id: 5, p: 'b', x: 1, v: 5, w: 'w5' }, { _id: 6, p: 'b', x: 2, v: 7, w: 'w6' }, { _id: 7, p: 'b', x: 3, v: 7, w: 'w7' }, { _id: 8, p: 'c', x: 1, v: 1 }, { _id: 9, p: 'a', x: 5, v: [1, 2], w: 'w9' }]);
  const run = (db, output, sortBy, extra = {}) => tr(() => db.collection('t').aggregate([{ $setWindowFields: { sortBy: sortBy || { x: 1, _id: 1 }, output, ...extra } }, { $sort: { _id: 1 } }, { $project: { r: 1, q: 1 } }]).toArray());
  const WINDOWS = { unbounded_current: { documents: ['unbounded', 'current'] }, prev_next: { documents: [-1, 1] }, current_unbounded: { documents: ['current', 'unbounded'] }, all: { documents: ['unbounded', 'unbounded'] }, prev2: { documents: [-2, 0] }, range: { range: [-1, 1] }, range_unbounded: { range: ['unbounded', 0] } };
  const OPS = {
    top: { $top: { output: '$v', sortBy: { v: -1 } } }, bottom: { $bottom: { output: '$v', sortBy: { v: -1 } } }, top_doc: { $top: { output: { v: '$v', w: '$w' }, sortBy: { v: 1, _id: 1 } } },
    topN: { $topN: { output: '$v', sortBy: { v: 1, _id: 1 }, n: 2 } }, bottomN: { $bottomN: { output: '$v', sortBy: { v: 1, _id: 1 }, n: 2 } }, topN_doc: { $topN: { output: ['$_id', '$w'], sortBy: { w: -1, _id: 1 }, n: 3 } },
    firstN: { $firstN: { input: '$v', n: 2 } }, lastN: { $lastN: { input: '$v', n: 2 } }, maxN: { $maxN: { input: '$v', n: 2 } }, minN: { $minN: { input: '$v', n: 2 } }, firstN_big: { $firstN: { input: '$w', n: 10 } },
    maxN_w: { $maxN: { input: '$w', n: 1 } }, n_expr: { $topN: { output: '$v', sortBy: { _id: 1 }, n: { $literal: 2 } } },
  };
  for (const [n, op] of Object.entries(OPS)) for (const [wn, win] of Object.entries(WINDOWS)) {
    C(`g09.window.${n}.${wn}`, async (c, db) => { await seed(db); const sb = wn.includes('range') ? { x: 1 } : null; return { plain: await run(db, { r: { ...op, window: win } }, sb), partition: await run(db, { r: { ...op, window: win } }, sb, { partitionBy: '$p' }) }; });
  }
  C('g09.window.default_window_and_two_outputs', async (c, db) => { await seed(db); return { no_window: await run(db, { r: { $top: { output: '$v', sortBy: { _id: 1 } } } }), two: await run(db, { r: { $topN: { output: '$_id', sortBy: { _id: -1 }, n: 2 }, window: { documents: ['unbounded', 'current'] } }, q: { $bottom: { output: '$_id', sortBy: { _id: -1 } }, window: { documents: [-1, 1] } } }) }; });
  C('g09.window.errors', async (c, db) => {
    await seed(db); const out = {}; const W = { documents: ['unbounded', 'current'] };
    const bad = { top_no_sortBy: { $top: { output: '$v' } }, top_no_output: { $top: { sortBy: { v: 1 } } }, topN_no_n: { $topN: { output: '$v', sortBy: { v: 1 } } }, topN_n_zero: { $topN: { output: '$v', sortBy: { v: 1 }, n: 0 } }, topN_n_neg: { $topN: { output: '$v', sortBy: { v: 1 }, n: -1 } },
      topN_n_str: { $topN: { output: '$v', sortBy: { v: 1 }, n: 'x' } }, topN_n_double: { $topN: { output: '$v', sortBy: { v: 1 }, n: 2.5 } }, topN_n_field: { $topN: { output: '$v', sortBy: { v: 1 }, n: '$x' } }, firstN_no_n: { $firstN: { input: '$v' } }, firstN_no_input: { $firstN: { n: 2 } },
      firstN_extra: { $firstN: { input: '$v', n: 2, foo: 1 } }, top_extra: { $top: { output: '$v', sortBy: { v: 1 }, n: 2 } }, top_arg_type: { $top: 5 }, maxN_n_zero: { $maxN: { input: '$v', n: 0 } }, top_sort_empty: { $top: { output: '$v', sortBy: {} } }, top_sort_bad: { $top: { output: '$v', sortBy: { v: 2 } } }, firstN_array_n: { $firstN: { input: '$v', n: [1] } } };
    for (const [n, o] of Object.entries(bad)) out[n] = await run(db, { r: { ...o, window: W } }); return out;
  });
  C('g09.window.range_needs_one_sort_field', async (c, db) => { await seed(db); return { two: await run(db, { r: { $sum: '$v', window: { range: [-1, 1] } } }), one: await run(db, { r: { $sum: '$x', window: { range: [-1, 1] } } }, { x: 1 }) }; });
  C('g09.group.n_accumulators', async (c, db) => {
    await seed(db); const out = {};
    const accs = { firstN: { $firstN: { input: '$w', n: 3 } }, lastN: { $lastN: { input: '$w', n: 3 } }, maxN: { $maxN: { input: '$v', n: 3 } }, minN: { $minN: { input: '$v', n: 3 } }, top: { $top: { output: '$w', sortBy: { x: -1, _id: 1 } } }, bottom: { $bottom: { output: ['$_id', '$w'], sortBy: { x: -1, _id: 1 } } }, topN: { $topN: { output: '$_id', sortBy: { v: 1, _id: 1 }, n: 3 } }, bottomN: { $bottomN: { output: '$_id', sortBy: { v: 1, _id: 1 }, n: 3 } } };
    for (const [n, acc] of Object.entries(accs)) out[n] = await tr(() => db.collection('t').aggregate([{ $sort: { _id: 1 } }, { $group: { _id: '$p', r: acc } }, { $sort: { _id: 1 } }]).toArray()); return out;
  });
};
