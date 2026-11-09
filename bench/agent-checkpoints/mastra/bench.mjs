// Mastra variant of the agent checkpoint benchmark: the same turns (generator.py --export-turns) through Mastra's real
// storage adapters, @mastra/mongodb (MongoDBStore: our server or mongod/Atlas) and @mastra/pg (PostgresStore).
//
// Important difference from LangGraph: Mastra's agent memory stores each message once (append-only rows/documents),
// it does not re-write the whole message list at every step. So storage grows linearly with thread length, not with its
// square, and there is no per-step checkpoint to "time travel" to (Mastra keeps full-state snapshots only for workflows,
// persistWorkflowSnapshot, which an agent loop does not use). Operations measured, per the same plan:
//   save_step    saveMessages([the new message(s) of one superstep])     (user msg; tool call; tool result; answer)
//   resume       listMessages(threadId, all pages)                        at the start of every turn (the agent reloads memory)
//   resume_last  listMessages(threadId, last 40)                          what an agent with lastMessages=40 loads
//   list         listMessages(threadId, all) in the read phase
//   erase_user   deleteThread for each thread of a sampled user, then a check that no message is left
// Database: "app_chat" (Mongo database name; for Postgres the URI must point at a database of that name).
// Output: OUT/ops.jsonl (one line per operation) and OUT/summary.json.
//
// Usage: node mastra/bench.mjs mongo|pg URI TURNS.jsonl OUT_DIR [--local]
//   (packages from mastra/package.json; run_local.sh installs them in $HOME/.fenvix-agentbench-node)
import fs from 'node:fs';
import path from 'node:path';
import { createRequire } from 'node:module';

const [kind, uri, turnsPath, out, ...flags] = process.argv.slice(2);
const local = flags.includes('--local');
const NODE_DIR = process.env.MASTRA_NODE_DIR || path.join(process.env.HOME, '.fenvix-agentbench-node');
const req = createRequire(path.join(NODE_DIR, 'package.json'));
const imp = async name => import(req.resolve(name));

fs.mkdirSync(out, { recursive: true });
const ops = fs.createWriteStream(path.join(out, 'ops.jsonl'));
const rows = [];
const rec = r => { r = { ts: Date.now() / 1000, ...r }; rows.push(r); ops.write(JSON.stringify(r) + '\n'); };
const timed = async (op, extra, fn) => {
  const t0 = process.hrtime.bigint();
  const res = await fn();
  rec({ op, ms: Number(process.hrtime.bigint() - t0) / 1e6, ...extra });
  return res;
};

async function openStore() {
  if (kind === 'mongo') {
    const { MongoDBStore } = await imp('@mastra/mongodb');
    const s = new MongoDBStore({ id: 'app_chat', url: uri, dbName: 'app_chat' });
    await s.init();
    return s;
  }
  const { PostgresStore } = await imp('@mastra/pg');
  const s = new PostgresStore({ id: 'app_chat', connectionString: uri });
  await s.init();
  return s;
}

const store = await openStore();
const memory = await store.getStore('memory');
const turns = fs.readFileSync(turnsPath, 'utf8').trim().split('\n').map(l => JSON.parse(l));
const BASE = Date.UTC(2026, 0, 1);
const seenThreads = new Set();
const userThreads = new Map();
const counter = new Map();
let logical = 0;

const mkMsg = (e, m, k) => {
  const n = counter.get(e.thread_id) || 0;
  counter.set(e.thread_id, n + 1);
  const parts = m.tool_call
    ? [{ type: 'tool-invocation', toolInvocation: { state: 'call', toolCallId: m.tool_call.id, toolName: m.tool_call.name, args: m.tool_call.args } }]
    : m.role === 'tool'
      ? [{ type: 'tool-invocation', toolInvocation: { state: 'result', toolCallId: m.tool_call_id, toolName: 'tool', args: {}, result: m.text } }]
      : [{ type: 'text', text: m.text }];
  return {
    id: `${e.thread_id}-${String(n).padStart(6, '0')}`, threadId: e.thread_id, resourceId: e.user,
    role: m.role === 'tool' ? 'assistant' : m.role, type: 'v2', createdAt: new Date(BASE + e.t * 1000 + k),
    content: { format: 2, parts, content: m.text || '' },
  };
};

// -- write phase -------------------------------------------------------------------------------------------------------
const tw = Date.now();
for (const e of turns) {
  if (!seenThreads.has(e.thread_id)) {
    seenThreads.add(e.thread_id);
    if (!userThreads.has(e.user)) userThreads.set(e.user, []);
    userThreads.get(e.user).push(e.thread_id);
    await memory.saveThread({ thread: { id: e.thread_id, resourceId: e.user, title: 'conversation', metadata: {}, createdAt: new Date(BASE), updatedAt: new Date(BASE) } });
  }
  await timed('resume', { phase: 'write', gap: e.gap, kind: e.kind, thread_id: e.thread_id },
    () => memory.listMessages({ threadId: e.thread_id, perPage: false }));
  await timed('resume_last40', { phase: 'write', gap: e.gap, kind: e.kind, thread_id: e.thread_id },
    () => memory.listMessages({ threadId: e.thread_id, perPage: 40, page: 0, orderBy: { field: 'createdAt', direction: 'DESC' } }));
  // one superstep per message, like the LangGraph run (input, agent tool call, tool result, ..., final answer)
  for (let k = 0; k < e.messages.length; k++) {
    const msg = mkMsg(e, e.messages[k], k);
    const bytes = Buffer.byteLength(JSON.stringify(msg));
    logical += bytes;
    await timed('save_step', { phase: 'write', kind: e.kind, thread_id: e.thread_id, logical_bytes: bytes },
      () => memory.saveMessages({ messages: [msg] }));
  }
}
const writeSeconds = (Date.now() - tw) / 1000;

// -- read phase: whole history of the 30 longest threads -----------------------------------------------------------------
const longest = [...counter.entries()].sort((a, b) => b[1] - a[1]).slice(0, 30).map(x => x[0]);
for (const t of longest) {
  await timed('list', { phase: 'read', thread_id: t }, () => memory.listMessages({ threadId: t, perPage: false }));
}

// -- erase phase: 10% of users, at least 2 -------------------------------------------------------------------------------
const users = [...userThreads.keys()].sort();
const nErase = Math.max(2, Math.floor(users.length / 10));
let left = 0;
for (const u of users.slice(0, nErase)) {
  await timed('erase_user', { phase: 'erase', user: u, threads: userThreads.get(u).length }, async () => {
    for (const t of userThreads.get(u)) await memory.deleteThread({ threadId: t });
  });
  for (const t of userThreads.get(u)) left += (await memory.listMessages({ threadId: t, perPage: false })).messages.length;
}
ops.end();
await store.close();

const pct = (xs, q) => { xs = [...xs].sort((a, b) => a - b); return xs.length ? +xs[Math.min(xs.length - 1, Math.floor(q * xs.length))].toFixed(3) : null; };
const byOp = {};
for (const r of rows) (byOp[r.op + (r.gap && r.op.startsWith('resume') ? `[${r.gap}]` : '')] ||= []).push(r.ms);
const latency = Object.fromEntries(Object.entries(byOp).sort().map(([k, v]) => [k, { n: v.length, p50: pct(v, 0.5), p99: pct(v, 0.99), max: pct(v, 1) }]));
const summary = {
  note: local ? 'LOCAL runs are validation, not results' : 'AWS round result',
  framework: 'mastra', store: kind, turns: turns.length, threads: seenThreads.size, messages: [...counter.values()].reduce((a, b) => a + b, 0),
  write_seconds: writeSeconds, logical_message_bytes: logical, latency_ms: latency,
  erase: { users: nErase, messages_left: left }, ok: left === 0,
};
fs.writeFileSync(path.join(out, 'summary.json'), JSON.stringify(summary, null, 2));
console.log(`[mastra/${kind}] ${local ? 'VALIDATION, NOT RESULTS: ' : ''}ok=${summary.ok} messages=${summary.messages} ` +
  `save_step p50/p99=${latency.save_step.p50}/${latency.save_step.p99} ms -> ${out}/summary.json`);
process.exit(0);
