#!/usr/bin/env bash
# Step 1 evidence: fetch the pinned saver releases and print the lines the report quotes.
# Output: results/saver-code.txt. Needs pip and npm (PyPI and the npm registry).
set -euo pipefail
cd "$(dirname "$0")"
W=$(mktemp -d); trap 'rm -rf "$W"' EXIT
PY_PKGS="langgraph==1.2.20 langgraph-checkpoint==4.3.1 langgraph-checkpoint-postgres==3.2.0 langgraph-checkpoint-mongodb==0.5.1 langgraph-checkpoint-sqlite==3.1.3 langchain==1.4.9"
NPM_PKGS="@langchain/langgraph-checkpoint-postgres@1.1.2 @langchain/langgraph-checkpoint-mongodb@1.5.1 @langchain/langgraph@1.7.2 @mastra/pg@1.35.0 @mastra/mongodb@1.26.0"
(cd "$W" && for p in $PY_PKGS; do pip download -q --no-deps "$p" -d py >/dev/null; done && for f in py/*.whl; do unzip -q -o "$f" -d py/x; done)
(cd "$W" && mkdir -p npm && cd npm && for p in $NPM_PKGS; do f=$(npm pack -q "$p"); d=${f%.tgz}; mkdir -p "$d"; tar xzf "$f" -C "$d"; done)
show() { echo; echo "---- $1  lines $2-$3  ($4)"; sed -n "$2,$3p" "$W/$1" | awk -v s="$2" '{printf "%6d  %s\n", s+NR-1, $0}'; }
{
echo "Pinned: $PY_PKGS"
echo "Pinned: $NPM_PKGS"
P=py/x/langgraph/checkpoint
show $P/postgres/base.py 50 79 "Python Postgres schema: checkpoints JSONB, checkpoint_blobs keyed by channel+version, checkpoint_writes"
show $P/postgres/base.py 133 137 "blob upsert: one row per (channel, version), DO NOTHING if it exists"
show $P/postgres/__init__.py 314 337 "Python Postgres put: non-primitive channel values go to blobs, only channels in new_versions are written"
show $P/mongodb/saver.py 447 463 "Python MongoDB put: the whole checkpoint (every channel value) serialised into one document"
show $P/sqlite/__init__.py 433 441 "Python SQLite put: the whole checkpoint serialised into one row"
show py/x/langgraph/graph/message.py 310 311 "MessagesState: messages use add_messages (BinaryOperatorAggregate, full value per version)"
show py/x/langgraph/channels/delta.py 25 63 "DeltaChannel: beta, opt-in, stores writes only, snapshot every snapshot_frequency (default 100) updates"
show py/x/langgraph/pregel/_messages.py 436 451 "the messages reducer for DeltaChannel is private (_messages_delta_reducer) and experimental"
show py/x/langchain/agents/factory.py 1226 1232 "create_agent: AgentState by default (BinaryOperatorAggregate); DeltaChannel only with a caller schema"
J=npm/langchain-langgraph-checkpoint-postgres-1.1.2/package/dist
show $J/index.js 166 178 "JS Postgres _dumpBlobs: one blob per channel in newVersions"
show $J/index.js 190 194 "JS Postgres _dumpCheckpoint: channel_values removed from the checkpoint row"
show $J/index.js 375 397 "JS Postgres put"
show npm/langchain-langgraph-checkpoint-mongodb-1.5.1/package/dist/checkpoint.js 196 220 "JS MongoDB put: whole checkpoint serialised"
show npm/langchain-langgraph-1.7.2/package/dist/graph/messages_annotation.js 41 44 "JS MessagesAnnotation: messagesStateReducer (full value)"
show npm/langchain-langgraph-1.7.2/package/dist/graph/messages_reducer.js 95 105 "JS DeltaChannel reducer: experimental"
M=npm/mastra-pg-1.35.0/package/dist/index.js
show $M 12890 12910 "Mastra pg saveMessages: one row per message, upsert by message id"
show $M 26096 26111 "Mastra pg persistWorkflowSnapshot: one row per (workflow, run), snapshot overwritten"
M=npm/mastra-mongodb-1.26.0/package/dist/index.js
show $M 8718 8740 "Mastra mongodb saveMessages: one document per message, upsert by id"
show $M 13687 13706 "Mastra mongodb persistWorkflowSnapshot: one document per run, \$set snapshot (overwritten)"
} > results/saver-code.txt
echo "wrote results/saver-code.txt"
