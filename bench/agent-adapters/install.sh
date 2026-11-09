#!/usr/bin/env bash
# Install the published adapter packages and fetch their official test suites under $ADAPTERS_HOME (default /home/user/adapters).
# The repositories are checked out at their default branch; on 2026-11-06 each one's version equals the published package
# (langgraph-checkpoint-mongodb 0.5.1, langgraph-store-mongodb 0.4.0, @langchain/langgraph-checkpoint-mongodb 1.5.1,
# openai-agents 0.24.0, llama-index-storage-chat-store-mongo 0.5.0, mem0ai 2.2.1, @mastra/mongodb 1.26.0).
# Mastra's tests import its monorepo internals, so they run from the repository source (same version as the published package).
# Needs: uv, node 22, pnpm. Idempotent. Nothing here is committed.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd); A=${ADAPTERS_HOME:-/home/user/adapters}; mkdir -p "$A"; cd "$A"
export UV_LINK_MODE=copy
for r in langchain-ai/langchain-mongodb langchain-ai/langgraphjs openai/openai-agents-python run-llama/llama_index mastra-ai/mastra mem0ai/mem0; do
  [ -d "$(basename $r)" ] || git clone -q --depth 1 "https://github.com/$r"
done
venv() { [ -x "$1/bin/python" ] || uv venv -q -p 3.12 "$1"; shift_=$1; shift; VIRTUAL_ENV="$shift_" uv pip install -q "$@"; }
venv venv-lg langgraph-checkpoint-mongodb==0.5.1 langgraph-store-mongodb==0.4.0 "langgraph>=1.0.0" anyio pytest pytest-asyncio pytest-mock pytest-repeat syrupy langchain-core langchain-openai langchain-ollama
venv venv-oai "openai-agents[mongodb]==0.24.0" pytest pytest-asyncio inline-snapshot
venv venv-llama llama-index-storage-chat-store-mongo==0.5.0 llama-index pytest pytest-asyncio docker
venv venv-mem0 mem0ai==2.2.1 pymongo pytest pytest-asyncio
# LangGraph JS: the published package plus the repository's own tests, whose relative imports point at shims re-exporting it
mkdir -p js-lg/src
if [ ! -d js-lg/node_modules ]; then
  cp -r langgraphjs/libs/checkpoint-mongodb/src/tests js-lg/src/ && cp langgraphjs/libs/checkpoint-mongodb/vitest.config.* js-lg/
  echo 'export * from "@langchain/langgraph-checkpoint-mongodb";' > js-lg/src/index.ts; cp js-lg/src/index.ts js-lg/src/store.ts
  echo '{"name":"js-lg","private":true,"type":"module"}' > js-lg/package.json
  (cd js-lg && npm i -s @langchain/langgraph-checkpoint-mongodb@1.5.1 @langchain/core @langchain/langgraph-checkpoint @langchain/langgraph mongodb@6 vitest@3)
fi
# Mastra: install the store's workspace dependencies and build them (and the store, for the e2e loop)
if [ ! -f mastra/stores/mongodb/dist/index.js ]; then
  (cd mastra && pnpm install --filter "@mastra/mongodb..." && npx turbo run build:lib --filter="@mastra/mongodb...")
fi
echo "adapters ready in $A"
