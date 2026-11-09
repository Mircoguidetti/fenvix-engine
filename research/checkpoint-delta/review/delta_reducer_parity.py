"""DeltaChannel's messages reducer (langgraph 1.2.20, _messages_delta_reducer) is not add_messages.
Two failing cases found while running edge_cases.py. Run: ../.venv/bin/python delta_reducer_parity.py
Exit 1 while the gaps exist."""
from langchain_core.messages import AIMessage, HumanMessage, RemoveMessage
from langgraph.graph.message import REMOVE_ALL_MESSAGES, add_messages
from langgraph.pregel._messages import _messages_delta_reducer

s = [HumanMessage("h1", id="h1"), AIMessage("a1", id="a1"), HumanMessage("h2", id="h2")]
fails = 0
# 1. LangChain's SummarizationMiddleware (langchain 1.4.9, middleware/summarization.py 519) writes this:
w = [RemoveMessage(id=REMOVE_ALL_MESSAGES), AIMessage("summary", id="sum"), HumanMessage("h2", id="h2")]
a, d = [m.id for m in add_messages(s, w)], [m.id for m in _messages_delta_reducer(s, [w])]
print("REMOVE_ALL   add_messages", a, " delta", d)
fails += a != d
# 2. one update removes a message and writes a new one with the same id (a summary kept under one id)
w = [RemoveMessage(id="h1"), AIMessage("summary", id="h1")]
a, d = [m.id for m in add_messages(s, w)], [m.id for m in _messages_delta_reducer(s, [w])]
print("remove+re-add add_messages", a, " delta", d)
fails += a != d
raise SystemExit(1 if fails else 0)
