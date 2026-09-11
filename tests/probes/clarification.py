"""Offline infrastructure probe only; never presented as a real AI interpretation."""

from typing import Any, TypedDict

from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt


class ClarificationState(TypedDict, total=False):
    question: str
    answer: str
    status: str


def ask(state: ClarificationState) -> ClarificationState:
    answer = interrupt({"question": state["question"]})
    if not isinstance(answer, str) or not answer.strip():
        raise ValueError("A non-empty answer is required")
    return {"answer": answer.strip(), "status": "answered"}


def build_demo(checkpointer: BaseCheckpointSaver[Any]) -> Any:
    graph = StateGraph(ClarificationState)
    graph.add_node("ask", ask)
    graph.add_edge(START, "ask")
    graph.add_edge("ask", END)
    return graph.compile(checkpointer=checkpointer)
