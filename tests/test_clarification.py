from langgraph.checkpoint.memory import InMemorySaver
from langgraph.types import Command

from tests.probes.clarification import build_demo


def test_pause_resume_and_thread_isolation() -> None:
    saver = InMemorySaver()
    graph = build_demo(saver)
    first = {"configurable": {"thread_id": "first"}}
    second = {"configurable": {"thread_id": "second"}}
    paused = graph.invoke({"question": "Who completed the report?"}, first)
    assert paused["__interrupt__"][0].value["question"] == "Who completed the report?"
    graph.invoke({"question": "What period does this describe?"}, second)
    # Rebuild the graph against the same store, then continue the original thread.
    resumed = build_demo(saver).invoke(Command(resume="Mother"), first)
    assert resumed["answer"] == "Mother"
    assert resumed["status"] == "answered"
    assert "answer" not in graph.get_state(second).values
