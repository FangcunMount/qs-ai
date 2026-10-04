from dataclasses import dataclass

from qs_ai.domain.interpretation.model import Actor


@dataclass(frozen=True)
class StateEvent:
    event_id: str
    request_id: str
    session_id: str
    actor: Actor
    testee_id: str
    version: int
    status: str
    question_id: str = ""
    question: str = ""
    can_skip: bool = False
    failure_code: str = ""
    artifact_json: str = ""
