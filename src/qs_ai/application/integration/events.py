from dataclasses import dataclass
from typing import Protocol

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


class EventStore(Protocol):
    async def pending(self, limit: int) -> list[StateEvent]: ...
    async def delivered(self, event_id: str) -> None: ...
    async def retry(self, event_id: str) -> None: ...


class ResultReceiver(Protocol):
    async def accept(self, event: StateEvent) -> None: ...


class DeliverResults:
    def __init__(self, store: EventStore, receiver: ResultReceiver) -> None:
        self.store = store
        self.receiver = receiver

    async def once(self, limit: int = 20) -> int:
        sent = 0
        for event in await self.store.pending(limit):
            try:
                await self.receiver.accept(event)
            except Exception:
                await self.store.retry(event.event_id)
            else:
                await self.store.delivered(event.event_id)
                sent += 1
        return sent
