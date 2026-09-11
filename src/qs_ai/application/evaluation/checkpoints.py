from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from qs_ai.domain.evaluation.checkpoint import ExecutionCheckpoint


class CheckpointConflict(Exception):
    pass


@dataclass(frozen=True)
class CheckpointState:
    run_id: UUID
    version: int
    checkpoint: ExecutionCheckpoint | None

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version < 0:
            raise ValueError("Invalid checkpoint version")


class Checkpoints(Protocol):
    async def create(self, run_id: UUID) -> CheckpointState: ...
    async def get(self, run_id: UUID) -> CheckpointState | None: ...
    async def save(self, state: CheckpointState, expected_version: int) -> None: ...
