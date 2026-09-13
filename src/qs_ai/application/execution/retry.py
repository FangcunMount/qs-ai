"""Explicit participant retries retain original evidence and execution history."""

from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.interpretation.ports import EvidenceSource, Receipt
from qs_ai.domain.interpretation.model import Actor


@dataclass(frozen=True)
class ParticipantRetry:
    scope: DraftScope
    session_id: str
    command_id: str
    expected_run_id: str
    expected_version: int
    reason: str
    confirm: bool
    expected_provider_invocations: int
    accept_result_unknown_risk: bool = False

    def __post_init__(self) -> None:
        for value in (self.session_id, self.command_id, self.expected_run_id):
            if str(UUID(value)) != value or UUID(value).int == 0:
                raise ValueError("Retry identities must be canonical UUIDs")
        if (
            type(self.expected_version) is not int
            or self.expected_version < 1
            or self.confirm is not True
            or type(self.expected_provider_invocations) is not int
            or self.expected_provider_invocations != 1
            or type(self.accept_result_unknown_risk) is not bool
            or not self.reason.strip()
            or len(self.reason) > 2000
        ):
            raise ValueError("Invalid participant retry confirmation")


@dataclass(frozen=True)
class ParticipantTarget:
    actor: Actor
    testee_id: str
    assessment_ids: tuple[str, ...]


@dataclass(frozen=True)
class ParticipantExecution:
    organization_id: int
    session_id: str
    request_id: str
    run_id: str
    version: int
    status: str
    subject_id: str
    testee_id: str
    assessment_ids: tuple[str, ...]
    failure_code: str
    model_call_status: str
    invocation_id: str
    source_run_id: str
    can_retry: bool
    unknown_result_risk: bool
    retry_provider_invocations: int = 1


class ParticipantRetryStore(Protocol):
    async def get(self, scope: DraftScope, session_id: str) -> ParticipantExecution: ...
    async def target(self, scope: DraftScope, session_id: str) -> ParticipantTarget: ...
    async def retry(self, command: ParticipantRetry) -> Receipt: ...
    async def receipt(self, scope: DraftScope, command_id: str) -> Receipt: ...


class RetryParticipant:
    def __init__(self, store: ParticipantRetryStore, source: EvidenceSource) -> None:
        self.store, self.source = store, source

    async def execute(self, command: ParticipantRetry) -> Receipt:
        target = await self.store.target(command.scope, command.session_id)
        # Admin delegation is checked by QS. Current original-participant access
        # must also still exist, even for an idempotent replay of the command.
        await self.source.authorize(target.actor, target.testee_id, target.assessment_ids)
        return await self.store.retry(command)

    async def receipt(self, scope: DraftScope, command_id: str) -> Receipt:
        receipt = await self.store.receipt(scope, command_id)
        target = await self.store.target(scope, receipt.session_id)
        await self.source.authorize(target.actor, target.testee_id, target.assessment_ids)
        return receipt
