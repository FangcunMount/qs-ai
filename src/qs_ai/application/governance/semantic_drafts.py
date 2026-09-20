"""Typed judge editing commands; QS supplies organization-admin authorization."""

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from uuid import UUID

from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.domain.evaluation.identity import FrozenContractRef
from qs_ai.domain.evaluation.semantic_draft import SemanticDraft, bounded_markdown
from qs_ai.domain.governance.prompt_draft import nonzero_uuid, positive, valid_reason


@dataclass(frozen=True)
class CreateSemanticDraft:
    draft_id: UUID
    command_id: UUID
    source_prompt: FrozenContractRef
    source_schema: FrozenContractRef
    source_owner_organization_id: int
    target_version: str
    reason: str

    def __post_init__(self) -> None:
        if (
            not nonzero_uuid(self.draft_id)
            or not nonzero_uuid(self.command_id)
            or not valid_reason(self.reason)
            or type(self.source_owner_organization_id) is not int
            or self.source_owner_organization_id < 0
        ):
            raise ValueError("Invalid semantic creation command")
        FrozenContractRef(self.source_prompt.id, self.target_version, "sha256:" + "0" * 64)


@dataclass(frozen=True)
class ReviseSemanticDraft:
    draft_id: UUID
    command_id: UUID
    expected_revision: int
    markdown: str
    reason: str

    def __post_init__(self) -> None:
        if (
            not nonzero_uuid(self.draft_id)
            or not nonzero_uuid(self.command_id)
            or not positive(self.expected_revision)
            or not valid_reason(self.reason)
        ):
            raise ValueError("Invalid semantic revision command")
        bounded_markdown(self.markdown)


@dataclass(frozen=True)
class FreezeSemanticDraft:
    draft_id: UUID
    command_id: UUID
    expected_revision: int
    reason: str

    def __post_init__(self) -> None:
        if (
            not nonzero_uuid(self.draft_id)
            or not nonzero_uuid(self.command_id)
            or not positive(self.expected_revision)
            or not valid_reason(self.reason)
        ):
            raise ValueError("Invalid semantic freeze command")


SemanticCommand = CreateSemanticDraft | ReviseSemanticDraft | FreezeSemanticDraft


class SemanticDrafts(Protocol):
    async def apply(
        self, scope: DraftScope, command: SemanticCommand, at: datetime
    ) -> SemanticDraft: ...
    async def get(self, scope: DraftScope, draft_id: UUID, revision: int = 0) -> SemanticDraft: ...
    async def receipt(self, scope: DraftScope, command_id: UUID) -> SemanticDraft: ...
    async def validate(self, scope: DraftScope, draft_id: UUID, revision: int) -> SemanticDraft: ...
