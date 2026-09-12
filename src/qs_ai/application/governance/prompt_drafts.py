"""QS supplies current administration scope; saving a draft never activates it."""

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from uuid import UUID

from qs_ai.domain.governance.manifest import AssetReference
from qs_ai.domain.governance.prompt_draft import (
    PromptDraft,
    PromptDraftContent,
    nonzero_uuid,
    positive,
    valid_reason,
    validate_target,
)


@dataclass(frozen=True)
class DraftScope:
    organization_id: int
    operator_user_id: int

    def __post_init__(self) -> None:
        if not positive(self.organization_id) or not positive(self.operator_user_id):
            raise ValueError("Trusted draft management scope required")


@dataclass(frozen=True)
class CreatePromptDraft:
    draft_id: UUID
    command_id: UUID
    source: AssetReference
    template_id: str
    target_version: str
    reason: str

    def __post_init__(self) -> None:
        validate_target(self.template_id, self.target_version)
        if (
            not nonzero_uuid(self.draft_id)
            or not nonzero_uuid(self.command_id)
            or not isinstance(self.source, AssetReference)
            or not valid_reason(self.reason)
        ):
            raise ValueError("Draft source and audit are required")


@dataclass(frozen=True)
class RevisePromptDraft:
    draft_id: UUID
    command_id: UUID
    expected_revision: int
    content: PromptDraftContent
    reason: str

    def __post_init__(self) -> None:
        if (
            not nonzero_uuid(self.draft_id)
            or not nonzero_uuid(self.command_id)
            or not positive(self.expected_revision)
            or not isinstance(self.content, PromptDraftContent)
            or not valid_reason(self.reason)
        ):
            raise ValueError("Draft revision, content and audit are required")


class PromptDraftStore(Protocol):
    async def get(
        self, scope: DraftScope, draft_id: UUID, revision: int | None = None
    ) -> PromptDraft: ...

    async def get_receipt(self, scope: DraftScope, command_id: UUID) -> PromptDraft: ...

    async def apply(
        self, scope: DraftScope, command: CreatePromptDraft | RevisePromptDraft, at: datetime
    ) -> PromptDraft: ...
