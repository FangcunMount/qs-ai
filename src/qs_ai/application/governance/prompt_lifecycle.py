"""Current editability is a read model, separate from immutable command receipts."""

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from uuid import UUID

from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.domain.governance.manifest import AssetReference
from qs_ai.domain.governance.prompt_draft import PromptDraft


@dataclass(frozen=True)
class FrozenPromptVersion:
    asset: AssetReference
    revision: int
    frozen_at: datetime


@dataclass(frozen=True)
class PromptDraftLifecycle:
    draft: PromptDraft
    frozen: FrozenPromptVersion | None


class PromptLifecycleReader(Protocol):
    async def get(self, scope: DraftScope, draft_id: UUID) -> PromptDraftLifecycle: ...
