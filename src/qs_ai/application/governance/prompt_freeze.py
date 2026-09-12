"""Freeze valid template syntax, independently of Profile and model quality approval."""

import hashlib
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Protocol
from uuid import UUID

from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.interpretation.prompts import PLACEHOLDER, InvalidPrompt
from qs_ai.domain.governance.manifest import AssetReference
from qs_ai.domain.governance.prompt import PromptAsset
from qs_ai.domain.governance.prompt_draft import PromptDraft, nonzero_uuid, positive, valid_reason
from qs_ai.domain.governance.prompt_origin import (
    FORMAT,
    SYNTAX_VERSION,
    canonical,
    native_fingerprint,
)

SUPPORTED = frozenset(
    "locale focus_areas_json allowed_insight_kinds_json insight_min_items insight_max_items "
    "min_dimension_refs max_dimension_refs allow_parent_child_in_same_insight "
    "allowed_suggestion_origins_json allowed_suggestion_categories_json suggestion_min_items "
    "suggestion_max_items max_actions_per_item max_output_characters".split()
)


def freeze_asset(draft: PromptDraft, snapshot_sha256: str) -> PromptAsset:
    content = draft.content
    if not all(
        s.strip() for s in (content.system_message, content.task_template, content.data_preamble)
    ):
        raise InvalidPrompt("Prompt messages are required before freezing")
    if any(
        token in text
        for text in (content.system_message, content.data_preamble)
        for token in ("{{", "}}")
    ):
        raise InvalidPrompt("Dynamic system or data preamble is forbidden")
    allowed = content.allowed_placeholders
    if len(set(allowed)) != len(allowed) or any(
        not PLACEHOLDER.fullmatch(p) or p[2:-2] not in SUPPORTED for p in allowed
    ):
        raise InvalidPrompt("Unknown or duplicated placeholder declaration")
    if any(p not in allowed for p in PLACEHOLDER.findall(content.task_template)):
        raise InvalidPrompt("Undeclared task placeholder")
    remainder = PLACEHOLDER.sub("", content.task_template)
    if "{{" in remainder or "}}" in remainder:
        raise InvalidPrompt("Unresolved task placeholder")
    reference = {"TemplateID": draft.template_id, "Version": draft.target_version}
    data = {
        "Format": FORMAT,
        "Ref": reference,
        "Origin": {
            "kind": "draft_revision",
            "organization_id": draft.organization_id,
            "draft_id": str(draft.draft_id),
            "revision": draft.revision,
            "snapshot_sha256": snapshot_sha256,
            "source": asdict(draft.source),
            "validator_version": SYNTAX_VERSION,
        },
        "SystemMessage": content.system_message,
        "TaskTemplate": content.task_template,
        "DataPreamble": content.data_preamble,
        "AllowedPlaceholders": list(allowed),
    }
    fingerprint = native_fingerprint(data)
    reference["Fingerprint"] = fingerprint
    raw = canonical(data)
    return PromptAsset(
        draft.template_id,
        draft.target_version,
        fingerprint,
        hashlib.sha256(raw.encode()).hexdigest(),
        raw,
    )


@dataclass(frozen=True)
class FreezePromptDraft:
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
            raise ValueError("Explicit draft revision and freeze audit required")


@dataclass(frozen=True)
class FrozenPromptReceipt:
    scope: DraftScope
    command: FreezePromptDraft
    asset: AssetReference
    snapshot_sha256: str
    validator_version: str
    frozen_at: datetime

    def __post_init__(self) -> None:
        if (
            self.validator_version != SYNTAX_VERSION
            or self.frozen_at.tzinfo is None
            or self.frozen_at.utcoffset() is None
        ):
            raise ValueError("Freeze receipt requires server time and validator identity")


class PromptFreezer(Protocol):
    async def freeze(
        self, scope: DraftScope, command: FreezePromptDraft, at: datetime
    ) -> FrozenPromptReceipt: ...

    async def get_receipt(self, scope: DraftScope, command_id: UUID) -> FrozenPromptReceipt: ...
