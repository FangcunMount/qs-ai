"""Organization-owned judge editing; freezing an asset does not approve or publish it."""

import hashlib
import json
import re
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Literal
from uuid import UUID

from qs_ai.domain.evaluation.identity import FrozenContractRef
from qs_ai.domain.governance.prompt_draft import DraftConflict, nonzero_uuid, positive, valid_reason


def bounded_markdown(markdown: str) -> None:
    if (
        not isinstance(markdown, str)
        or "\x00" in markdown
        or len(json.dumps(markdown, ensure_ascii=False).encode()) > 131072
    ):
        raise ValueError("Invalid semantic draft content")


def validate_semantic_markdown(markdown: str) -> None:
    bounded_markdown(markdown)
    blocks = re.findall(r"```text\n(.*?)\n```", markdown.replace("\r\n", "\n"), re.DOTALL)
    suffix = "\n\n{{semantic_evaluation_payload_json}}"
    if (
        len(blocks) != 3
        or any(not text.strip() for text in blocks)
        or not blocks[2].endswith(suffix)
        or not blocks[2][: -len(suffix)].strip()
        or markdown.count("{{") != 1
        or markdown.count("}}") != 1
    ):
        raise ValueError("Semantic prompt must retain three blocks and its single payload variable")


@dataclass(frozen=True)
class SemanticDraft:
    draft_id: UUID
    organization_id: int
    source_prompt: FrozenContractRef
    source_schema: FrozenContractRef
    source_owner_organization_id: int
    target_version: str
    revision: int
    markdown: str
    state: Literal["editing", "frozen"]
    command_id: UUID
    operator_user_id: int
    reason: str
    saved_at: datetime

    def __post_init__(self) -> None:
        if (
            not nonzero_uuid(self.draft_id)
            or not nonzero_uuid(self.command_id)
            or not all(
                positive(value)
                for value in (self.organization_id, self.operator_user_id, self.revision)
            )
            or type(self.source_owner_organization_id) is not int
            or self.source_owner_organization_id not in (0, self.organization_id)
            or not valid_reason(self.reason)
            or self.saved_at.tzinfo is None
            or self.saved_at.utcoffset() is None
            or self.state not in ("editing", "frozen")
        ):
            raise ValueError("Semantic draft requires complete scope and revision audit")
        bounded_markdown(self.markdown)
        FrozenContractRef(self.source_prompt.id, self.target_version, "sha256:" + "0" * 64)
        if self.target_version == self.source_prompt.version:
            raise ValueError("A new semantic prompt version is required")
        if self.state == "frozen":
            validate_semantic_markdown(self.markdown)

    def change(
        self,
        expected_revision: int,
        markdown: str,
        command_id: UUID,
        operator_user_id: int,
        reason: str,
        at: datetime,
        *,
        freeze: bool = False,
    ) -> "SemanticDraft":
        if (
            not positive(expected_revision)
            or expected_revision != self.revision
            or self.state != "editing"
        ):
            raise DraftConflict("Semantic draft changed or is already frozen")
        if at.tzinfo is None or at < self.saved_at:
            raise ValueError("Revision cannot precede prior audit")
        return replace(
            self,
            revision=self.revision + 1,
            markdown=markdown,
            state="frozen" if freeze else "editing",
            command_id=command_id,
            operator_user_id=operator_user_id,
            reason=reason,
            saved_at=at,
        )

    def asset_reference(self) -> FrozenContractRef:
        if self.state != "frozen":
            raise ValueError("Draft is not an immutable asset")
        return FrozenContractRef(
            self.source_prompt.id,
            self.target_version,
            "sha256:" + hashlib.sha256(self.markdown.encode()).hexdigest(),
        )
