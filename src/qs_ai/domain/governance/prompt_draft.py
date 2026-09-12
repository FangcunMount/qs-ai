"""Editable Prompt revisions are not published assets or evaluation approvals."""

import json
import re
from dataclasses import asdict, dataclass, replace
from datetime import datetime
from uuid import UUID

from qs_ai.domain.governance.manifest import AssetReference


class DraftConflict(ValueError):
    pass


def positive(value: int) -> bool:
    return type(value) is int and 0 < value < 2**63


def nonzero_uuid(value: UUID) -> bool:
    return isinstance(value, UUID) and value.int != 0


def valid_reason(value: str) -> bool:
    return (
        isinstance(value, str)
        and bool(value.strip())
        and len(value.encode()) <= 1000
        and not any(c in value for c in "<>\x00")
    )


def validate_target(template_id: str, version: str) -> None:
    if not isinstance(template_id, str) or not re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9._/-]{0,127}", template_id
    ):
        raise ValueError("Invalid target Prompt identity")
    if not isinstance(version, str) or not re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9._/-]{0,127}", version
    ):
        raise ValueError("Invalid target Prompt version")


@dataclass(frozen=True)
class PromptDraftContent:
    system_message: str
    task_template: str
    data_preamble: str
    allowed_placeholders: tuple[str, ...]

    def __post_init__(self) -> None:
        # Incomplete text is saveable while editing. Rendering, release policy
        # validation and quality approval are separate operations.
        if any(
            not isinstance(value, str) or "\x00" in value
            for value in (self.system_message, self.task_template, self.data_preamble)
        ):
            raise ValueError("Draft text must be strings without NUL")
        if (
            not isinstance(self.allowed_placeholders, tuple)
            or len(self.allowed_placeholders) > 64
            or any(not isinstance(v, str) or len(v) > 128 for v in self.allowed_placeholders)
        ):
            raise ValueError("Invalid draft placeholder list")
        if len(json.dumps(asdict(self), ensure_ascii=False).encode()) > 131072:
            raise ValueError("Prompt draft exceeds limit")


@dataclass(frozen=True)
class PromptDraft:
    draft_id: UUID
    organization_id: int
    template_id: str
    target_version: str
    source: AssetReference
    revision: int
    content: PromptDraftContent
    command_id: UUID
    operator_user_id: int
    reason: str
    saved_at: datetime

    def __post_init__(self) -> None:
        validate_target(self.template_id, self.target_version)
        if (
            not nonzero_uuid(self.draft_id)
            or not nonzero_uuid(self.command_id)
            or not all(
                positive(v) for v in (self.organization_id, self.operator_user_id, self.revision)
            )
            or not isinstance(self.source, AssetReference)
            or not isinstance(self.content, PromptDraftContent)
            or not valid_reason(self.reason)
            or not isinstance(self.saved_at, datetime)
            or self.saved_at.tzinfo is None
            or self.saved_at.utcoffset() is None
            or (self.template_id, self.target_version)
            == (self.source.identity, self.source.version)
        ):
            raise ValueError("Draft requires a new target version and complete revision audit")

    def revise(
        self,
        expected_revision: int,
        content: PromptDraftContent,
        command_id: UUID,
        operator_user_id: int,
        reason: str,
        at: datetime,
    ) -> "PromptDraft":
        if not positive(expected_revision) or expected_revision != self.revision:
            raise DraftConflict("Prompt draft revision changed")
        if at.tzinfo is None or at.utcoffset() is None or at < self.saved_at:
            raise ValueError("Revision time cannot precede previous revision")
        return replace(
            self,
            revision=self.revision + 1,
            content=content,
            command_id=command_id,
            operator_user_id=operator_user_id,
            reason=reason,
            saved_at=at,
        )
