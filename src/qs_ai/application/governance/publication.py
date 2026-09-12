"""QS authorizes global configuration governance; AI owns atomic publication."""

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from uuid import UUID

from qs_ai.domain.governance.publication import (
    PublicationChange,
    PublicationPointer,
    ReleaseSelector,
    valid_version,
)


def valid_uuid(value: UUID | None, *, nullable: bool = False) -> bool:
    return value is None and nullable or isinstance(value, UUID) and value.int != 0


@dataclass(frozen=True)
class PublicationScope:
    organization_id: int
    operator_user_id: int

    def __post_init__(self) -> None:
        if not all(valid_version(value) for value in (self.organization_id, self.operator_user_id)):
            raise ValueError("Trusted publication scope required")

    @property
    def actor(self) -> str:
        return f"user:{self.operator_user_id}"


@dataclass(frozen=True)
class PublicationCommand:
    command_id: UUID
    selector: ReleaseSelector
    expected_version: int
    expected_active_id: UUID | None
    reason: str
    confirm: bool

    def __post_init__(self) -> None:
        if (
            not valid_uuid(self.command_id)
            or not isinstance(self.selector, ReleaseSelector)
            or not valid_version(self.expected_version, initial=True)
            or not valid_uuid(self.expected_active_id, nullable=True)
            or self.confirm is not True
            or not isinstance(self.reason, str)
            or not self.reason.strip()
            or len(self.reason.encode()) > 1000
            or any(c in self.reason for c in "<>")
        ):
            raise ValueError(
                "Explicit publication identity, version, reason and confirmation required"
            )


@dataclass(frozen=True)
class PublishConfiguration(PublicationCommand):
    run_id: UUID
    run_version: int
    release_fingerprint: str

    def __post_init__(self) -> None:
        super().__post_init__()
        if (
            not valid_uuid(self.run_id)
            or not valid_version(self.run_version)
            or not re.fullmatch(r"sha256:[0-9a-f]{64}", self.release_fingerprint)
        ):
            raise ValueError("Confirmed evaluation identity required")

    @property
    def action(self) -> str:
        return "publish"


@dataclass(frozen=True)
class MovePublication(PublicationCommand):
    target_id: UUID | None

    def __post_init__(self) -> None:
        super().__post_init__()
        if not valid_uuid(self.target_id, nullable=True):
            raise ValueError("Stored rollback publication identity required")

    @property
    def action(self) -> str:
        return "disable" if self.target_id is None else "rollback"


@dataclass(frozen=True)
class PublicationReceipt:
    command_id: UUID
    change: PublicationChange


class PublicationStore(Protocol):
    async def get_receipt(
        self, scope: PublicationScope, command_id: UUID
    ) -> PublicationReceipt: ...

    async def get(self, selector: ReleaseSelector) -> PublicationPointer: ...

    async def apply(
        self,
        scope: PublicationScope,
        command: PublishConfiguration | MovePublication,
        at: datetime,
    ) -> PublicationReceipt: ...
