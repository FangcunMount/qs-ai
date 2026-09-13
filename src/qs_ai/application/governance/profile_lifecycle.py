"""Profile lifecycle is a read projection of registered assets and publication authority."""

import base64
import binascii
import json
from dataclasses import dataclass
from typing import Literal, Protocol

from qs_ai.application.governance.asset_catalog import validate_identity, validate_version
from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.domain.governance.manifest import AssetReference

ProfileStatus = Literal["draft", "published", "disabled"]


@dataclass(frozen=True)
class ProfileLifecycleQuery:
    identity: str = ""
    status: str = ""
    limit: int = 20
    cursor: str = ""

    def __post_init__(self) -> None:
        validate_identity(self.identity, optional=True)
        if (
            self.status not in {"", "draft", "published", "disabled"}
            or type(self.limit) is not int
            or not 1 <= self.limit <= 50
        ):
            raise ValueError("Invalid Profile lifecycle query")
        self.after()

    def after(self) -> tuple[str, str] | None:
        if not isinstance(self.cursor, str) or len(self.cursor) > 4096:
            raise ValueError("Invalid Profile cursor")
        if not self.cursor:
            return None
        try:
            value = json.loads(base64.b64decode(self.cursor, altchars=b"-_", validate=True))
            if (
                not isinstance(value, list)
                or len(value) != 5
                or value[:3] != [1, self.identity, self.status]
            ):
                raise ValueError("Profile cursor belongs to another query")
            identity, version = value[3:]
            validate_identity(identity)
            validate_version(version)
            if self.identity and identity != self.identity:
                raise ValueError("Profile cursor differs from identity")
            return identity, version
        except (binascii.Error, UnicodeError, TypeError, ValueError) as error:
            raise ValueError("Invalid Profile cursor") from error

    def next_cursor(self, reference: AssetReference) -> str:
        return base64.urlsafe_b64encode(
            json.dumps(
                [1, self.identity, self.status, reference.identity, reference.version],
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode()
        ).decode()


@dataclass(frozen=True)
class ProfileLifecycle:
    reference: AssetReference
    status: ProfileStatus
    source_ref: str
    imported_at: str
    active_publication_id: str
    active_run_id: str
    selector_version: int
    selector_changed_at: str
    inactive_reason: str


@dataclass(frozen=True)
class ProfileLifecyclePage:
    items: tuple[ProfileLifecycle, ...]
    next_cursor: str


class ProfileLifecycleReader(Protocol):
    async def list(
        self, scope: DraftScope, query: ProfileLifecycleQuery
    ) -> ProfileLifecyclePage: ...
    async def get(self, scope: DraftScope, identity: str, version: str) -> ProfileLifecycle: ...
