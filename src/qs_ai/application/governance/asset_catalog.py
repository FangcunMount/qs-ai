"""Shared immutable configuration catalog; QS authorizes readers, receipts stay scoped."""

import base64
import binascii
import json
import re
from dataclasses import dataclass
from typing import Literal, Protocol

from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.domain.governance.manifest import AssetReference

AssetKind = Literal[
    "profile", "prompt", "route", "schema", "suite", "execution_policy", "gate_policy"
]
KINDS = ("profile", "prompt", "route", "schema", "suite", "execution_policy", "gate_policy")


def validate_identity(identity: str, *, optional: bool = False) -> None:
    if (
        not isinstance(identity, str)
        or len(identity) > 255
        or (
            not (optional and identity == "")
            and (not identity.strip() or identity != identity.strip())
        )
    ):
        raise ValueError("Invalid catalog identity")


def validate_version(version: str) -> None:
    if not isinstance(version, str) or not re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9._/-]{0,127}", version
    ):
        raise ValueError("Invalid catalog version")


@dataclass(frozen=True)
class CatalogQuery:
    kind: AssetKind
    identity: str = ""
    limit: int = 20
    cursor: str = ""

    def __post_init__(self) -> None:
        if self.kind not in KINDS or type(self.limit) is not int or not 1 <= self.limit <= 50:
            raise ValueError("Invalid catalog query")
        validate_identity(self.identity, optional=True)
        self.after()

    def after(self) -> tuple[str, str] | None:
        if not isinstance(self.cursor, str) or len(self.cursor) > 4096:
            raise ValueError("Invalid catalog cursor")
        if not self.cursor:
            return None
        try:
            data = json.loads(base64.b64decode(self.cursor, altchars=b"-_", validate=True))
            if (
                not isinstance(data, list)
                or len(data) != 5
                or data[:3] != [1, self.kind, self.identity]
            ):
                raise ValueError("Cursor belongs to another query")
            identity, version = data[3:]
            validate_identity(identity)
            validate_version(version)
            if self.identity and identity != self.identity:
                raise ValueError("Cursor identity differs from filter")
            return identity, version
        except (binascii.Error, UnicodeError, TypeError, ValueError) as error:
            raise ValueError("Invalid catalog cursor") from error

    def next_cursor(self, reference: AssetReference) -> str:
        return base64.urlsafe_b64encode(
            json.dumps(
                [1, self.kind, self.identity, reference.identity, reference.version],
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode()
        ).decode()


@dataclass(frozen=True)
class CatalogItem:
    kind: AssetKind
    reference: AssetReference


@dataclass(frozen=True)
class CatalogPage:
    items: tuple[CatalogItem, ...]
    next_cursor: str


@dataclass(frozen=True)
class CatalogDetail:
    item: CatalogItem
    definition_json: str


class AssetCatalog(Protocol):
    async def list(self, scope: DraftScope, query: CatalogQuery) -> CatalogPage: ...
    async def get(
        self, scope: DraftScope, kind: AssetKind, identity: str, version: str
    ) -> CatalogDetail: ...
