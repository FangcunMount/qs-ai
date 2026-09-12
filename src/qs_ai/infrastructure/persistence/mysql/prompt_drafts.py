"""Append revision and move its head atomically; editing never writes Prompt assets."""

import hashlib
import json
from dataclasses import asdict
from datetime import datetime
from typing import Any, cast
from uuid import UUID

from pydantic import TypeAdapter
from sqlalchemy import insert, select, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.governance.prompt_drafts import (
    CreatePromptDraft,
    DraftScope,
    RevisePromptDraft,
)
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.domain.governance.prompt import PromptAsset
from qs_ai.domain.governance.prompt_draft import (
    DraftConflict,
    PromptDraft,
    PromptDraftContent,
    nonzero_uuid,
    positive,
)
from qs_ai.infrastructure.persistence.mysql.asset_snapshot import AssetSnapshotReader
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.schema import (
    prompt_assets,
    prompt_draft_freezes,
)
from qs_ai.infrastructure.persistence.mysql.schema import (
    prompt_draft_revisions as revisions,
)
from qs_ai.infrastructure.persistence.mysql.schema import (
    prompt_drafts as heads,
)

SNAPSHOT = TypeAdapter(PromptDraft)


def request_json(scope: DraftScope, command: CreatePromptDraft | RevisePromptDraft) -> str:
    return json.dumps(
        {
            "action": "create" if isinstance(command, CreatePromptDraft) else "revise",
            "scope": asdict(scope),
            "command": asdict(command),
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
        allow_nan=False,
    )


def decode(row: Any) -> PromptDraft:
    raw = row["snapshot_json"]
    if (
        len(raw.encode()) > 262144
        or hashlib.sha256(raw.encode()).hexdigest() != row["snapshot_sha256"]
    ):
        raise ValueError("Prompt draft snapshot checksum mismatch")
    draft = SNAPSHOT.validate_json(raw, strict=True)
    if SNAPSHOT.dump_json(draft).decode() != raw or (
        str(draft.draft_id),
        draft.revision,
        draft.organization_id,
        draft.operator_user_id,
        str(draft.command_id),
    ) != (
        row["draft_id"],
        row["revision"],
        row["organization_id"],
        row["operator_user_id"],
        row["command_id"],
    ):
        raise ValueError("Prompt draft snapshot differs from revision index")
    if len(row["request_json"].encode()) > 262144:
        raise ValueError("Prompt draft command exceeds limit")
    original = json.loads(row["request_json"])
    command, scope = original["command"], original["scope"]
    if (
        scope
        != {"organization_id": draft.organization_id, "operator_user_id": draft.operator_user_id}
        or command["command_id"] != str(draft.command_id)
        or command["draft_id"] != str(draft.draft_id)
        or command["reason"] != draft.reason
    ):
        raise ValueError("Prompt draft revision audit changed")
    if original["action"] == "create":
        if (
            draft.revision != 1
            or command["source"] != asdict(draft.source)
            or command["template_id"] != draft.template_id
            or command["target_version"] != draft.target_version
        ):
            raise ValueError("Prompt draft creation audit changed")
    elif original["action"] == "revise":
        if command["expected_revision"] != draft.revision - 1 or command["content"] != json.loads(
            json.dumps(asdict(draft.content))
        ):
            raise ValueError("Prompt draft edit audit changed")
    else:
        raise ValueError("Unknown draft operation")
    return draft


async def replay(
    db: AsyncSession, scope: DraftScope, command_id: UUID, request: str
) -> PromptDraft | None:
    row = (
        (await db.execute(select(revisions).where(revisions.c.command_id == str(command_id))))
        .mappings()
        .one_or_none()
    )
    if row is None:
        return None
    if (row["organization_id"], row["operator_user_id"]) != (
        scope.organization_id,
        scope.operator_user_id,
    ):
        raise NotFound("Draft command unavailable")
    if row["request_json"] != request:
        raise DraftConflict("Draft command already used for a different request")
    return decode(row)


async def read_draft(
    db: AsyncSession,
    scope: DraftScope,
    draft_id: UUID,
    revision: int | None = None,
    *,
    lock: bool = False,
) -> PromptDraft:
    if not nonzero_uuid(draft_id) or revision is not None and not positive(revision):
        raise ValueError("Valid draft identity and revision required")
    query = select(heads).where(
        heads.c.draft_id == str(draft_id), heads.c.organization_id == scope.organization_id
    )
    head = (await db.execute(query.with_for_update() if lock else query)).mappings().one_or_none()
    if head is None or revision is not None and revision > head["revision"]:
        raise NotFound("Prompt draft revision unavailable")
    row = (
        (
            await db.execute(
                select(revisions).where(
                    revisions.c.draft_id == str(draft_id),
                    revisions.c.revision == (head["revision"] if revision is None else revision),
                    revisions.c.organization_id == scope.organization_id,
                )
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        raise ValueError("Prompt draft revision history incomplete")
    return decode(row)


async def apply_draft(
    db: AsyncSession,
    scope: DraftScope,
    command: CreatePromptDraft | RevisePromptDraft,
    at: datetime,
) -> PromptDraft:
    """Caller supplies a READ COMMITTED transaction and owns commit/rollback."""
    if not isinstance(at, datetime) or at.tzinfo is None or at.utcoffset() is None:
        raise ValueError("Server revision time must have a time zone")
    request = request_json(scope, command)
    prior = await replay(db, scope, command.command_id, request)
    if prior is not None:
        return prior
    if isinstance(command, CreatePromptDraft):
        reader = AssetSnapshotReader(db, prompt_assets, PromptAsset)
        source = await reader.get(command.source.identity, command.source.version)
        if source is None or (
            source.template_id,
            source.version,
            source.fingerprint,
            source.package_sha256,
        ) != (
            command.source.identity,
            command.source.version,
            command.source.fingerprint,
            command.source.content_sha256,
        ):
            raise ValueError("Draft source does not match immutable Prompt asset")
        if await reader.get(command.template_id, command.target_version) is not None:
            raise DraftConflict("Target Prompt version is already immutable")
        package = json.loads(source.package_json)
        content = PromptDraftContent(
            package["SystemMessage"],
            package["TaskTemplate"],
            package["DataPreamble"],
            tuple(package["AllowedPlaceholders"]),
        )
        draft = PromptDraft(
            command.draft_id,
            scope.organization_id,
            command.template_id,
            command.target_version,
            command.source,
            1,
            content,
            command.command_id,
            scope.operator_user_id,
            command.reason,
            at,
        )
        await db.execute(
            insert(heads).values(
                draft_id=str(draft.draft_id), organization_id=scope.organization_id, revision=0
            )
        )
    else:
        previous = await read_draft(db, scope, command.draft_id, lock=True)
        # The same command may have committed while this transaction waited
        # for the draft head lock. READ COMMITTED makes its receipt visible.
        prior = await replay(db, scope, command.command_id, request)
        if prior is not None:
            return prior
        if (
            await db.execute(
                select(prompt_draft_freezes.c.command_id).where(
                    prompt_draft_freezes.c.draft_id == str(command.draft_id)
                )
            )
        ).first() is not None:
            raise DraftConflict("Frozen draft cannot be edited; create a new target version")
        draft = previous.revise(
            command.expected_revision,
            command.content,
            command.command_id,
            scope.operator_user_id,
            command.reason,
            at,
        )
    raw = SNAPSHOT.dump_json(draft).decode()
    await db.execute(
        insert(revisions).values(
            command_id=str(draft.command_id),
            draft_id=str(draft.draft_id),
            revision=draft.revision,
            organization_id=scope.organization_id,
            operator_user_id=scope.operator_user_id,
            request_json=request,
            snapshot_json=raw,
            snapshot_sha256=hashlib.sha256(raw.encode()).hexdigest(),
        )
    )
    result = await db.execute(
        update(heads)
        .where(
            heads.c.draft_id == str(draft.draft_id),
            heads.c.revision == draft.revision - 1,
            heads.c.organization_id == scope.organization_id,
        )
        .values(revision=draft.revision)
    )
    if cast(CursorResult[Any], result).rowcount != 1:
        raise DraftConflict("Prompt draft revision changed")
    return draft


class MySQLPromptDrafts:
    def __init__(self, transactions: Transactions) -> None:
        self.transactions = transactions

    async def get(
        self, scope: DraftScope, draft_id: UUID, revision: int | None = None
    ) -> PromptDraft:
        async with self.transactions.open() as db:
            return await read_draft(db, scope, draft_id, revision)

    async def get_receipt(self, scope: DraftScope, command_id: UUID) -> PromptDraft:
        if not nonzero_uuid(command_id):
            raise ValueError("Valid command identity required")
        async with self.transactions.open() as db:
            row = (
                (
                    await db.execute(
                        select(revisions).where(
                            revisions.c.command_id == str(command_id),
                            revisions.c.organization_id == scope.organization_id,
                            revisions.c.operator_user_id == scope.operator_user_id,
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                raise NotFound("Draft command unavailable")
            return decode(row)

    async def apply(
        self, scope: DraftScope, command: CreatePromptDraft | RevisePromptDraft, at: datetime
    ) -> PromptDraft:
        try:
            async with self.transactions.open() as db:
                await db.connection(execution_options={"isolation_level": "READ COMMITTED"})
                draft = await apply_draft(db, scope, command, at)
                await db.commit()
                return draft
        except IntegrityError as error:
            if error.orig is None or not error.orig.args or error.orig.args[0] != 1062:
                raise
        # A competing create/edit may have committed the same command. Its
        # transaction won; after rollback, read the original immutable receipt.
        async with self.transactions.open() as db:
            prior = await replay(db, scope, command.command_id, request_json(scope, command))
            if prior is not None:
                return prior
        raise DraftConflict("Draft identity or revision already exists")
