"""Atomic judge editing, immutable revisions and original command receipts."""

import asyncio
import hashlib
import json
from dataclasses import asdict
from datetime import datetime
from uuid import UUID

from pydantic import TypeAdapter
from sqlalchemy import insert, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.governance.semantic_drafts import (
    CreateSemanticDraft,
    FreezeSemanticDraft,
    ReviseSemanticDraft,
    SemanticCommand,
)
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.domain.evaluation.semantic_draft import SemanticDraft, validate_semantic_markdown
from qs_ai.domain.governance.prompt_draft import DraftConflict
from qs_ai.domain.governance.schema import SchemaAsset
from qs_ai.infrastructure.persistence.mysql.asset_snapshot import AssetSnapshotReader
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_asset_registry import (
    duplicate,
    read_semantic_prompt,
)
from qs_ai.infrastructure.persistence.mysql.schema import schema_assets, semantic_prompt_assets
from qs_ai.infrastructure.persistence.mysql.schema import semantic_draft_commands as commands
from qs_ai.infrastructure.persistence.mysql.schema import semantic_draft_heads as heads
from qs_ai.infrastructure.persistence.mysql.schema import semantic_draft_versions as versions
from qs_ai.infrastructure.qs_server.semantic_assets import semantic_assets

SNAPSHOT = TypeAdapter(SemanticDraft)


def digest(raw: str) -> str:
    return hashlib.sha256(raw.encode()).hexdigest()


def decode(raw: str, checksum: str) -> SemanticDraft:
    if len(raw.encode()) > 262144 or digest(raw) != checksum:
        raise ValueError("Semantic draft checksum mismatch")
    value = SNAPSHOT.validate_json(raw, strict=True)
    if SNAPSHOT.dump_json(value).decode() != raw:
        raise ValueError("Noncanonical semantic draft")
    return value


async def prior(
    db: AsyncSession, scope: DraftScope, command_id: UUID, request: str | None = None
) -> SemanticDraft | None:
    row = (
        (
            await db.execute(
                select(commands).where(
                    commands.c.organization_id == scope.organization_id,
                    commands.c.command_id == str(command_id),
                )
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        return None
    if row["operator_user_id"] != scope.operator_user_id:
        raise NotFound("Semantic command unavailable")
    if request is not None and row["request_json"] != request:
        raise DraftConflict("Semantic command already used with different content")
    value = decode(row["receipt_json"], row["receipt_sha256"])
    if (
        value.organization_id,
        value.operator_user_id,
        str(value.command_id),
        str(value.draft_id),
    ) != (scope.organization_id, scope.operator_user_id, row["command_id"], row["draft_id"]):
        raise ValueError("Semantic command receipt index changed")
    return value


async def read(
    db: AsyncSession, scope: DraftScope, draft_id: UUID, revision: int = 0, *, lock: bool = False
) -> SemanticDraft:
    statement = select(heads.c.revision).where(
        heads.c.organization_id == scope.organization_id, heads.c.draft_id == str(draft_id)
    )
    if lock:
        statement = statement.with_for_update()
    current = (await db.execute(statement)).scalar_one_or_none()
    if current is None:
        raise NotFound("Semantic draft unavailable")
    if type(revision) is not int or revision < 0:
        raise ValueError("Invalid semantic revision")
    chosen = revision or current
    row = (
        (
            await db.execute(
                select(versions).where(
                    versions.c.organization_id == scope.organization_id,
                    versions.c.draft_id == str(draft_id),
                    versions.c.revision == chosen,
                )
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        raise NotFound("Semantic draft revision unavailable")
    value = decode(row["snapshot_json"], row["snapshot_sha256"])
    if (value.organization_id, str(value.draft_id), value.revision) != (
        scope.organization_id,
        str(draft_id),
        chosen,
    ):
        raise ValueError("Semantic draft index changed")
    return value


async def validate(db: AsyncSession, draft: SemanticDraft) -> None:
    schema_id, version = draft.source_schema.version.rsplit("/", 1)
    schema = await AssetSnapshotReader(db, schema_assets, SchemaAsset).get(schema_id, version)
    if schema is None or schema.fingerprint != draft.source_schema.fingerprint:
        raise ValueError("Original semantic schema unavailable")
    await asyncio.to_thread(validate_semantic_markdown, draft.markdown)
    # Validate the exact immutable output contract; a draft cannot introduce another contract.
    reference = draft.asset_reference() if draft.state == "frozen" else draft.source_prompt
    if draft.state != "frozen":
        from dataclasses import replace

        reference = replace(reference, fingerprint="sha256:" + digest(draft.markdown))
    await asyncio.to_thread(
        semantic_assets, draft.markdown, schema.definition_json, reference, draft.source_schema
    )


class MySQLSemanticDrafts:
    def __init__(self, transactions: Transactions) -> None:
        self.transactions = transactions

    async def get(self, scope: DraftScope, draft_id: UUID, revision: int = 0) -> SemanticDraft:
        async with self.transactions.open() as db:
            return await read(db, scope, draft_id, revision)

    async def receipt(self, scope: DraftScope, command_id: UUID) -> SemanticDraft:
        async with self.transactions.open() as db:
            value = await prior(db, scope, command_id)
            if value is None:
                raise NotFound("Semantic command unavailable")
            return value

    async def validate(self, scope: DraftScope, draft_id: UUID, revision: int) -> SemanticDraft:
        async with self.transactions.open() as db:
            value = await read(db, scope, draft_id, revision)
            await validate(db, value)
            return value

    async def apply(
        self, scope: DraftScope, command: SemanticCommand, at: datetime
    ) -> SemanticDraft:
        if at.tzinfo is None or at.utcoffset() is None:
            raise ValueError("Command requires server time")
        request = json.dumps(
            {"scope": asdict(scope), "action": type(command).__name__, "command": asdict(command)},
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":"),
            default=str,
        )
        try:
            async with self.transactions.open() as db:
                await db.connection(execution_options={"isolation_level": "READ COMMITTED"})
                previous = await prior(db, scope, command.command_id, request)
                if previous is not None:
                    return previous
                if isinstance(command, CreateSemanticDraft):
                    source = await read_semantic_prompt(
                        db,
                        command.source_prompt,
                        owner_organization_id=command.source_owner_organization_id,
                        requesting_organization_id=scope.organization_id,
                    )
                    value = SemanticDraft(
                        command.draft_id,
                        scope.organization_id,
                        command.source_prompt,
                        command.source_schema,
                        command.source_owner_organization_id,
                        command.target_version,
                        1,
                        source.markdown,
                        "editing",
                        command.command_id,
                        scope.operator_user_id,
                        command.reason,
                        at,
                    )
                    await validate(db, value)
                    await db.execute(
                        insert(heads).values(
                            organization_id=scope.organization_id,
                            draft_id=str(command.draft_id),
                            revision=1,
                        )
                    )
                else:
                    current = await read(db, scope, command.draft_id, lock=True)
                    previous = await prior(db, scope, command.command_id, request)
                    if previous is not None:
                        return previous
                    value = current.change(
                        command.expected_revision,
                        command.markdown
                        if isinstance(command, ReviseSemanticDraft)
                        else current.markdown,
                        command.command_id,
                        scope.operator_user_id,
                        command.reason,
                        at,
                        freeze=isinstance(command, FreezeSemanticDraft),
                    )
                    if isinstance(command, FreezeSemanticDraft):
                        await validate(db, value)
                        reference = value.asset_reference()
                        await db.execute(
                            insert(semantic_prompt_assets).values(
                                organization_id=scope.organization_id,
                                asset_id=reference.id,
                                version=reference.version,
                                fingerprint=reference.fingerprint,
                                markdown=value.markdown,
                                source_ref=f"semantic-draft:{value.draft_id}:{value.revision}",
                                imported_by=f"user:{scope.operator_user_id}",
                            )
                        )
                    await db.execute(
                        update(heads)
                        .where(
                            heads.c.organization_id == scope.organization_id,
                            heads.c.draft_id == str(command.draft_id),
                        )
                        .values(revision=value.revision)
                    )
                raw = SNAPSHOT.dump_json(value).decode()
                await db.execute(
                    insert(versions).values(
                        organization_id=scope.organization_id,
                        draft_id=str(command.draft_id),
                        revision=value.revision,
                        snapshot_json=raw,
                        snapshot_sha256=digest(raw),
                    )
                )
                await db.execute(
                    insert(commands).values(
                        organization_id=scope.organization_id,
                        command_id=str(command.command_id),
                        operator_user_id=scope.operator_user_id,
                        draft_id=str(command.draft_id),
                        request_json=request,
                        receipt_json=raw,
                        receipt_sha256=digest(raw),
                    )
                )
                await db.commit()
                return value
        except IntegrityError as error:
            if not duplicate(error):
                raise
        async with self.transactions.open() as db:
            previous = await prior(db, scope, command.command_id, request)
            if previous is not None:
                return previous
        raise DraftConflict("Semantic draft or target asset already exists")
