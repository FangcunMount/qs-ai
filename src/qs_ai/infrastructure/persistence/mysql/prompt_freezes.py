"""Native asset and freeze audit commit together under the draft head lock."""

import hashlib
from dataclasses import asdict
from datetime import datetime
from uuid import UUID

from pydantic import TypeAdapter
from sqlalchemy import insert, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.governance.prompt_freeze import (
    FreezePromptDraft,
    FrozenPromptReceipt,
    freeze_asset,
)
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.domain.governance.manifest import AssetReference
from qs_ai.domain.governance.prompt import PromptAsset
from qs_ai.domain.governance.prompt_draft import DraftConflict, nonzero_uuid
from qs_ai.domain.governance.prompt_origin import SYNTAX_VERSION
from qs_ai.infrastructure.persistence.mysql.asset_snapshot import AssetSnapshotReader
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.prompt_drafts import SNAPSHOT, read_draft
from qs_ai.infrastructure.persistence.mysql.schema import prompt_assets
from qs_ai.infrastructure.persistence.mysql.schema import prompt_draft_freezes as freezes

RECEIPT = TypeAdapter(FrozenPromptReceipt)


def checksum(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def source_ref(receipt: FrozenPromptReceipt) -> str:
    return f"qs-ai:draft:{receipt.command.draft_id}:{receipt.command.expected_revision}"


def actor(receipt: FrozenPromptReceipt) -> str:
    return f"qs:org/{receipt.scope.organization_id}/user/{receipt.scope.operator_user_id}"


async def load_receipt(
    db: AsyncSession, scope: DraftScope, command_id: UUID
) -> FrozenPromptReceipt | None:
    row = (
        (await db.execute(select(freezes).where(freezes.c.command_id == str(command_id))))
        .mappings()
        .one_or_none()
    )
    if row is None:
        return None
    if (row["organization_id"], row["operator_user_id"]) != (
        scope.organization_id,
        scope.operator_user_id,
    ):
        raise NotFound("Freeze command unavailable")
    raw = row["receipt_json"].encode()
    if len(raw) > 32768 or checksum(raw) != row["receipt_sha256"]:
        raise ValueError("Freeze receipt checksum mismatch")
    receipt = RECEIPT.validate_json(raw, strict=True)
    if (
        RECEIPT.dump_json(receipt) != raw
        or receipt.scope != scope
        or receipt.command.command_id != command_id
        or str(receipt.command.draft_id) != row["draft_id"]
    ):
        raise ValueError("Freeze receipt differs from its index")
    draft = await read_draft(db, scope, receipt.command.draft_id, receipt.command.expected_revision)
    if (
        checksum(SNAPSHOT.dump_json(draft)) != receipt.snapshot_sha256
        or receipt.frozen_at < draft.saved_at
    ):
        raise ValueError("Frozen draft revision changed")
    expected = freeze_asset(draft, receipt.snapshot_sha256)
    reference = AssetReference(
        expected.template_id, expected.version, expected.fingerprint, expected.package_sha256
    )
    actual = await AssetSnapshotReader(db, prompt_assets, PromptAsset).get(
        reference.identity, reference.version
    )
    provenance = (
        await db.execute(
            select(prompt_assets.c.source_ref, prompt_assets.c.imported_by).where(
                prompt_assets.c.template_id == reference.identity,
                prompt_assets.c.version == reference.version,
            )
        )
    ).one_or_none()
    if (
        actual != expected
        or receipt.asset != reference
        or provenance is None
        or tuple(provenance) != (source_ref(receipt), actor(receipt))
    ):
        raise ValueError("Frozen native Prompt asset or provenance changed")
    return receipt


async def apply_freeze(
    db: AsyncSession, scope: DraftScope, command: FreezePromptDraft, at: datetime
) -> FrozenPromptReceipt:
    """Caller owns READ COMMITTED transaction. Replays return the original receipt."""
    prior = await load_receipt(db, scope, command.command_id)
    if prior is not None:
        if prior.command != command:
            raise DraftConflict("Freeze command already used")
        return prior
    draft = await read_draft(db, scope, command.draft_id, lock=True)
    prior = await load_receipt(db, scope, command.command_id)
    if prior is not None:
        if prior.command != command:
            raise DraftConflict("Freeze command already used")
        return prior
    if (
        draft.revision != command.expected_revision
        or (
            await db.execute(
                select(freezes.c.command_id).where(freezes.c.draft_id == str(command.draft_id))
            )
        ).first()
        is not None
    ):
        raise DraftConflict("Draft revision changed or is already frozen")
    if (
        not isinstance(at, datetime)
        or at.tzinfo is None
        or at.utcoffset() is None
        or at < draft.saved_at
    ):
        raise ValueError("Freeze time must follow the saved revision")
    reader = AssetSnapshotReader(db, prompt_assets, PromptAsset)
    source = await reader.get(draft.source.identity, draft.source.version)
    if (
        source is None
        or AssetReference(
            source.template_id, source.version, source.fingerprint, source.package_sha256
        )
        != draft.source
    ):
        raise ValueError("Draft source asset changed")
    snapshot = checksum(SNAPSHOT.dump_json(draft))
    asset = freeze_asset(draft, snapshot)
    if await reader.get(asset.template_id, asset.version) is not None:
        raise DraftConflict("Target Prompt version is already immutable")
    receipt = FrozenPromptReceipt(
        scope,
        command,
        AssetReference(asset.template_id, asset.version, asset.fingerprint, asset.package_sha256),
        snapshot,
        SYNTAX_VERSION,
        at,
    )
    await db.execute(
        insert(prompt_assets).values(
            **asdict(asset), source_ref=source_ref(receipt), imported_by=actor(receipt)
        )
    )
    raw = RECEIPT.dump_json(receipt)
    await db.execute(
        insert(freezes).values(
            command_id=str(command.command_id),
            draft_id=str(command.draft_id),
            organization_id=scope.organization_id,
            operator_user_id=scope.operator_user_id,
            receipt_json=raw.decode(),
            receipt_sha256=checksum(raw),
        )
    )
    return receipt


class MySQLPromptFreezer:
    def __init__(self, transactions: Transactions) -> None:
        self.transactions = transactions

    async def freeze(
        self, scope: DraftScope, command: FreezePromptDraft, at: datetime
    ) -> FrozenPromptReceipt:
        try:
            async with self.transactions.open() as db:
                await db.connection(execution_options={"isolation_level": "READ COMMITTED"})
                receipt = await apply_freeze(db, scope, command, at)
                await db.commit()
                return receipt
        except IntegrityError as error:
            if error.orig is None or not error.orig.args or error.orig.args[0] != 1062:
                raise
        async with self.transactions.open() as db:
            prior = await load_receipt(db, scope, command.command_id)
            if prior is not None and prior.command == command:
                return prior
        raise DraftConflict("Freeze command or target Prompt version already exists")

    async def get_receipt(self, scope: DraftScope, command_id: UUID) -> FrozenPromptReceipt:
        if not nonzero_uuid(command_id):
            raise ValueError("Valid freeze command required")
        async with self.transactions.open() as db:
            receipt = await load_receipt(db, scope, command_id)
            if receipt is None:
                raise NotFound("Freeze command unavailable")
            return receipt
