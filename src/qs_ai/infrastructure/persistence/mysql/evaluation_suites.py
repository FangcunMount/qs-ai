"""Register and resolve complete suite bytes, retaining all original evaluation obligations."""

import hashlib
from datetime import datetime
from uuid import UUID

from pydantic import TypeAdapter
from sqlalchemy import RowMapping, insert, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.governance.suite_registration import RegisterSuite, SuiteRegistrationReceipt
from qs_ai.application.interpretation.manifest import build_generation_manifest
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.domain.evaluation.identity import FrozenContractRef
from qs_ai.domain.governance.profile import AssetConflict, ProfileAsset
from qs_ai.domain.governance.prompt import PromptAsset
from qs_ai.domain.governance.prompt_draft import nonzero_uuid
from qs_ai.domain.governance.route import RouteAsset
from qs_ai.domain.governance.schema import SchemaAsset
from qs_ai.infrastructure.persistence.mysql.asset_snapshot import AssetSnapshotReader
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_suites as table
from qs_ai.infrastructure.persistence.mysql.schema import (
    profile_assets,
    prompt_assets,
    route_assets,
    schema_assets,
)
from qs_ai.infrastructure.qs_server.evaluation_suite import (
    SUITE_FILES,
    V6_PUBLISHED,
    FrozenSuite,
    derive_suite,
    load_suite,
)

RECEIPT = TypeAdapter(SuiteRegistrationReceipt)


def decode_record(row: RowMapping) -> tuple[FrozenSuite, SuiteRegistrationReceipt]:
    suite = load_suite(
        FrozenContractRef(row["suite_id"], row["suite_version"], row["fingerprint"]),
        definition_json=row["definition_json"],
    )
    raw = row["receipt_json"]
    if (
        len(raw.encode()) > 32768
        or hashlib.sha256(raw.encode()).hexdigest() != row["receipt_sha256"]
    ):
        raise ValueError("Suite registration receipt changed")
    receipt = RECEIPT.validate_json(raw, strict=True)
    if RECEIPT.dump_json(receipt).decode() != raw or (
        receipt.suite,
        receipt.manifest,
        str(receipt.command.command_id),
        receipt.scope.organization_id,
        receipt.scope.operator_user_id,
    ) != (
        suite.reference,
        suite.manifest,
        row["command_id"],
        row["organization_id"],
        row["operator_user_id"],
    ):
        raise ValueError("Suite receipt differs from asset index")
    if receipt.command.source != V6_PUBLISHED:
        raise ValueError("Suite registration source changed")
    return suite, receipt


async def load_registered_suite(db: AsyncSession, reference: FrozenContractRef) -> FrozenSuite:
    if reference in SUITE_FILES:
        return load_suite(reference)
    row = (
        (
            await db.execute(
                select(table).where(
                    table.c.suite_id == reference.id, table.c.suite_version == reference.version
                )
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        raise ValueError("Registered evaluation suite unavailable")
    suite, _ = decode_record(row)
    if suite.reference != reference:
        raise ValueError("Registered evaluation suite fingerprint mismatch")
    return suite


async def prior_receipt(
    db: AsyncSession, scope: DraftScope, command_id: UUID
) -> SuiteRegistrationReceipt | None:
    row = (
        (await db.execute(select(table).where(table.c.command_id == str(command_id))))
        .mappings()
        .one_or_none()
    )
    if row is None:
        return None
    if (row["organization_id"], row["operator_user_id"]) != (
        scope.organization_id,
        scope.operator_user_id,
    ):
        raise NotFound("Suite command unavailable")
    return decode_record(row)[1]


async def apply_registration(
    db: AsyncSession, scope: DraftScope, command: RegisterSuite, at: datetime
) -> SuiteRegistrationReceipt:
    prior = await prior_receipt(db, scope, command.command_id)
    if prior is not None:
        if prior.command != command:
            raise AssetConflict("Suite command already used")
        return prior
    if command.source != V6_PUBLISHED:
        raise ValueError("Confirmed published-input case source required")
    manifest = await build_generation_manifest(
        AssetSnapshotReader(db, profile_assets, ProfileAsset),
        AssetSnapshotReader(db, prompt_assets, PromptAsset),
        AssetSnapshotReader(db, route_assets, RouteAsset),
        AssetSnapshotReader(db, schema_assets, SchemaAsset),
        profile_id=command.profile.identity,
        profile_version=command.profile.version,
        route_revision=command.generation_route.version,
    )
    if (manifest.profile, manifest.prompt, manifest.generation_route) != (
        command.profile,
        command.prompt,
        command.generation_route,
    ):
        raise ValueError("Confirmed suite assets changed")
    profile = await AssetSnapshotReader(db, profile_assets, ProfileAsset).get(
        command.profile.identity, command.profile.version
    )
    if profile is None:
        raise ValueError("Profile unavailable")
    suite = derive_suite(command.suite_id, command.suite_version, profile, manifest)
    receipt = SuiteRegistrationReceipt(scope, command, suite.reference, manifest, at)
    raw = RECEIPT.dump_json(receipt).decode()
    if len(raw.encode()) > 32768:
        raise ValueError("Suite receipt exceeds limit")
    await db.execute(
        insert(table).values(
            suite_id=suite.reference.id,
            suite_version=suite.reference.version,
            fingerprint=suite.reference.fingerprint,
            definition_json=suite.definition_json,
            command_id=str(command.command_id),
            organization_id=scope.organization_id,
            operator_user_id=scope.operator_user_id,
            receipt_json=raw,
            receipt_sha256=hashlib.sha256(raw.encode()).hexdigest(),
        )
    )
    return receipt


class MySQLSuiteRegistrar:
    def __init__(self, transactions: Transactions) -> None:
        self.transactions = transactions

    async def register(
        self, scope: DraftScope, command: RegisterSuite, at: datetime
    ) -> SuiteRegistrationReceipt:
        try:
            async with self.transactions.open() as db:
                await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
                receipt = await apply_registration(db, scope, command, at)
                await db.commit()
                return receipt
        except IntegrityError as error:
            if error.orig is None or not error.orig.args or error.orig.args[0] != 1062:
                raise
        async with self.transactions.open() as db:
            prior = await prior_receipt(db, scope, command.command_id)
            if prior is not None and prior.command == command:
                return prior
        raise AssetConflict("Suite version or command already registered")

    async def get_receipt(self, scope: DraftScope, command_id: UUID) -> SuiteRegistrationReceipt:
        if not nonzero_uuid(command_id):
            raise ValueError("Canonical nonzero command UUID required")
        async with self.transactions.open() as db:
            await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            receipt = await prior_receipt(db, scope, command_id)
            if receipt is None:
                raise NotFound("Suite command unavailable")
            return receipt
