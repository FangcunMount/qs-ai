"""Register and resolve complete suite bytes, retaining all original evaluation obligations."""

import asyncio
import hashlib
import json
from dataclasses import asdict, replace
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
    BASELINE_SUITE_FILES,
    SUITE_FILES,
    FrozenSuite,
    derive_suite,
    load_suite,
)

RECEIPT = TypeAdapter(SuiteRegistrationReceipt)


def decode_record(
    row: RowMapping, source: FrozenSuite | None = None
) -> tuple[FrozenSuite, SuiteRegistrationReceipt | None]:
    suite = load_suite(
        FrozenContractRef(row["suite_id"], row["suite_version"], row["fingerprint"]),
        definition_json=row["definition_json"],
        source=source,
    )
    if row["organization_id"] == 0:
        if (
            suite.reference not in SUITE_FILES
            or not row["source_ref"]
            or not row["imported_by"]
            or any(
                row[key] is not None
                for key in ("command_id", "operator_user_id", "receipt_json", "receipt_sha256")
            )
        ):
            raise ValueError("Invalid initialized suite provenance")
        return suite, None
    raw = row["receipt_json"]
    if (
        len(raw.encode()) > 32768
        or hashlib.sha256(raw.encode()).hexdigest() != row["receipt_sha256"]
    ):
        raise ValueError("Suite registration receipt changed")
    receipt = RECEIPT.validate_json(raw, strict=True)
    if RECEIPT.dump_json(receipt, exclude_defaults=True).decode() != raw or (
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
    if asdict(receipt.command.source) != json.loads(suite.definition_json)["derived_from"]:
        raise ValueError("Suite registration source changed")
    return suite, receipt


async def load_registered_suite(
    db: AsyncSession,
    reference: FrozenContractRef,
    *,
    organization_id: int,
    _seen: frozenset[tuple[str, str]] = frozenset(),
) -> FrozenSuite:
    if (reference.id, reference.version) in {(ref.id, ref.version) for ref in BASELINE_SUITE_FILES}:
        raise ValueError("Retired suite is a verification baseline, not an executable suite")
    if type(organization_id) is not int or organization_id <= 0:
        raise ValueError("Suite caller organization required")
    key = (reference.id, reference.version)
    if key in _seen or len(_seen) >= 64:
        raise ValueError("Suite source lineage cyclic or exceeds bound")
    row = (
        (
            await db.execute(
                select(table).where(
                    table.c.suite_id == reference.id,
                    table.c.suite_version == reference.version,
                    table.c.organization_id.in_((0, organization_id)),
                )
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is None or row["fingerprint"] != reference.fingerprint:
        raise ValueError("Exact scoped evaluation suite unavailable")
    source = None
    if row["organization_id"] != 0:
        try:
            parent = FrozenContractRef(**json.loads(row["definition_json"])["derived_from"])
        except (KeyError, TypeError) as error:
            raise ValueError("Suite source reference unavailable") from error
        source = await load_registered_suite(
            db, parent, organization_id=organization_id, _seen=_seen | {key}
        )
    suite, _ = await asyncio.to_thread(decode_record, row, source)
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
    suite = await load_registered_suite(
        db,
        FrozenContractRef(row["suite_id"], row["suite_version"], row["fingerprint"]),
        organization_id=scope.organization_id,
    )
    source = await load_registered_suite(
        db,
        FrozenContractRef(**json.loads(suite.definition_json)["derived_from"]),
        organization_id=scope.organization_id,
    )
    return (await asyncio.to_thread(decode_record, row, source))[1]


async def apply_registration(
    db: AsyncSession, scope: DraftScope, command: RegisterSuite, at: datetime
) -> SuiteRegistrationReceipt:
    prior = await prior_receipt(db, scope, command.command_id)
    if prior is not None:
        if prior.command != command:
            raise AssetConflict("Suite command already used")
        return prior
    source = await load_registered_suite(db, command.source, organization_id=scope.organization_id)
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
    suite = await asyncio.to_thread(
        derive_suite,
        command.suite_id,
        command.suite_version,
        profile,
        manifest,
        source=source,
        case_edits_json=command.case_edits_json,
    )
    from qs_ai.infrastructure.qs_server.evaluation_input import validate_suite_inputs

    if suite.input_schema is None:
        raise ValueError("Suite input schema required")
    await asyncio.to_thread(validate_suite_inputs, suite, suite.input_schema)
    receipt = SuiteRegistrationReceipt(scope, command, suite.reference, manifest, at)
    raw = RECEIPT.dump_json(receipt, exclude_defaults=True).decode()
    if len(raw.encode()) > 32768:
        raise ValueError("Suite receipt exceeds limit")
    from qs_ai.infrastructure.persistence.mysql.suite_contracts import encode, read

    bindings = await read(db, command.source, scope.organization_id)
    if command.semantic_prompt is not None:
        from qs_ai.infrastructure.persistence.mysql.evaluation_asset_registry import (
            read_semantic_prompt,
        )
        from qs_ai.infrastructure.qs_server.semantic_assets import semantic_assets

        asset = await read_semantic_prompt(
            db,
            command.semantic_prompt,
            owner_organization_id=command.semantic_owner_organization_id,
            requesting_organization_id=scope.organization_id,
        )
        schema_id, version = bindings.semantic_output_schema.version.rsplit("/", 1)
        schema = await AssetSnapshotReader(db, schema_assets, SchemaAsset).get(schema_id, version)
        if schema is None:
            raise ValueError("Fixed semantic schema unavailable")
        await asyncio.to_thread(
            semantic_assets,
            asset.markdown,
            schema.definition_json,
            asset.reference,
            bindings.semantic_output_schema,
        )
        bindings = replace(
            bindings,
            semantic_prompt=asset.reference,
            semantic_owner_organization_id=command.semantic_owner_organization_id,
        )
    contracts, contracts_sha = encode(bindings)
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
            contracts_json=contracts,
            contracts_sha256=contracts_sha,
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
