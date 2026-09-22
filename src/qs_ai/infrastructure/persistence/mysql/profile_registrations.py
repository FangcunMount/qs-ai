"""Atomic immutable Profile registration and original-command audit receipts."""

import hashlib
import json
from dataclasses import asdict
from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import TypeAdapter
from sqlalchemy import insert, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.governance.profile_registration import (
    ProfileRegistrationReceipt,
    RegisterProfile,
)
from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.interpretation.manifest import build_generation_manifest
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.application.interpretation.prompt_assets import executable_prompt
from qs_ai.application.interpretation.prompts import render_prompt
from qs_ai.application.interpretation.route_assets import executable_route
from qs_ai.domain.governance.manifest import AssetReference, GenerationManifest
from qs_ai.domain.governance.profile import AssetConflict, ProfileAsset
from qs_ai.domain.governance.prompt import PromptAsset
from qs_ai.domain.governance.prompt_draft import nonzero_uuid
from qs_ai.domain.governance.route import RouteAsset
from qs_ai.domain.governance.schema import SchemaAsset
from qs_ai.infrastructure.persistence.mysql.asset_snapshot import AssetSnapshotReader
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.schema import (
    profile_assets,
    prompt_assets,
    route_assets,
    schema_assets,
)
from qs_ai.infrastructure.persistence.mysql.schema import (
    profile_registrations as registrations,
)
from qs_ai.infrastructure.qs_server.profiles import (
    canonical_definition,
    decode_profile_definition,
    decode_published_profile,
)

RECEIPT = TypeAdapter(ProfileRegistrationReceipt)


def checksum(raw: str) -> str:
    return hashlib.sha256(raw.encode()).hexdigest()


def reference(asset: ProfileAsset) -> AssetReference:
    return AssetReference(
        asset.profile_id, asset.version, asset.fingerprint, checksum(asset.definition_json)
    )


def profile_from(command: RegisterProfile) -> ProfileAsset:
    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("Duplicate Profile field")
            result[key] = value
        return result

    definition = decode_profile_definition(
        json.loads(command.definition_json, object_pairs_hook=unique)
    )
    raw = canonical_definition(definition.model_dump())
    return ProfileAsset(definition.profile_id, definition.version, "sha256:" + checksum(raw), raw)


def provenance(receipt: ProfileRegistrationReceipt) -> tuple[str, str]:
    return (
        f"qs-ai:profile-registration:{receipt.command.command_id}",
        f"qs:org/{receipt.scope.organization_id}/user/{receipt.scope.operator_user_id}",
    )


async def validate_source(db: AsyncSession, command: RegisterProfile) -> None:
    source = await AssetSnapshotReader(db, profile_assets, ProfileAsset).get(
        command.source.identity, command.source.version
    )
    if source is None or reference(source) != command.source:
        raise ValueError("Original Profile source unavailable or changed")
    original = json.loads(source.definition_json)
    target = json.loads(profile_from(command).definition_json)
    if "scene_contract_version" in original or "scene_contract_version" in target:
        if any(
            original.get(k) != target.get(k)
            for k in ("schema_version", "scene_contract_version", "selector")
        ):
            raise ValueError("Derived Profile cannot change source scene")


async def manifest_for(
    db: AsyncSession, command: RegisterProfile, profile: ProfileAsset
) -> GenerationManifest:
    manifest = await build_generation_manifest(
        AssetSnapshotReader(db, profile_assets, ProfileAsset),
        AssetSnapshotReader(db, prompt_assets, PromptAsset),
        AssetSnapshotReader(db, route_assets, RouteAsset),
        AssetSnapshotReader(db, schema_assets, SchemaAsset),
        profile_id=profile.profile_id,
        profile_version=profile.version,
        route_revision=command.generation_route.version,
    )
    if manifest.prompt != command.prompt or manifest.generation_route != command.generation_route:
        raise ValueError("Profile references differ from confirmed assets")
    prompt = await AssetSnapshotReader(db, prompt_assets, PromptAsset).get(
        command.prompt.identity, command.prompt.version
    )
    route = await AssetSnapshotReader(db, route_assets, RouteAsset).get(
        command.generation_route.identity, command.generation_route.version
    )
    if prompt is None or route is None:
        raise ValueError("Executable assets unavailable")
    executable_route(route)
    policy = decode_published_profile(
        {
            "definition": json.loads(profile.definition_json),
            "fingerprint": profile.fingerprint,
            "status": "published",
        }
    )
    # Validate render compatibility only; this is not a report, model call, or quality verdict.
    render_prompt(
        executable_prompt(prompt),
        policy.render_policy,
        '{"context":{"locale":"zh-CN","focus_areas":[]},"facts":{}}',
    )
    return manifest


async def load_receipt(
    db: AsyncSession, scope: DraftScope, command_id: UUID
) -> ProfileRegistrationReceipt | None:
    row = (
        (
            await db.execute(
                select(registrations).where(registrations.c.command_id == str(command_id))
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        return None
    if (row["organization_id"], row["operator_user_id"]) != (
        scope.organization_id,
        scope.operator_user_id,
    ):
        raise NotFound("Profile command unavailable")
    raw = row["receipt_json"]
    if len(raw.encode()) > 524288 or checksum(raw) != row["receipt_sha256"]:
        raise ValueError("Profile receipt checksum mismatch")
    receipt = RECEIPT.validate_json(raw, strict=True)
    if (
        RECEIPT.dump_json(receipt).decode() != raw
        or receipt.scope != scope
        or receipt.command.command_id != command_id
        or (receipt.manifest.profile.identity, receipt.manifest.profile.version)
        != (row["profile_id"], row["profile_version"])
    ):
        raise ValueError("Profile receipt differs from index")
    await validate_source(db, receipt.command)
    profile = profile_from(receipt.command)
    actual = await AssetSnapshotReader(db, profile_assets, ProfileAsset).get(
        profile.profile_id, profile.version
    )
    audit = (
        await db.execute(
            select(profile_assets.c.source_ref, profile_assets.c.imported_by).where(
                profile_assets.c.profile_id == profile.profile_id,
                profile_assets.c.version == profile.version,
            )
        )
    ).one_or_none()
    if actual != profile or audit is None or tuple(audit) != provenance(receipt):
        raise ValueError("Registered Profile or provenance changed")
    if receipt.manifest != await manifest_for(db, receipt.command, profile):
        raise ValueError("Registered executable assets changed")
    return receipt


async def apply_registration(
    db: AsyncSession, scope: DraftScope, command: RegisterProfile, at: datetime
) -> ProfileRegistrationReceipt:
    prior = await load_receipt(db, scope, command.command_id)
    if prior is not None:
        if prior.command != command:
            raise AssetConflict("Profile command already used")
        return prior
    if at.tzinfo is None or at.utcoffset() is None:
        raise ValueError("Registration time requires a timezone")
    await validate_source(db, command)
    profile = profile_from(command)
    if (
        await AssetSnapshotReader(db, profile_assets, ProfileAsset).get(
            profile.profile_id, profile.version
        )
        is not None
    ):
        raise AssetConflict("Target Profile version is already immutable")
    source_ref = f"qs-ai:profile-registration:{command.command_id}"
    imported_by = f"qs:org/{scope.organization_id}/user/{scope.operator_user_id}"
    await db.execute(
        insert(profile_assets).values(
            **asdict(profile), source_ref=source_ref, imported_by=imported_by
        )
    )
    manifest = await manifest_for(db, command, profile)
    receipt = ProfileRegistrationReceipt(scope, command, manifest, at)
    raw = RECEIPT.dump_json(receipt).decode()
    if len(raw.encode()) > 524288:
        raise ValueError("Profile receipt exceeds limit")
    await db.execute(
        insert(registrations).values(
            command_id=str(command.command_id),
            organization_id=scope.organization_id,
            operator_user_id=scope.operator_user_id,
            profile_id=profile.profile_id,
            profile_version=profile.version,
            receipt_json=raw,
            receipt_sha256=checksum(raw),
        )
    )
    return receipt


class MySQLProfileRegistrar:
    def __init__(self, transactions: Transactions) -> None:
        self.transactions = transactions

    async def register(
        self, scope: DraftScope, command: RegisterProfile, at: datetime
    ) -> ProfileRegistrationReceipt:
        try:
            async with self.transactions.open() as db:
                await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
                receipt = await apply_registration(db, scope, command, at)
                await db.commit()
                return receipt
        except IntegrityError as error:
            if error.orig is None or not error.orig.args or error.orig.args[0] != 1062:
                raise
        # A competing transaction can win either the target version or command key.
        async with self.transactions.open() as db:
            prior = await load_receipt(db, scope, command.command_id)
            if prior is not None and prior.command == command:
                return prior
        raise AssetConflict("Profile version or command already registered")

    async def get_receipt(self, scope: DraftScope, command_id: UUID) -> ProfileRegistrationReceipt:
        if not nonzero_uuid(command_id):
            raise ValueError("Canonical nonzero command UUID required")
        async with self.transactions.open() as db:
            await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            receipt = await load_receipt(db, scope, command_id)
            if receipt is None:
                raise NotFound("Profile command unavailable")
            return receipt
