"""Merged storage preserves legacy identity, bytes and typed foreign keys."""

import hashlib
import os
from uuid import uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import DBAPIError, IntegrityError

from qs_ai.infrastructure.persistence.mysql import asset_records as assets
from qs_ai.infrastructure.persistence.mysql import draft_records as drafts
from qs_ai.infrastructure.persistence.mysql import governance_records as typed
from qs_ai.infrastructure.persistence.mysql import schema
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions

pytestmark = pytest.mark.integration


@pytest.fixture
async def storage():
    dsn = os.getenv("QS_AI_TEST_MYSQL_DSN")
    if not dsn:
        pytest.skip("Requires migrated disposable MySQL")
    database = Database(dsn.replace("mysql://", "mysql+asyncmy://", 1))
    transactions = Transactions(database)
    source = "storage-contract:" + str(uuid4())
    organization = uuid4().int % (2**60) + 1
    try:
        yield transactions, source, organization
    finally:
        async with transactions.open() as db:
            await db.execute(
                sa.delete(schema.draft_versions).where(
                    schema.draft_versions.c.organization_id.in_((organization, organization + 1))
                )
            )
            await db.execute(
                sa.delete(schema.draft_heads).where(
                    schema.draft_heads.c.organization_id.in_((organization, organization + 1))
                )
            )
            await db.execute(
                sa.delete(schema.asset_versions).where(schema.asset_versions.c.source_ref == source)
            )
            await db.commit()
        await database.close()


def asset_row(kind, identity, source, *, organization=0, raw=b' { "text": "raw" }\r\n'):
    return {
        "asset_kind": kind,
        "owner_organization_id": organization,
        "asset_id": identity,
        "version": "v1",
        "fingerprint": "sha256:" + hashlib.sha256(raw).hexdigest(),
        "package_sha256": hashlib.sha256(b"package hash differs").hexdigest()
        if kind == "prompt"
        else None,
        "body_format": "prompt_package_json"
        if kind == "prompt"
        else "semantic_markdown"
        if kind == "semantic_prompt"
        else "definition_json",
        "body_bytes": raw,
        "source_ref": source,
        "imported_by": "storage-contract",
    }


async def test_same_asset_identity_is_typed_and_scoped_with_original_bytes(storage):
    tx, source, organization = storage
    identity = str(uuid4())
    raw = ' { "中文": "原始正文", "n": 1.00 }\r\n'.encode()
    kinds = (
        "profile",
        "prompt",
        "route",
        "schema",
        "execution_policy",
        "gate_policy",
        "semantic_prompt",
    )
    async with tx.open() as db:
        for kind in kinds:
            await db.execute(
                sa.insert(schema.asset_versions).values(
                    **asset_row(
                        kind,
                        identity,
                        source,
                        raw=raw,
                        organization=organization if kind == "semantic_prompt" else 0,
                    )
                )
            )
        await db.execute(
            sa.insert(schema.asset_versions).values(
                **asset_row(
                    "semantic_prompt",
                    identity,
                    source,
                    organization=organization + 1,
                    raw=raw,
                )
            )
        )
        await db.commit()
    async with tx.open() as db:
        for projection in (
            assets.profile_assets,
            assets.prompt_assets,
            assets.route_assets,
            assets.schema_assets,
        ):
            spec = assets.ASSET_SPECS[projection]
            row = (
                (
                    await db.execute(
                        sa.select(projection).where(
                            projection.c[spec.identity] == identity,
                        )
                    )
                )
                .mappings()
                .one()
            )
            assert row[spec.body] == raw.decode()
        policies = (
            (
                await db.execute(
                    sa.select(assets.evaluation_policy_assets).where(
                        assets.evaluation_policy_assets.c.asset_id == identity,
                    )
                )
            )
            .mappings()
            .all()
        )
        assert {row["kind"] for row in policies} == {"execution", "gate"}
        assert all(row["definition_json"].encode() == raw for row in policies)
        prompts = (
            (
                await db.execute(
                    sa.select(assets.semantic_prompt_assets).where(
                        assets.semantic_prompt_assets.c.asset_id == identity,
                    )
                )
            )
            .mappings()
            .all()
        )
        assert {row["organization_id"] for row in prompts} == {organization, organization + 1}
        assert all(row["markdown"].encode() == raw for row in prompts)
        prompt = (
            (
                await db.execute(
                    sa.select(assets.prompt_assets).where(
                        assets.prompt_assets.c.template_id == identity,
                    )
                )
            )
            .mappings()
            .one()
        )
        assert prompt["package_sha256"] != prompt["fingerprint"].removeprefix("sha256:")
        await db.execute(
            typed.delete(assets.profile_assets).where(
                assets.profile_assets.c.profile_id == identity,
            )
        )
        await db.commit()
    async with tx.open() as db:
        assert (
            await db.scalar(
                sa.select(sa.func.count())
                .select_from(schema.asset_versions)
                .where(
                    schema.asset_versions.c.source_ref == source,
                )
            )
            == 7
        )
        assert (
            await db.scalars(
                sa.select(schema.asset_versions.c.body_bytes).where(
                    schema.asset_versions.c.source_ref == source,
                )
            )
        ).all() == [raw] * 7


async def test_asset_identity_preserves_no_pad_and_pad_collations(storage):
    tx, source, _ = storage
    identity = str(uuid4())
    async with tx.open() as db:
        for name in (identity, identity + " "):
            await db.execute(
                sa.insert(schema.asset_versions).values(
                    **asset_row(
                        "profile",
                        name,
                        source,
                    )
                )
            )
        await db.execute(
            sa.insert(schema.asset_versions).values(
                **asset_row(
                    "execution_policy",
                    identity,
                    source,
                )
            )
        )
        await db.commit()
    async with tx.open() as db:
        with pytest.raises(IntegrityError):
            await db.execute(
                sa.insert(schema.asset_versions).values(
                    **asset_row(
                        "execution_policy",
                        identity + " ",
                        source,
                    )
                )
            )
        await db.rollback()
    async with tx.open() as db:
        row = asset_row("profile", identity, source)
        row["version"] = "v1 "
        with pytest.raises(IntegrityError):
            await db.execute(sa.insert(schema.asset_versions).values(**row))
        await db.rollback()
    async with tx.open() as db:
        assert (
            await db.scalars(
                sa.select(assets.profile_assets.c.profile_id).where(
                    assets.profile_assets.c.source_ref == source,
                )
            )
        ).all() == [identity, identity + " "]


@pytest.mark.parametrize("damage", ["scope", "identity", "prompt_hash", "unknown_kind"])
async def test_asset_shape_constraints_reject_invalid_rows(storage, damage):
    tx, source, organization = storage
    row = asset_row("execution_policy", str(uuid4()), source)
    if damage == "scope":
        row["owner_organization_id"] = organization
    elif damage == "identity":
        row["asset_id"] = "x" * 129
    elif damage == "prompt_hash":
        row.update(asset_kind="prompt", body_format="prompt_package_json", package_sha256=None)
    else:
        row["asset_kind"] = "unknown"
    async with tx.open() as db:
        with pytest.raises(DBAPIError) as error:
            await db.execute(sa.insert(schema.asset_versions).values(**row))
        assert error.value.orig.args[0] == (1406 if damage == "identity" else 3819)
        await db.rollback()


async def test_draft_type_scope_and_raw_history_remain_independent(storage):
    tx, _, organization = storage
    identity = str(uuid4())
    raw = '{ "revision": 1, "text": "草稿字节" }\r\n'
    digest = hashlib.sha256(raw.encode()).hexdigest()
    async with tx.open() as db:
        for projection, owner in (
            (drafts.prompt_drafts, organization),
            (drafts.semantic_draft_heads, organization),
            (drafts.semantic_draft_heads, organization + 1),
        ):
            await db.execute(
                typed.insert(projection).values(
                    draft_id=identity,
                    organization_id=owner,
                    revision=1,
                )
            )
        await db.execute(
            typed.insert(drafts.prompt_draft_revisions).values(
                draft_id=identity,
                organization_id=organization,
                revision=1,
                command_id=str(uuid4()),
                operator_user_id=42,
                request_json=' { "reason": "审计" }\n',
                snapshot_json=raw,
                snapshot_sha256=digest,
            )
        )
        for owner in (organization, organization + 1):
            await db.execute(
                typed.insert(drafts.semantic_draft_versions).values(
                    draft_id=identity,
                    organization_id=owner,
                    revision=1,
                    snapshot_json=raw,
                    snapshot_sha256=digest,
                )
            )
        await db.commit()
    async with tx.open() as db:
        with pytest.raises(IntegrityError):
            await db.execute(
                typed.insert(drafts.prompt_drafts).values(
                    draft_id=identity.upper(),
                    organization_id=organization + 1,
                    revision=1,
                )
            )
        await db.rollback()
    async with tx.open() as db:
        assert (
            await db.scalar(
                sa.select(drafts.prompt_draft_revisions.c.snapshot_json).where(
                    drafts.prompt_draft_revisions.c.draft_id == identity,
                )
            )
            == raw
        )
        assert (
            await db.scalars(
                sa.select(schema.draft_versions.c.snapshot_bytes).where(
                    schema.draft_versions.c.organization_id.in_((organization, organization + 1)),
                )
            )
        ).all() == [raw.encode()] * 3
        await db.execute(
            typed.delete(drafts.semantic_draft_versions).where(
                drafts.semantic_draft_versions.c.organization_id == organization,
            )
        )
        await db.commit()
    async with tx.open() as db:
        assert (
            await db.scalar(
                sa.select(sa.func.count())
                .select_from(
                    drafts.prompt_draft_revisions,
                )
                .where(drafts.prompt_draft_revisions.c.draft_id == identity)
            )
            == 1
        )
        assert (
            await db.scalar(
                sa.select(sa.func.count())
                .select_from(
                    drafts.semantic_draft_versions,
                )
                .where(drafts.semantic_draft_versions.c.draft_id == identity)
            )
            == 1
        )
        prompt_row_id = await db.scalar(
            sa.select(schema.draft_heads.c.draft_row_id).where(
                schema.draft_heads.c.prompt_draft_id_key == identity,
            )
        )
        with pytest.raises(IntegrityError):
            await db.execute(
                sa.insert(schema.draft_versions).values(
                    draft_row_id=prompt_row_id,
                    draft_kind="semantic",
                    organization_id=organization,
                    revision=2,
                    snapshot_bytes=raw.encode(),
                    snapshot_sha256=digest,
                )
            )
        await db.rollback()


async def test_registration_foreign_key_requires_profile_and_preserves_restrict(storage):
    tx, source, organization = storage
    identity = str(uuid4())
    command_id = str(uuid4())
    receipt = {
        "command_id": command_id,
        "organization_id": organization,
        "operator_user_id": 42,
        "profile_id": identity,
        "profile_version": "v1",
        "receipt_json": "{}",
        "receipt_sha256": "a" * 64,
    }
    async with tx.open() as db:
        await db.execute(
            sa.insert(schema.asset_versions).values(
                **asset_row(
                    "route",
                    identity,
                    source,
                )
            )
        )
        await db.commit()
    async with tx.open() as db:
        with pytest.raises(IntegrityError):
            await db.execute(sa.insert(schema.profile_registrations).values(**receipt))
        await db.rollback()
    try:
        async with tx.open() as db:
            await db.execute(
                sa.insert(schema.asset_versions).values(
                    **asset_row(
                        "profile",
                        identity,
                        source,
                    )
                )
            )
            await db.execute(sa.insert(schema.profile_registrations).values(**receipt))
            await db.commit()
        async with tx.open() as db:
            with pytest.raises(IntegrityError):
                await db.execute(
                    typed.delete(assets.profile_assets).where(
                        assets.profile_assets.c.profile_id == identity,
                    )
                )
            await db.rollback()
    finally:
        async with tx.open() as db:
            await db.execute(
                sa.delete(schema.profile_registrations).where(
                    schema.profile_registrations.c.command_id == command_id,
                )
            )
            await db.commit()


async def test_prompt_freeze_cannot_reference_a_semantic_head(storage):
    tx, _, organization = storage
    identity = str(uuid4())
    command_id = str(uuid4())
    receipt = {
        "command_id": command_id,
        "organization_id": organization,
        "operator_user_id": 42,
        "draft_id": identity,
        "receipt_json": "{}",
        "receipt_sha256": "a" * 64,
    }
    async with tx.open() as db:
        await db.execute(
            typed.insert(drafts.semantic_draft_heads).values(
                draft_id=identity,
                organization_id=organization,
                revision=1,
            )
        )
        await db.commit()
    async with tx.open() as db:
        with pytest.raises(IntegrityError):
            await db.execute(sa.insert(schema.prompt_draft_freezes).values(**receipt))
        await db.rollback()
    try:
        async with tx.open() as db:
            await db.execute(
                typed.insert(drafts.prompt_drafts).values(
                    draft_id=identity,
                    organization_id=organization,
                    revision=1,
                )
            )
            await db.execute(sa.insert(schema.prompt_draft_freezes).values(**receipt))
            await db.commit()
        async with tx.open() as db:
            with pytest.raises(IntegrityError):
                await db.execute(
                    typed.delete(drafts.prompt_drafts).where(
                        drafts.prompt_drafts.c.draft_id == identity,
                    )
                )
            await db.rollback()
    finally:
        async with tx.open() as db:
            await db.execute(
                sa.delete(schema.prompt_draft_freezes).where(
                    schema.prompt_draft_freezes.c.command_id == command_id,
                )
            )
            await db.commit()
