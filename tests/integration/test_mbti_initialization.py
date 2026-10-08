"""Atomic MBTI root import against migrated disposable MySQL; no publication writes."""

import os
from dataclasses import asdict
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from qs_ai.bootstrap.import_evaluation_assets import baseline_assets as evaluation_baseline
from qs_ai.bootstrap.import_mbti_assets import insert_exact, install
from qs_ai.bootstrap.import_routes import baseline_assets as route_baseline
from qs_ai.bootstrap.import_schemas import baseline_assets as schema_baseline
from qs_ai.infrastructure.persistence.mysql.asset_records import (
    evaluation_policy_assets,
    profile_assets,
    prompt_assets,
    route_assets,
    schema_assets,
    semantic_prompt_assets,
)
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_suites import load_registered_suite
from qs_ai.infrastructure.persistence.mysql.governance_records import delete, update
from qs_ai.infrastructure.persistence.mysql.schema import (
    configuration_publication_pointers,
    evaluation_runs,
    evaluation_suites,
    sessions,
)
from qs_ai.infrastructure.qs_server.mbti_assets import load_mbti_root, load_mbti_themes_root

pytestmark = pytest.mark.integration


@pytest.fixture
async def initialized_dependencies():
    dsn = os.getenv("QS_AI_TEST_MYSQL_DSN")
    if not dsn:
        pytest.skip("Requires migrated disposable MySQL")
    database = Database(dsn.replace("mysql://", "mysql+asyncmy://", 1))
    tx = Transactions(database)
    actor = "mbti-integration-" + str(uuid4())
    commit = uuid4().hex + "00000000"
    source = "test:" + actor
    _, policies, _, semantic_schema = evaluation_baseline()
    entries = [(route_assets, asdict(a)) for a in route_baseline(include_evaluation=True)[1]]
    entries += [(schema_assets, asdict(a)) for a in (*schema_baseline()[1], semantic_schema)]
    entries += [
        (
            evaluation_policy_assets,
            {
                "kind": a.kind.value,
                "asset_id": a.reference.id,
                "version": a.reference.version,
                "fingerprint": a.reference.fingerprint,
                "definition_json": a.definition_json,
            },
        )
        for a in policies
    ]
    try:
        async with tx.open() as db:
            for table, values in entries:
                await insert_exact(db, table, values, source, actor)
            await db.commit()
        yield tx, actor, commit
    finally:
        async with tx.open() as db:
            for table in (
                evaluation_suites,
                semantic_prompt_assets,
                prompt_assets,
                profile_assets,
                schema_assets,
                route_assets,
                evaluation_policy_assets,
            ):
                await db.execute(delete(table).where(table.c.imported_by == actor))
            await db.commit()
        await database.close()


async def apply(tx, actor, commit):
    async with tx.open() as db:
        await db.connection(execution_options={"isolation_level": "SERIALIZABLE"})
        inserted = await install(db, load_mbti_root(), commit, actor)
        await db.commit()
        return inserted


async def counts(tx):
    async with tx.open() as db:
        return [
            await db.scalar(select(func.count()).select_from(table))
            for table in (sessions, evaluation_runs, configuration_publication_pointers)
        ]


async def test_atomic_import_repeat_keeps_original_provenance_and_no_approval(
    initialized_dependencies,
):
    tx, actor, commit = initialized_dependencies
    before = await counts(tx)
    assert await apply(tx, actor, commit) == 5
    assert await apply(tx, "replay-operator", "b" * 40) == 0
    assert await counts(tx) == before
    root = load_mbti_root()
    async with tx.open() as db:
        assert (
            await load_registered_suite(db, root.suite.reference, organization_id=1) == root.suite
        )
        assert (
            await db.scalar(
                select(profile_assets.c.imported_by).where(
                    profile_assets.c.profile_id == root.profile.profile_id
                )
            )
            == actor
        )
        source = await db.scalar(
            select(profile_assets.c.source_ref).where(
                profile_assets.c.profile_id == root.profile.profile_id
            )
        )
        assert commit in source and root.manifest_sha256 in source


async def test_conflicting_existing_asset_fails_without_overwrite(initialized_dependencies):
    tx, actor, commit = initialized_dependencies
    await apply(tx, actor, commit)
    async with tx.open() as db:
        await db.execute(
            update(profile_assets)
            .where(profile_assets.c.profile_id == "participant-mbti-single")
            .values(fingerprint="sha256:" + "0" * 64)
        )
        await db.commit()
    with pytest.raises(ValueError, match="different content"):
        await apply(tx, actor, commit)
    async with tx.open() as db:
        assert (
            await db.scalar(
                select(profile_assets.c.fingerprint).where(
                    profile_assets.c.profile_id == "participant-mbti-single"
                )
            )
            == "sha256:" + "0" * 64
        )


async def test_late_dependency_failure_rolls_back_all_new_assets(initialized_dependencies):
    tx, actor, commit = initialized_dependencies
    async with tx.open() as db:
        await db.execute(
            delete(schema_assets).where(schema_assets.c.schema_id == "ai-explanation-output")
        )
        await db.commit()
    with pytest.raises(ValueError):
        await apply(tx, actor, commit)
    async with tx.open() as db:
        for table in (evaluation_suites, semantic_prompt_assets, profile_assets, prompt_assets):
            assert (
                await db.scalar(
                    select(func.count()).select_from(table).where(table.c.imported_by == actor)
                )
                == 0
            )
        assert (
            await db.scalar(
                select(func.count())
                .select_from(schema_assets)
                .where(
                    schema_assets.c.schema_id == "ai-explanation-input",
                    schema_assets.c.version == "v2",
                )
            )
            == 0
        )


async def test_thematic_root_import_and_template_keep_original_versions(initialized_dependencies):
    from qs_ai.application.governance.prompt_drafts import DraftScope
    from qs_ai.infrastructure.persistence.mysql.solution_templates import (
        template_catalog,
        template_release,
    )

    tx, actor, commit = initialized_dependencies
    before = await counts(tx)
    assert await apply(tx, actor, commit) == 5
    root = load_mbti_themes_root()
    async with tx.open() as db:
        await db.connection(execution_options={"isolation_level": "SERIALIZABLE"})
        assert await install(db, root, commit, actor) == 6
        await db.commit()
    async with tx.open() as db:
        assert await install(db, root, "b" * 40, "replay-operator") == 0
        assert (
            await load_registered_suite(db, root.suite.reference, organization_id=1) == root.suite
        )
        catalog = await template_catalog(db, DraftScope(1, 42))
        assert [entry["template_ref"]["version"] for entry in catalog] == ["v1", "three-topic-v1"]
        assert all(entry["published"] is False for entry in catalog)
        assert await template_release(db, DraftScope(1, 42), root.suite.reference) == root.release
        await db.commit()
    assert await counts(tx) == before


async def test_thematic_root_late_validation_failure_is_atomic(
    initialized_dependencies, monkeypatch
):
    from qs_ai.bootstrap import import_mbti_assets

    tx, actor, commit = initialized_dependencies
    root = load_mbti_themes_root()

    async def unavailable_after_insert(*args, **kwargs):
        raise ValueError("isolated late validation failure")

    monkeypatch.setattr(import_mbti_assets, "validate_release_assets", unavailable_after_insert)
    with pytest.raises(ValueError, match="isolated late validation failure"):
        async with tx.open() as db:
            await install(db, root, commit, actor)
    async with tx.open() as db:
        for table in (evaluation_suites, semantic_prompt_assets, profile_assets, prompt_assets):
            assert (
                await db.scalar(
                    select(func.count()).select_from(table).where(table.c.imported_by == actor)
                )
                == 0
            )
        assert (
            await db.scalar(
                select(func.count())
                .select_from(schema_assets)
                .where(
                    schema_assets.c.schema_id == "ai-explanation-output",
                    schema_assets.c.version == "v2",
                )
            )
            == 0
        )


async def test_exploration_import_is_independent_repeatable_and_never_publishes(
    initialized_dependencies,
):
    import json

    from qs_ai.application.governance.prompt_drafts import DraftScope
    from qs_ai.infrastructure.persistence.mysql.solution_templates import (
        template_catalog,
        template_release,
    )
    from qs_ai.infrastructure.qs_server.evaluation_policies import evaluation_directory
    from qs_ai.infrastructure.qs_server.mbti_assets import load_mbti_exploration_root

    tx, actor, commit = initialized_dependencies
    before = await counts(tx)
    root = load_mbti_exploration_root()
    # Initializer requires existing exact routes; it cannot silently create or replace them.
    with pytest.raises(ValueError, match="model route unavailable"):
        async with tx.open() as db:
            await install(db, root, commit, actor)
    proof = json.loads(
        (evaluation_directory() / "mbti-exploration/source-proof-v1.json").read_text()
    )
    async with tx.open() as db:
        for source in proof["routes"].values():
            ref = source["reference"]
            await insert_exact(
                db,
                route_assets,
                {
                    "route": ref["id"],
                    "revision": ref["version"],
                    "fingerprint": ref["fingerprint"],
                    "definition_json": source["body"],
                },
                "test:" + actor,
                actor,
            )
        assert await install(db, root, commit, actor) == 6
        await db.commit()
    async with tx.open() as db:
        assert await install(db, root, "b" * 40, "replay") == 0
        catalog = await template_catalog(db, DraftScope(1, 42))
        entry = next(x for x in catalog if x["template_ref"]["id"] == root.suite.reference.id)
        assert entry["selector"]["model_code"] == "MBTI_FC_93"
        assert entry["published"] is False
        assert await template_release(db, DraftScope(1, 42), root.suite.reference) == root.release
        await db.commit()
    assert await counts(tx) == before
