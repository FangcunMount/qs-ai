"""Initializer preserves registered bytes and refuses unprovable historical bindings."""

import os

import pytest
from sqlalchemy import select, update

from qs_ai.bootstrap.import_evaluation_suites import run
from qs_ai.infrastructure.persistence.mysql.asset_catalog import MySQLAssetCatalog
from qs_ai.infrastructure.persistence.mysql.evaluation_suites import MySQLSuiteRegistrar
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_suites
from qs_ai.infrastructure.persistence.mysql.suite_contracts import read
from qs_ai.infrastructure.qs_server.evaluation_suite import V6_PUBLISHED, load_suite
from tests.integration.test_evaluation_suites import suite_registration as suite_registration
from tests.integration.test_profile_registrations import assets as assets
from tests.integration.test_profile_registrations import complete_release as complete_release
from tests.integration.test_profile_registrations import evaluation_release as evaluation_release
from tests.integration.test_profile_registrations import persisted_assets as persisted_assets
from tests.integration.test_profile_registrations import registration as registration
from tests.integration.test_profile_registrations import setup_run as setup_run

pytestmark = pytest.mark.integration


async def test_initializer_preserves_bytes_and_original_receipt(suite_registration, monkeypatch):
    tx, _, scope, command, at, _, _ = suite_registration
    monkeypatch.setenv(
        "QS_AI_DATABASE_URL",
        os.environ["QS_AI_TEST_MYSQL_DSN"].replace("mysql://", "mysql+asyncmy://", 1),
    )
    registered = await MySQLSuiteRegistrar(tx).register(scope, command, at)
    async with tx.open() as db:
        before = (
            (
                await db.execute(
                    select(evaluation_suites).where(
                        evaluation_suites.c.suite_id == command.suite_id,
                    )
                )
            )
            .mappings()
            .one()
        )
    first = await run("integration-initializer")
    assert first["bindings_added"] == 1
    assert (await run("integration-initializer"))["bindings_added"] == 0
    async with tx.open() as db:
        after = (
            (
                await db.execute(
                    select(evaluation_suites).where(
                        evaluation_suites.c.suite_id == command.suite_id,
                    )
                )
            )
            .mappings()
            .one()
        )
        for key in ("definition_json", "fingerprint", "receipt_json", "receipt_sha256"):
            assert after[key] == before[key]
        binding = await read(db, registered.suite, scope.organization_id)
        assert binding.semantic_owner_organization_id == 0
        with pytest.raises(ValueError):
            await read(db, registered.suite, scope.organization_id + 1)
    catalog = await MySQLAssetCatalog(tx).get(scope, "suite", V6_PUBLISHED.id, V6_PUBLISHED.version)
    assert catalog.definition_json == load_suite(V6_PUBLISHED).definition_json
    assert (await MySQLSuiteRegistrar(tx).get_receipt(scope, command.command_id)) == registered


async def test_initializer_rejects_changed_existing_binding(suite_registration, monkeypatch):
    tx, _, scope, command, at, _, _ = suite_registration
    monkeypatch.setenv(
        "QS_AI_DATABASE_URL",
        os.environ["QS_AI_TEST_MYSQL_DSN"].replace("mysql://", "mysql+asyncmy://", 1),
    )
    await MySQLSuiteRegistrar(tx).register(scope, command, at)
    await run("integration-initializer")
    async with tx.open() as db:
        await db.execute(
            update(evaluation_suites)
            .where(
                evaluation_suites.c.suite_id == command.suite_id,
            )
            .values(contracts_sha256="0" * 64)
        )
        await db.commit()
    with pytest.raises(ValueError):
        await run("integration-initializer")
