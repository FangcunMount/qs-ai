"""Native assets need their own complete Run/reviews before publication and compilation."""

from dataclasses import replace

import pytest

from qs_ai.infrastructure.persistence.mysql.evaluation_runs import create_run
from qs_ai.infrastructure.persistence.mysql.execution_configurations import compile_configuration
from qs_ai.infrastructure.persistence.mysql.publications import MySQLPublications
from tests.integration.test_evaluation_step import AT
from tests.integration.test_evaluation_suites import assets as assets
from tests.integration.test_evaluation_suites import complete_release as complete_release
from tests.integration.test_evaluation_suites import evaluation_release as evaluation_release
from tests.integration.test_evaluation_suites import registration as registration
from tests.integration.test_evaluation_suites import suite_registration as suite_registration
from tests.integration.test_execution_configurations import admitted as admitted
from tests.integration.test_execution_configurations import kit as kit
from tests.integration.test_publications import dispatched as dispatched
from tests.integration.test_publications import freeze_creation as freeze_creation
from tests.integration.test_publications import judge as judge
from tests.integration.test_publications import passing_reviewable as passing_reviewable
from tests.integration.test_publications import passing_semantics as passing_semantics
from tests.integration.test_publications import persisted_assets as persisted_assets
from tests.integration.test_publications import ready as ready
from tests.integration.test_publications import reviewable as reviewable
from tests.integration.test_publications import setup_run as setup_run

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
async def native_creation(suite_registration, freeze_creation, monkeypatch):
    from tests.integration import test_evaluation_completions as fixtures

    _, store, scope, command, at, manifest, release = suite_registration
    registered = await store.register(scope, command, at)
    release = replace(release, suite=registered.suite)

    async def create(db, run_id, ignored):
        return await create_run(
            db, run_id, release, 1, "actor:1", "完整评测原生资产", AT, generation_manifest=manifest
        )

    monkeypatch.setattr(fixtures, "create", create)
    return registered


async def test_new_run_approved_publication_compiles_exact_native_prompt(ready, native_creation):
    tx, scope, command, at = ready
    receipt = await MySQLPublications(tx).apply(scope, command, at)
    publication = receipt.change.current.active
    assert publication.evidence.release.suite == native_creation.suite
    assert publication.evidence.manifest == native_creation.manifest
    async with tx.open() as db:
        configuration = await compile_configuration(db, publication)
    assert "QS_NATIVE_SUITE_TEST" in configuration.package.system_message
    assert configuration.package.fingerprint == native_creation.manifest.prompt.fingerprint
    assert await MySQLPublications(tx).get_receipt(scope, command.command_id) == receipt


async def test_native_publication_acceptance_execution_and_pointer_replacement(
    admitted, native_creation
):
    from tests.integration import test_execution_configurations as runtime

    assert admitted[5].change.current.active.evidence.release.suite == native_creation.suite
    await runtime.test_acceptance_replay_and_execution_keep_original_publication_after_replace(
        admitted
    )
