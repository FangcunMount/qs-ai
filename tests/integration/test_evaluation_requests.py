import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.infrastructure.persistence.mysql.evaluation_management import MySQLEvaluationManagement
from qs_ai.infrastructure.persistence.mysql.evaluation_requests import MySQLEvaluationRequests
from qs_ai.infrastructure.persistence.mysql.evaluation_runs import MySQLRunCreator
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.test_generation_manifest import assets as assets
from tests.test_generation_manifest import complete_release as complete_release
from tests.test_generation_manifest import evaluation_release as evaluation_release

pytestmark = pytest.mark.integration
AT = datetime(2026, 9, 12, tzinfo=UTC)


@pytest.fixture
async def requests(setup_run, assets, complete_release):
    tx, run_id, _ = setup_run
    return (
        MySQLEvaluationRequests(tx, MySQLRunCreator(tx, *assets[0])),
        ManagementScope(run_id, 1, 42),
        complete_release,
        tx,
    )


async def test_concurrent_creation_and_replay_after_start_never_reset_run(requests):
    store, scope, release, tx = requests
    first, second = await asyncio.gather(
        *(store.create(scope, release, " 创建评测 ", AT, confirm=True) for _ in range(2))
    )
    assert first == second
    assert (first.status, first.version) == ("requested", 1)
    management = MySQLEvaluationManagement(tx)
    started = await management.start(scope, 1, "启动", AT, confirm=True)
    before = await rows(tx, scope.run_id)
    replay = await store.create(scope, release, "创建评测", AT + timedelta(hours=1), confirm=True)
    assert replay == started
    assert await rows(tx, scope.run_id) == before
    assert replay.version == 2


@pytest.mark.parametrize("conflict", ["org", "actor", "reason", "release"])
async def test_different_request_never_reuses_existing_run(requests, conflict):
    store, scope, release, tx = requests
    await store.create(scope, release, "创建评测", AT, confirm=True)
    before = await rows(tx, scope.run_id)
    reason = "创建评测"
    if conflict == "org":
        scope = replace(scope, organization_id=2)
    elif conflict == "actor":
        scope = replace(scope, operator_user_id=43)
    elif conflict == "reason":
        reason = "另外一个目的"
    else:
        release = replace(release, prompt=replace(release.prompt, fingerprint="sha256:" + "0" * 64))
    with pytest.raises((CheckpointConflict, ValueError)):
        await store.create(scope, release, reason, AT, confirm=True)
    assert await rows(tx, scope.run_id) == before


@pytest.mark.parametrize("reason,confirm", [("", True), ("有效原因", False), ("中" * 334, True)])
async def test_invalid_creation_leaves_no_partial_records(requests, reason, confirm):
    store, scope, release, tx = requests
    with pytest.raises(ValueError):
        await store.create(scope, release, reason, AT, confirm=confirm)
    assert await rows(tx, scope.run_id) == [None, None, None]
