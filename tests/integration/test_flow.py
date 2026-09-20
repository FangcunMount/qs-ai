"""Flows do not mutate snapshots and never substitute current publication pointers."""

from uuid import uuid4

import pytest
from sqlalchemy import func, select, update

from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.governance.solutions import PrepareSolution
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.infrastructure.persistence.mysql import schema as tables
from qs_ai.infrastructure.persistence.mysql.flow import MySQLFlowReader
from tests.integration.test_solutions import assets as assets
from tests.integration.test_solutions import complete_release as complete_release
from tests.integration.test_solutions import edit
from tests.integration.test_solutions import evaluation_release as evaluation_release
from tests.integration.test_solutions import persisted_assets as persisted_assets
from tests.integration.test_solutions import setup_run as setup_run
from tests.integration.test_solutions import workspace as workspace

pytestmark = pytest.mark.integration


async def test_solution_flow_has_one_business_call_and_separate_evaluation(workspace):
    tx, store, scope, sid, command, at = workspace
    initial = await store.apply(scope, sid, command, at)
    reader = MySQLFlowReader(tx)
    async with tx.open() as db:
        before = await db.scalar(select(func.count()).select_from(tables.model_calls))
    flow = await reader.solution(scope, sid)
    business_calls = [
        n for n in flow["nodes"] if n["lane"] == "business" and n["kind"] == "model_call"
    ]
    assert len(business_calls) == 1
    assert flow["asset_reference_mode"] == "source_with_draft_overrides"
    assert flow["version"] == 1 and flow["draft"]
    assert next(n for n in flow["nodes"] if n["id"] == "prompt")["details"] == initial["content"]
    assert (
        next(n for n in flow["nodes"] if n["id"] == "semantic")["details"]["prompt_editable"]
        is False
    )
    await store.apply(scope, sid, edit(initial), at)
    edited = await reader.solution(scope, sid)
    assert (
        next(n for n in edited["nodes"] if n["id"] == "generation")["details"]["max_output_tokens"]
        == 4000
    )
    async with tx.open() as db:
        assert await db.scalar(select(func.count()).select_from(tables.model_calls)) == before
    with pytest.raises(NotFound):
        await reader.solution(DraftScope(2, 42), sid)
    prepared = await store.apply(
        scope, sid, PrepareSolution(command_id=uuid4(), expected_revision=2, reason="固定图"), at
    )
    frozen = await reader.solution(scope, sid)
    assert frozen["immutable"] and not any(n["editable"] for n in frozen["nodes"])
    assert frozen["release_fingerprint"] != flow["release_fingerprint"]
    cases = next(n for n in frozen["nodes"] if n["id"] == "cases")
    assert cases["details"] == prepared["prepared"]["plan"]
    assert cases["details"]["candidate_count"] == 35


async def test_corrupt_source_does_not_produce_plausible_flow(workspace):
    tx, store, scope, sid, command, at = workspace
    await store.apply(scope, sid, command, at)
    async with tx.open() as db:
        await db.execute(
            update(tables.solution_revisions)
            .where(tables.solution_revisions.c.solution_id == str(sid))
            .values(state_sha256="0" * 64)
        )
        await db.commit()
    with pytest.raises(ValueError, match="checksum"):
        await MySQLFlowReader(tx).solution(scope, sid)


@pytest.mark.usefixtures("freeze_creation")
async def test_publication_flow_remains_fixed_after_pointer_pause(ready, published_configuration):
    from qs_ai.application.governance.publication import MovePublication
    from qs_ai.infrastructure.persistence.mysql.publications import MySQLPublications

    tx, scope, _, at = ready
    active = published_configuration.change.current.active
    reader = MySQLFlowReader(tx)
    view_scope = DraftScope(scope.organization_id, scope.operator_user_id)
    before = await reader.publication(view_scope, active.publication_id)
    assert before["immutable"] and not any(n["editable"] for n in before["nodes"])
    with pytest.raises(NotFound):
        await reader.publication(DraftScope(2, 42), active.publication_id)
    await MySQLPublications(tx).apply(
        scope,
        MovePublication(
            uuid4(), active.evidence.selector, 1, active.publication_id, "暂停验证", True, None
        ),
        at,
    )
    after = await reader.publication(view_scope, active.publication_id)
    assert after["version"] == before["version"]
    assert after["nodes"] == before["nodes"]
    assert after["release_fingerprint"] == active.evidence.release.fingerprint()
