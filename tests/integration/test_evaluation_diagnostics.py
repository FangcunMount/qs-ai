"""Disposable MySQL and synthetic outputs: diagnosis does not perform recovery or approval."""

import hashlib
import json
from dataclasses import replace
from datetime import timedelta
from uuid import uuid4

import pytest
from sqlalchemy import select, update

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.evaluation.diagnostics import EvaluationDiagnostics, ExecutionQuery
from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.application.interpretation.provider import ModelResponse
from qs_ai.bootstrap.container import create_container
from qs_ai.config import Settings
from qs_ai.infrastructure.persistence.mysql.evaluation_diagnostics import MySQLEvaluationDiagnostics
from qs_ai.infrastructure.persistence.mysql.evaluation_management import MySQLEvaluationManagement
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_generation_completions as gen
from tests.integration.test_evaluation_recovery import pending
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_evaluation_step import AT, Gateway, step
from tests.integration.test_evaluation_step import ready as ready
from tests.integration.test_evaluation_unknowns import snapshot

pytestmark = pytest.mark.integration


async def test_generation_and_semantic_diagnostics_keep_exact_bytes_and_do_not_write(ready):
    tx, run_id, *_ = ready
    gateway = Gateway(ready)
    first = await step(ready, gateway)
    second = await step(ready, gateway, version=first.version)
    store = MySQLEvaluationDiagnostics(tx)
    query = ExecutionQuery(ManagementScope(run_id, 1, 42), second.version, limit=1)
    before = await snapshot(tx, run_id)
    page = await store.list(query)
    assert len(page.executions) == 1 and page.next_cursor
    next_page = await store.list(replace(query, cursor=page.next_cursor))
    assert len(next_page.executions) == 1 and not next_page.next_cursor
    assert {x.kind for x in page.executions + next_page.executions} == {"generation", "semantic"}
    for item in page.executions + next_page.executions:
        assert item.status == "succeeded"
        assert item.raw_output_bytes > 0 and item.normalized_output_bytes > 0
        assert "raw_output" not in json.loads(item.evidence_json)
        result = await store.get(query, item.execution_id)
        assert result.execution == item
        assert len(result.raw_output) == item.raw_output_bytes
        assert hashlib.sha256(result.raw_output).hexdigest() == result.raw_sha256
        assert hashlib.sha256(result.normalized_output).hexdigest() == result.normalized_sha256
        assert (
            json.loads(result.execution.evidence_json)["receipt"]["invocation_id"]
            == item.invocation_id
        )
    assert gateway.calls == 2 and await snapshot(tx, run_id) == before


class InvalidOutput(Gateway):
    async def generate_messages(self, messages, route, schema, invocation_id):
        self.calls += 1
        return ModelResponse(
            invocation_id, "request:bad", route.model, "not-json", "not-json", "none", 1, 2, 3
        )


@pytest.mark.parametrize("unknown", [False, True])
async def test_non_candidate_failure_exposes_diagnosis_and_original_output_without_retry(
    ready, unknown
):
    tx, run_id, *_ = ready
    gateway = Gateway(ready, fail=True) if unknown else InvalidOutput(ready)
    state = await step(ready, gateway)
    store = MySQLEvaluationDiagnostics(tx)
    query = ExecutionQuery(ManagementScope(run_id, 1, 42), state.version)
    before = await snapshot(tx, run_id)
    page = await store.list(query)
    assert len(page.executions) == 1
    item = page.executions[0]
    assert item.status == ("result_unknown" if unknown else "failed")
    result = await store.get(query, item.execution_id)
    evidence = json.loads(result.execution.evidence_json)
    assert evidence["failure"]["code"] and evidence["failure"]["stage"]
    if not unknown:
        assert b"not-json" in result.raw_output
    async with tx.open() as db:
        assert (
            await db.execute(select(gen.c.candidate_id).where(gen.c.run_id == str(run_id)))
        ).scalar_one() is None
    assert gateway.calls == 1 and await snapshot(tx, run_id) == before


async def test_active_dispatch_is_not_labeled_failed_or_retried(ready):
    tx, run_id, *_ = ready
    state = await pending(ready, dispatched=True)
    store = MySQLEvaluationDiagnostics(tx)
    query = ExecutionQuery(ManagementScope(run_id, 1, 42), state.version)
    before = await snapshot(tx, run_id)
    page = await store.list(query)
    assert len(page.executions) == 1 and page.executions[0].status == "dispatching"
    result = await store.get(query, page.executions[0].execution_id)
    assert result.raw_output == result.normalized_output == b""
    assert await snapshot(tx, run_id) == before


async def test_scope_version_and_wrong_execution_refuse_output(ready):
    tx, run_id, *_ = ready
    state = await step(ready, Gateway(ready, fail=True))
    store = MySQLEvaluationDiagnostics(tx)
    query = ExecutionQuery(ManagementScope(run_id, 1, 42), state.version)
    item = (await store.list(query)).executions[0]
    for scope in (replace(query.scope, organization_id=2), replace(query.scope, run_id=uuid4())):
        with pytest.raises(NotFound):
            await store.list(replace(query, scope=scope))
        with pytest.raises(NotFound):
            await store.get(replace(query, scope=scope), item.execution_id)
    with pytest.raises(CheckpointConflict):
        await store.list(replace(query, expected_version=state.version - 1))
    with pytest.raises(NotFound):
        await store.get(query, "execution:missing")
    for invalid in ("", "../bad execution", "x" * 129):
        with pytest.raises(ValueError):
            await store.get(query, invalid)


async def test_corrupt_normalized_output_is_not_returned_as_valid_evidence(ready):
    tx, run_id, *_ = ready
    state = await step(ready, Gateway(ready))
    store = MySQLEvaluationDiagnostics(tx)
    query = ExecutionQuery(ManagementScope(run_id, 1, 42), state.version)
    item = (await store.list(query)).executions[0]
    async with tx.open() as db:
        await db.execute(
            update(gen).where(gen.c.run_id == str(run_id)).values(normalized_output=b"{}")
        )
        await db.commit()
    with pytest.raises(ValueError, match="fingerprint"):
        await store.get(query, item.execution_id)


async def test_diagnostics_resolve_from_application_container():
    container = create_container(Settings())
    try:
        async with container() as operation:
            assert isinstance(
                await operation.get(EvaluationDiagnostics), MySQLEvaluationDiagnostics
            )
    finally:
        await container.close()


async def test_cancellation_keeps_previous_output_available_at_new_version(ready):
    tx, run_id, *_ = ready
    state = await step(ready, Gateway(ready))
    store = MySQLEvaluationDiagnostics(tx)
    query = ExecutionQuery(ManagementScope(run_id, 1, 42), state.version)
    item = (await store.list(query)).executions[0]
    original = await store.get(query, item.execution_id)
    canceled = await MySQLEvaluationManagement(tx).cancel(
        query.scope,
        state.version,
        "停止本次评测",
        AT + timedelta(seconds=1),
        discard=False,
        confirm=True,
    )
    current = await store.get(replace(query, expected_version=canceled.version), item.execution_id)
    assert current.raw_output == original.raw_output
    assert current.normalized_output == original.normalized_output
    assert current.execution.evidence_json == original.execution.evidence_json
    with pytest.raises(CheckpointConflict):
        await store.get(query, item.execution_id)
