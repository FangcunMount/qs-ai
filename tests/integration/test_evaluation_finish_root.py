"""Borrowed original completion atomicity; staged views are not SDK/commit evidence."""

from datetime import timedelta

import pytest
from reliable_messaging.transaction import BindingError

from qs_ai.application.evaluation.provider_failure import classify_provider_failure
from qs_ai.application.interpretation.provider import ProviderFailure
from qs_ai.infrastructure.persistence.mysql import evaluation_step as module
from tests.integration.test_evaluation_creation_interop import persisted_assets as persisted_assets
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_evaluation_step import AT, Gateway, step
from tests.integration.test_evaluation_step import ready as ready
from tests.integration.test_evaluation_unknowns import snapshot

pytestmark = pytest.mark.integration


async def prepared_case(ready, semantic, unknown):
    tx, run_id, _, routes, schemas = ready
    version = (await step(ready, Gateway(ready))).version if semantic else 3
    prepared = await module.prepare_step(
        tx, run_id, version, 1, "worker:step", routes, schemas, clock=lambda: AT
    )
    gateway = Gateway(ready, fail=unknown)
    try:
        response = await gateway.generate_messages(
            prepared.messages, prepared.route, prepared.schema, prepared.invocation_id
        )
        failure = None
    except ProviderFailure as error:
        response = None
        failure = classify_provider_failure(prepared.cp.kind, prepared.execution_id, error)
    before = await snapshot(tx, run_id)
    value = await module.prepare_step_completion(
        prepared, response, failure, routes, clock=lambda: AT + timedelta(seconds=1)
    )
    assert await snapshot(tx, run_id) == before and gateway.calls == 1
    return tx, run_id, routes, schemas, prepared, value, before, gateway


@pytest.mark.parametrize("semantic", [False, True])
@pytest.mark.parametrize("unknown", [False, True])
@pytest.mark.parametrize("commit", [False, True])
async def test_original_prepared_completion_evidence_and_checkpoint_share_borrowed_root(
    ready, semantic, unknown, commit, monkeypatch
):
    tx, run_id, routes, schemas, prepared, value, before, gateway = await prepared_case(
        ready, semantic, unknown
    )

    def no_identity():
        raise AssertionError("Candidate identity was frozen before this root")

    with monkeypatch.context() as patch:
        patch.setattr(module, "uuid4", no_identity)
        async with tx.open() as db:
            await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            root = db.get_transaction()
            state, applied = await module.finish_step_in(
                db, run_id, 1, "worker:step", routes, schemas, value
            )
            assert state.version == prepared.state.version + 1 and applied is True
            assert db.get_transaction() is root and root.is_active
            assert await snapshot(tx, run_id) == before
            if commit:
                await db.commit()
    after = await snapshot(tx, run_id)
    assert gateway.calls == 1
    if not commit:
        assert after == before
    else:
        assert after[0][2]["version"] == prepared.state.version + 1
        records = after[1][2 if semantic else 1]
        record = next(row for row in records if row["execution_id"] == prepared.execution_id)
        assert record["invocation_id"] == prepared.invocation_id
        assert record["evidence_json"]["status"] == ("result_unknown" if unknown else "succeeded")
        assert record["evidence_json"]["provider_call_count"] == 1


@pytest.mark.parametrize("nested", [False, True])
async def test_borrowed_completion_refuses_inactive_or_savepoint_without_writes(ready, nested):
    tx, run_id, routes, schemas, prepared, value, before, gateway = await prepared_case(
        ready, False, True
    )
    async with tx.open() as db:
        if nested:
            async with db.begin(), db.begin_nested():
                with pytest.raises(BindingError):
                    await module.finish_step_in(
                        db, run_id, 1, "worker:step", routes, schemas, value
                    )
        else:
            assert db.get_transaction() is None
            with pytest.raises(BindingError):
                await module.finish_step_in(db, run_id, 1, "worker:step", routes, schemas, value)
    assert await snapshot(tx, run_id) == before and gateway.calls == 1
