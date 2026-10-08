"""Consolidation retains independent evidence identities and write-once bindings."""

import os
from uuid import uuid4

import pytest
from sqlalchemy import delete, insert, select
from sqlalchemy.exc import DBAPIError, IntegrityError

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.domain.interpretation.model import RuleViolation
from qs_ai.infrastructure.persistence.mysql.completion_records import (
    generation_completions as gen,
)
from qs_ai.infrastructure.persistence.mysql.completion_records import (
    semantic_completions as sem,
)
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_dispatches import freeze_policy
from qs_ai.infrastructure.persistence.mysql.interpretation import MySQLUnitOfWork
from qs_ai.infrastructure.persistence.mysql.run_policy import frozen_policy_query
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_completions,
    evaluation_runs,
    sessions,
)
from qs_ai.infrastructure.qs_server.evaluation_policies import load_execution_policy

pytestmark = pytest.mark.integration


@pytest.fixture
async def storage():
    dsn = os.getenv("QS_AI_TEST_MYSQL_DSN")
    if not dsn:
        pytest.skip("Requires migrated disposable MySQL")
    database = Database(dsn.replace("mysql://", "mysql+asyncmy://", 1))
    tx, run_id, session_ids = Transactions(database), uuid4(), [str(uuid4()), str(uuid4())]
    try:
        yield tx, run_id, session_ids
    finally:
        async with tx.open() as db:
            await db.execute(
                delete(evaluation_completions).where(evaluation_completions.c.run_id == str(run_id))
            )
            await db.execute(delete(evaluation_runs).where(evaluation_runs.c.run_id == str(run_id)))
            await db.execute(delete(sessions).where(sessions.c.id.in_(session_ids)))
            await db.commit()
        await database.close()


def evidence(run_id, execution="execution:shared", invocation="invocation:shared", **values):
    return dict(
        run_id=str(run_id),
        execution_id=execution,
        invocation_id=invocation,
        execution_ordinal=1,
        evidence_json={"status": "result_unknown"},
        raw_output=b"original-provider-bytes",
        normalized_output=b"original-normalized-bytes",
        **values,
    )


async def test_kind_scopes_allow_original_shared_ids_and_isolate_mutations(storage):
    tx, run_id, _ = storage
    async with tx.open() as db:
        await db.execute(gen.insert().values(**evidence(run_id, case_id="case:1", slot_ordinal=1)))
        await db.execute(sem.insert().values(**evidence(run_id, candidate_id="candidate:1")))
        await db.commit()
    async with tx.open() as db:
        await db.execute(
            gen.update()
            .where(gen.c.run_id == str(run_id), gen.c.execution_id == "execution:shared")
            .values(candidate_json={"review_ready": True})
        )
        (generation,) = (
            await db.execute(gen.select().where(gen.c.run_id == str(run_id)))
        ).mappings()
        (semantic,) = (await db.execute(sem.select().where(sem.c.run_id == str(run_id)))).mappings()
        assert generation["candidate_json"] == {"review_ready": True}
        assert semantic["candidate_json"] is None
        assert generation["raw_output"] == semantic["raw_output"] == b"original-provider-bytes"
        await db.execute(gen.delete().where(gen.c.run_id == str(run_id)))
        assert (await db.execute(gen.select().where(gen.c.run_id == str(run_id)))).first() is None
        assert (await db.execute(sem.select().where(sem.c.run_id == str(run_id)))).one()
        await db.commit()


@pytest.mark.parametrize("constraint", ["invocation", "candidate", "slot"])
async def test_generation_retains_each_original_unique_boundary(storage, constraint):
    tx, run_id, _ = storage
    initial = evidence(run_id, case_id="case:1", slot_ordinal=1, candidate_id="candidate:1")
    duplicate = evidence(
        run_id,
        execution="execution:other",
        invocation="invocation:other",
        case_id="case:2",
        slot_ordinal=2,
        candidate_id="candidate:2",
    )
    for key in {
        "invocation": ("invocation_id",),
        "candidate": ("candidate_id",),
        "slot": ("case_id", "slot_ordinal", "execution_ordinal"),
    }[constraint]:
        duplicate[key] = initial[key]
    async with tx.open() as db:
        await db.execute(gen.insert().values(**initial))
        await db.commit()
    async with tx.open() as db:
        with pytest.raises(IntegrityError):
            await db.execute(gen.insert().values(**duplicate))
        await db.rollback()


async def test_failed_generations_and_semantic_retries_keep_nullable_and_ordinal_rules(storage):
    tx, run_id, _ = storage
    async with tx.open() as db:
        for n in (1, 2):
            await db.execute(
                gen.insert().values(
                    **evidence(
                        run_id, f"gen:{n}", f"gen-invoke:{n}", case_id=f"case:{n}", slot_ordinal=1
                    )
                )
            )
            row = evidence(run_id, f"sem:{n}", f"sem-invoke:{n}", candidate_id="candidate:1")
            row["execution_ordinal"] = n
            await db.execute(sem.insert().values(**row))
        await db.commit()
    async with tx.open() as db:
        with pytest.raises(IntegrityError):
            await db.execute(
                sem.insert().values(
                    **evidence(
                        run_id, "sem:duplicate", "sem-invoke:duplicate", candidate_id="candidate:1"
                    )
                )
            )
        await db.rollback()


async def test_semantic_invocation_remains_unique_within_its_original_kind(storage):
    tx, run_id, _ = storage
    async with tx.open() as db:
        await db.execute(sem.insert().values(**evidence(run_id, candidate_id="candidate:1")))
        await db.commit()
    async with tx.open() as db:
        with pytest.raises(IntegrityError):
            await db.execute(
                sem.insert().values(
                    **evidence(run_id, execution="execution:other", candidate_id="candidate:2")
                )
            )
        await db.rollback()


@pytest.mark.parametrize(
    "kind,shape",
    [
        (None, "generation"),
        ("unsupported", "generation"),
        ("semantic", "generation"),
        ("generation", "semantic"),
    ],
)
async def test_untyped_or_wrong_kind_write_is_rejected_by_database(storage, kind, shape):
    tx, run_id, _ = storage
    row = evidence(run_id)
    if shape == "generation":
        row.update(case_id="case:1", slot_ordinal=1)
    else:
        row.update(candidate_id="candidate:1")
    if kind is not None:
        row["kind"] = kind
    async with tx.open() as db:
        with pytest.raises(DBAPIError) as error:
            await db.execute(insert(evaluation_completions).values(**row))
        assert error.value.orig.args[0] in (1048, 1364, 3819)
        await db.rollback()


async def test_external_request_binding_allows_unbound_sessions_and_cannot_be_replaced(storage):
    tx, _, session_ids = storage
    request_id = str(uuid4())
    async with tx.open() as db:
        for session_id in session_ids:
            await db.execute(
                insert(sessions).values(
                    id=session_id,
                    org_id=1,
                    owner_subject_id="storage-contract",
                    testee_id=1,
                    assessment_ids=[],
                    goal="contract",
                    status="created",
                    version=1,
                    workflow_version="v1",
                )
            )
        await MySQLUnitOfWork(db).bind_request(session_ids[0], request_id)
        await db.commit()
    async with tx.open() as db:
        with pytest.raises(RuleViolation, match="idempotency_conflict"):
            await MySQLUnitOfWork(db).bind_request(session_ids[0], str(uuid4()))
        await db.rollback()
    async with tx.open() as db:
        with pytest.raises(IntegrityError):
            await MySQLUnitOfWork(db).bind_request(session_ids[1], request_id)
        await db.rollback()
    async with tx.open() as db:
        assert (
            await db.scalar(select(sessions.c.request_id).where(sessions.c.id == session_ids[0]))
            == request_id
        )
        assert (
            await db.scalar(select(sessions.c.request_id).where(sessions.c.id == session_ids[1]))
            is None
        )


async def test_absent_legacy_policy_stays_absent_and_original_policy_freezes_once(storage):
    tx, run_id, _ = storage
    policy = load_execution_policy()
    async with tx.open() as db:
        await db.execute(
            insert(evaluation_runs).values(
                run_id=str(run_id),
                organization_id=1,
                requested_by="storage-contract",
                definition_json='{"legacy":"no independent policy evidence"}',
            )
        )
        assert (await db.execute(frozen_policy_query(str(run_id)))).first() is None
        await freeze_policy(db, run_id, policy)
        await db.commit()
    async with tx.open() as db:
        before = (await db.execute(frozen_policy_query(str(run_id)))).mappings().one()
        assert before["definition_json"] == policy.definition_json
        with pytest.raises(CheckpointConflict, match="already frozen"):
            await freeze_policy(db, run_id, policy)
        await db.rollback()
    async with tx.open() as db:
        assert (await db.execute(frozen_policy_query(str(run_id)))).mappings().one() == before


async def test_partial_frozen_policy_pair_is_rejected(storage):
    tx, run_id, _ = storage
    async with tx.open() as db:
        with pytest.raises(DBAPIError) as error:
            await db.execute(
                insert(evaluation_runs).values(
                    run_id=str(run_id),
                    organization_id=1,
                    requested_by="storage-contract",
                    definition_json="{}",
                    frozen_execution_policy_fingerprint="sha256:" + "a" * 64,
                )
            )
        assert error.value.orig.args[0] == 3819
        await db.rollback()
