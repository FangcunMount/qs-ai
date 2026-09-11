import json
import os
from dataclasses import fields
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import delete, insert, select
from sqlalchemy.exc import IntegrityError

from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity, FrozenContractRef
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_runs import create_run
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_checkpoints,
    evaluation_run_policies,
    evaluation_runs,
)
from qs_ai.infrastructure.qs_server.evaluation_policies import (
    load_execution_policy,
    load_gate_policy,
)
from qs_ai.infrastructure.qs_server.evaluation_suite import V6

pytestmark = pytest.mark.integration


@pytest.fixture
async def setup_run():
    dsn = os.getenv("QS_AI_TEST_MYSQL_DSN")
    if not dsn:
        pytest.skip("Requires migrated disposable MySQL")
    database = Database(dsn.replace("mysql://", "mysql+asyncmy://", 1))
    tx = Transactions(database)
    run_id = uuid4()
    execution = load_execution_policy()
    # Placeholder asset refs isolate storage atomicity; this is not an eligible release.
    refs = {
        f.name: FrozenContractRef(f.name, "v1", "sha256:" + "a" * 64)
        for f in fields(EvidenceReleaseIdentity)
    }
    refs.update(
        suite=V6,
        execution_policy=FrozenContractRef(
            execution.policy_id, execution.version, execution.fingerprint
        ),
        gate_policy=load_gate_policy().reference,
    )
    release = EvidenceReleaseIdentity(**refs)
    try:
        yield tx, run_id, release
    finally:
        async with tx.open() as db:
            for table in (evaluation_checkpoints, evaluation_run_policies, evaluation_runs):
                await db.execute(delete(table).where(table.c.run_id == str(run_id)))
            await db.commit()
        await database.close()


async def rows(tx, run_id):
    async with tx.open() as db:
        return [
            (await db.execute(select(table).where(table.c.run_id == str(run_id))))
            .mappings()
            .one_or_none()
            for table in (evaluation_runs, evaluation_run_policies, evaluation_checkpoints)
        ]


async def create(db, run_id, release):
    return await create_run(
        db, run_id, release, 1, "actor:1", "评测迁移验证", datetime(2026, 9, 12, tzinfo=UTC)
    )


async def test_creation_freezes_documents_slots_audit_and_one_initial_version(setup_run):
    tx, run_id, release = setup_run
    async with tx.open() as db:
        state = await create(db, run_id, release)
        await db.commit()
    run, policy, checkpoint = await rows(tx, run_id)
    definition = json.loads(run["definition_json"])
    assert len(definition["slots"]) == 35
    assert definition["preflight"] == {"case_id": "PROMPT-EVAL-008", "status": "pending"}
    assert definition["release_fingerprint"] == release.fingerprint()
    assert definition["execution_policy_json"] == policy["definition_json"]
    assert definition["gate_policy_json"] == load_gate_policy().definition_json
    assert definition["audit"]["requested_by"] == run["requested_by"] == "actor:1"
    assert state.version == checkpoint["version"] == 1
    assert checkpoint["checkpoint_json"] is None
    before = await rows(tx, run_id)
    with pytest.raises(IntegrityError):
        async with tx.open() as db:
            await create(db, run_id, release)
            await db.commit()
    assert await rows(tx, run_id) == before


@pytest.mark.parametrize("conflict", ["policy", "checkpoint", "after_writes"])
async def test_failure_at_each_creation_write_rolls_back_all_new_records(setup_run, conflict):
    tx, run_id, release = setup_run
    async with tx.open() as db:
        if conflict == "policy":
            await db.execute(
                insert(evaluation_run_policies).values(
                    run_id=str(run_id), fingerprint="prior", definition_json="{}"
                )
            )
        elif conflict == "checkpoint":
            await db.execute(insert(evaluation_checkpoints).values(run_id=str(run_id), version=9))
        await db.commit()
    before = await rows(tx, run_id)
    with pytest.raises((IntegrityError, RuntimeError)):
        async with tx.open() as db:
            await create(db, run_id, release)
            raise RuntimeError("injected before commit")
    assert await rows(tx, run_id) == before
