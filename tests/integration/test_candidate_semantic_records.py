"""Virtual candidate terminal compared with the real original acceptor and root rollback."""

import json
from dataclasses import replace

import pytest
from sqlalchemy import delete, update

from qs_ai.infrastructure.persistence.mysql.completion_records import (
    generation_completions,
    select_records,
    semantic_completions,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_candidate_completion import (
    candidate_completion_progress,
    completion_owner,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_candidate_plan import (
    project_candidate_records,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_candidate_records import (
    semantic_terminal_records,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_completion_progress import (
    semantic_completion_progress,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_semantic import (
    complete_semantic,
    load_serial_semantic_state,
    read_semantic_acceptance,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_slot_claims import (
    active_claims,
    create_claim,
)
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_checkpoints,
    evaluation_dispatches,
    evaluation_runs,
    evaluation_slot_claims,
)
from tests.integration.test_evaluation_completions import dispatched as dispatched
from tests.integration.test_evaluation_completions import stored
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_semantic_completions import evidence
from tests.integration.test_semantic_completions import judge as judge

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("other", [False, True])
@pytest.mark.parametrize("commit", [False, True])
async def test_virtual_candidate_semantic_records_match_original_root_commit_and_rollback(
    judge, other, commit
):
    tx, run_id, value, routes = judge
    # Isolated original evidence is reused to seed candidate ownership. This is
    # neither a production migration nor permission to convert live serial Runs.
    async with tx.open() as db:
        serial = await load_serial_semantic_state(db, run_id, 8, 1, lock=True)
        await db.execute(
            update(evaluation_runs)
            .where(evaluation_runs.c.run_id == str(run_id))
            .values(execution_mode="candidate_v2")
        )
        await db.execute(
            update(evaluation_checkpoints)
            .where(evaluation_checkpoints.c.run_id == str(run_id))
            .values(checkpoint_json=None)
        )
        claim = await create_claim(db, run_id, serial.checkpoint, 8)
        if other:
            await create_claim(
                db,
                run_id,
                replace(
                    serial.checkpoint,
                    kind="generation",
                    candidate_id="",
                    slot_ordinal=2,
                    execution_id="generation:other",
                    invocation_id="invocation:other",
                    phase="prepared",
                    dispatch_started_at=None,
                ),
                7,
            )
        await db.commit()
    try:
        before, generations_before = await rows(tx, run_id), await stored(tx, run_id)
        async with tx.open() as db:
            run, version, current = await completion_owner(db, run_id, 1, claim, lock=False)
            active = await active_claims(db, run_id, lock=False)
            accepted = await read_semantic_acceptance(
                db, run_id, 1, "worker:2", value, run, current.checkpoint, routes, lock=False
            )
            records = []
            for table in (evaluation_dispatches, generation_completions, semantic_completions):
                records.append(
                    list(
                        (
                            await db.execute(
                                select_records(table).where(table.c.run_id == str(run_id))
                            )
                        )
                        .mappings()
                        .all()
                    )
                )
            virtual = semantic_terminal_records(
                run_id,
                current,
                active,
                value,
                accepted,
                dispatches=records[0],
                generations=records[1],
                semantics=records[2],
            )
            progress = semantic_completion_progress(
                run["progress_json"], value, accepted.policy, "worker:2", candidate_claim=True
            )
            plan = await project_candidate_records(
                accepted.creation,
                progress,
                accepted.policy,
                virtual.active,
                32,
                dispatches=list(virtual.dispatches),
                generations=list(virtual.generations),
                semantics=list(virtual.semantics),
            )
            projected = await candidate_completion_progress(
                db,
                run_id,
                current,
                progress,
                "worker:2",
                value.finished_at,
                active=virtual.active,
                plan=plan,
            )
        assert version == 8 and await rows(tx, run_id) == before
        assert await stored(tx, run_id) == generations_before and await evidence(tx, run_id) == []
        async with tx.open() as db:
            actual = await complete_semantic(
                db, run_id, 8, 1, "worker:2", value, routes, claim=claim
            )
            assert actual.version == 9 and actual.checkpoint is None
            if commit:
                await db.commit()
            else:
                await db.rollback()
        async with tx.open() as db:
            remaining = await active_claims(db, run_id, lock=False)
        if commit:
            assert remaining == virtual.active
            assert (await rows(tx, run_id))[0]["progress_json"] == json.loads(json.dumps(projected))
            (record,) = await evidence(tx, run_id)
            assert record["raw_output"] == virtual.semantics[-1]["raw_output"]
            assert record["evidence_json"] == json.loads(
                json.dumps(virtual.semantics[-1]["evidence_json"])
            )
            candidate = (await stored(tx, run_id))[0]
            assert candidate["candidate_json"] == json.loads(
                json.dumps(virtual.generations[0]["candidate_json"])
            )
            assert candidate["raw_output"] == generations_before[0]["raw_output"]
        else:
            assert remaining == active
            assert (
                await rows(tx, run_id) == before and await stored(tx, run_id) == generations_before
            )
            assert await evidence(tx, run_id) == []
    finally:
        async with tx.open() as db:
            await db.execute(
                delete(evaluation_slot_claims).where(evaluation_slot_claims.c.run_id == str(run_id))
            )
            await db.commit()
