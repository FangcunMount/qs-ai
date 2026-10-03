import pytest

from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.infrastructure.persistence.mysql.messaging import outbox
from tests.integration.test_evaluation_candidate_execution import parallel_run as parallel_run
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_evaluation_step import persisted_assets as persisted_assets
from tests.integration.test_evaluation_step import ready as ready
from tests.integration.test_interpretation import kit as kit
from tests.integration.test_mq_admission import mq_env as mq_env
from tests.integration.test_mq_admission import saved
from tests.integration.test_mq_storage import keys as keys

pytestmark = pytest.mark.integration


async def test_full_candidate_worker_commits_monotone_state_events_without_extra_calls(
    parallel_run, kit, keys, mq_env
):
    from tests.integration.test_evaluation_candidate_execution import (
        test_full_frozen_plan_completes_through_worker,
    )

    tx, run_id, *_ = parallel_run
    tx.database.state_events = mq_env
    try:
        await test_full_frozen_plan_completes_through_worker(parallel_run)
        rows = [r for r in await saved(tx, outbox) if r["kind"] == pb.EVALUATION_STATE]
        states = sorted(
            (pb.MessagingBody.FromString(r["body"]).evaluation_state for r in rows),
            key=lambda s: s.event_sequence,
        )
        assert len(states) > 70
        assert [s.event_sequence for s in states] == list(range(1, len(states) + 1))
        assert len({s.version for s in states}) == len(states)
        assert states[-1].status == "awaiting_review"
        assert all(s.run_id == str(run_id) for s in states)
    finally:
        tx.database.state_events = None
