from unittest.mock import AsyncMock

import pytest

from qs_ai.application.execution.worker import ExecuteNext
from qs_ai.application.interpretation.ports import Claim, WorkflowResult
from qs_ai.domain.interpretation.model import Actor, Session


@pytest.mark.parametrize(
    "workflow_version", ["interpretation-v1", "qs-snapshot-v1", "qs-published-snapshot-v1"]
)
async def test_missing_snapshot_never_reads_fresh_facts_or_calls_model(workflow_version):
    session = Session("session", Actor("1", "owner"), "7", ("42",), "goal")
    session.workflow_version = workflow_version
    claim = Claim("job", "run", session, 1, None, False, None)
    source = AsyncMock()
    store = AsyncMock()
    store.claim.return_value = claim
    store.evidence.return_value = None
    workflow = AsyncMock()
    assert await ExecuteNext(store, source, workflow).once()
    source.read.assert_not_called()
    store.freeze.assert_not_called()
    workflow.execute.assert_not_called()
    store.finish.assert_awaited_once_with(
        claim, WorkflowResult("", failure_code="evidence_invalid")
    )
