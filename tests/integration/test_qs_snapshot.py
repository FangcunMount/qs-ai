from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from qs_ai.application.execution.worker import ExecuteNext
from qs_ai.application.interpretation.ports import (
    AccessDenied,
    DependencyUnavailable,
    WorkflowResult,
)
from qs_ai.application.interpretation.service import InterpretationService
from qs_ai.domain.interpretation.model import Actor, EvidenceItem, Fact, RuleViolation
from qs_ai.infrastructure.interpretation.unconfigured import UnconfiguredEvidenceSource
from qs_ai.infrastructure.persistence.mysql.interpretation import MySQLUnitOfWorkFactory
from qs_ai.infrastructure.persistence.mysql.schema import evidence_sets
from tests.integration.test_interpretation import kit  # noqa: F401

pytestmark = pytest.mark.integration


async def test_snapshot_rechecks_access_without_rereading_facts_and_rejects_changed_replay(kit):  # noqa: F811
    source = kit.source
    service = InterpretationService(MySQLUnitOfWorkFactory(kit.transactions), source)
    item = EvidenceItem(
        "42", "7", "99", "standard-v1:100", (Fact("standard_report", '{"score":12}'),)
    )
    request_id = str(uuid4())
    receipt = await service.start_external(kit.actor, "7", ("42",), "goal", request_id, (item,))
    assert (
        await service.start_external(kit.actor, "7", ("42",), "goal", request_id, (item,))
        == receipt
    )
    with pytest.raises(RuleViolation, match="idempotency_conflict"):
        await service.start_external(
            kit.actor, "7", ("42",), "goal", request_id, (replace(item, report_id="101"),)
        )

    class Workflow:
        async def execute(self, claim, evidence):
            assert evidence.items == (item,)
            return WorkflowResult("", failure_code="model_not_connected")

    assert await ExecuteNext(kit.store, source, Workflow()).once()
    view = await service.get(kit.actor, receipt.session_id)
    assert view.session.failure_code == "model_not_connected"
    assert source.reads == 0
    with pytest.raises(AccessDenied):
        await service.get(Actor("1", "someone-else"), receipt.session_id)


async def test_snapshot_subject_mismatch_and_transaction_failure_do_not_persist(kit, monkeypatch):  # noqa: F811
    import qs_ai.infrastructure.persistence.mysql.interpretation as persistence

    service = InterpretationService(
        MySQLUnitOfWorkFactory(kit.transactions), UnconfiguredEvidenceSource()
    )
    item = EvidenceItem("42", "7", "99", "v1", (Fact("standard_report", "{}"),))
    with pytest.raises(RuleViolation, match="evidence_subject_mismatch"):
        await service.start_external(kit.actor, "8", ("42",), "goal", str(uuid4()), (item,))
    async with kit.transactions.open() as db:
        before = await db.scalar(select(func.count()).select_from(evidence_sets))

    async def fail(*args):
        raise RuntimeError("injected after evidence persistence")

    monkeypatch.setattr(persistence, "stage_state", fail)
    with pytest.raises(RuntimeError):
        await service.start_external(kit.actor, "7", ("42",), "goal", str(uuid4()), (item,))
    async with kit.transactions.open() as db:
        assert await db.scalar(select(func.count()).select_from(evidence_sets)) == before


@pytest.mark.parametrize("stage", ["before", "after"])
@pytest.mark.parametrize("failure", ["denied", "unavailable"])
async def test_snapshot_current_access_required_before_execution_and_result_acceptance(
    kit,  # noqa: F811
    stage,
    failure,
):
    calls = []

    class Source:
        async def authorize(self, *args):
            calls.append("authorize")
            if stage == "before" or calls.count("authorize") == 2:
                if failure == "denied":
                    raise AccessDenied
                raise DependencyUnavailable

        async def read(self, *args):
            pytest.fail("Frozen report must not be reread")

    class Workflow:
        async def execute(self, claim, evidence):
            calls.append("model")
            return WorkflowResult("checkpoint", question="must not be published")

    service = InterpretationService(MySQLUnitOfWorkFactory(kit.transactions), Source())
    item = EvidenceItem("42", "7", "99", "v1", (Fact("standard_report", "{}"),))
    receipt = await service.start_external(kit.actor, "7", ("42",), "goal", str(uuid4()), (item,))
    assert await ExecuteNext(kit.store, Source(), Workflow()).once()
    view = await service.get(kit.actor, receipt.session_id)
    assert view.session.failure_code == (
        "access_revoked" if failure == "denied" else "dependency_unavailable"
    )
    assert view.question is None
    assert calls == (["authorize"] if stage == "before" else ["authorize", "model", "authorize"])
