"""Original preflight completion rules can be planned before the guarded root."""

import copy
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.domain.evaluation.preflight import AssertionReceipt, PreflightEvidence
from qs_ai.infrastructure.persistence.mysql import evaluation_progress as progress


def fixture():
    run_id, at = uuid4(), datetime(2026, 10, 21, tzinfo=UTC)
    checkpoint = {"run_id": str(run_id), "version": 2, "checkpoint_json": None}
    creation = {"audit": {"created_at": at.isoformat()}, "preflight": {"case_id": "case:reject"}}
    run = {
        "run_id": str(run_id),
        "organization_id": 7,
        "definition_json": json.dumps(creation),
        "progress_json": {"status": "collecting", "transitions": []},
    }
    evidence = PreflightEvidence(
        "case:reject",
        "passed",
        at,
        0,
        "insufficient_dimensions",
        tuple(
            AssertionReceipt(name, "case", 1, True, "preflight", "passed", detail)
            for name, detail in (
                ("provider_call_count", "0"),
                ("rejection_reason", "insufficient_dimensions"),
            )
        ),
    )
    return SimpleNamespace(**locals())


@pytest.mark.parametrize("passed", [True, False])
def test_original_projection_keeps_version_exact_evidence_audit_and_read_snapshot(passed):
    f = fixture()
    if not passed:
        f.evidence = replace(f.evidence, status="failed")
    before = copy.deepcopy((f.checkpoint, f.run, f.evidence))
    state, updated = progress.plan_preflight_completion(
        f.run_id, 2, 7, f.evidence, f.checkpoint, f.run
    )
    assert state.version == 3 and state.checkpoint is None
    assert updated["status"] == ("collecting" if passed else "blocked")
    assert updated["preflight"]["evaluated_at"] == f.at.isoformat()
    assert updated["preflight"]["provider_call_count"] == 0
    assert updated["preflight"]["assertions"][0]["type"] == "provider_call_count"
    assert updated["transitions"] == (
        []
        if passed
        else [
            {
                "from": "collecting",
                "to": "blocked",
                "cause_code": "preflight_failed",
                "actor": "system:runner",
                "at": f.at.isoformat(),
                "evidence_refs": ["case:reject"],
            }
        ]
    )
    assert (f.checkpoint, f.run, f.evidence) == before


@pytest.mark.parametrize(
    "damage",
    ["version", "active", "missing", "organization", "status", "duplicate", "source", "time"],
)
def test_preflight_projection_refuses_changed_source_or_original_business_conditions(damage):
    f = fixture()
    if damage == "version":
        f.checkpoint["version"] = 3
    elif damage == "active":
        f.checkpoint["checkpoint_json"] = {"dispatched": True}
    elif damage == "missing":
        f.run = None
    elif damage == "organization":
        f.run["organization_id"] = 8
    elif damage == "status":
        f.run["progress_json"]["status"] = "blocked"
    elif damage == "duplicate":
        f.run["progress_json"]["preflight"] = {"already": True}
    elif damage == "source":
        f.evidence = replace(f.evidence, case_id="case:other")
    else:
        f.evidence = replace(f.evidence, evaluated_at=f.at - timedelta(seconds=1))
    with pytest.raises((CheckpointConflict, ValueError)):
        progress.plan_preflight_completion(f.run_id, 2, 7, f.evidence, f.checkpoint, f.run)


async def test_original_adapter_keeps_locked_reads_cas_and_host_commit_ownership(monkeypatch):
    f = fixture()

    def selected(row):
        result = Mock()
        result.mappings.return_value.one_or_none.return_value = row
        return result

    db = SimpleNamespace(
        execute=AsyncMock(side_effect=[selected(f.checkpoint), selected(f.run), Mock()]),
        commit=AsyncMock(),
    )
    save = AsyncMock()
    monkeypatch.setattr(progress, "save_checkpoint", save)
    state = await progress.complete_preflight(db, f.run_id, 2, 7, f.evidence)
    assert state.version == 3
    save.assert_awaited_once_with(db, state, 2)
    assert all(c.args[0]._for_update_arg is not None for c in db.execute.await_args_list[:2])
    assert db.execute.await_args.args[0].is_update
    db.commit.assert_not_called()
