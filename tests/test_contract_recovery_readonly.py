"""Original operator exception retains exact failed evidence, policy and lock order."""

import json
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from sqlalchemy.sql import Select

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.infrastructure.persistence.mysql import evaluation_contract_recovery as recovery
from tests.test_evaluation_case import release
from tests.test_semantic_contract_recovery import recovered_inventory


@pytest.fixture
def original(monkeypatch):
    run_id = uuid4()
    data = recovered_inventory()
    value, target = data["contract_recoveries"][0], data["semantics"][0]
    identity = release()
    checkpoint = dict(version=7, checkpoint_json=None)
    progress = dict(
        status="blocked",
        transitions=[
            dict(
                cause_code="semantic_recovery_not_allowed",
                evidence_refs=[value.execution_id],
                at=target.finished_at.isoformat(),
            )
        ],
        preflight=dict(status="passed", case_id="original"),
        retained_field={"original": [1, 2]},
    )
    run = dict(
        definition_json=json.dumps(
            dict(release=asdict(identity), release_fingerprint=identity.fingerprint(), slots=[])
        ),
        progress_json=progress,
    )
    order, written = [], []

    class DB:
        async def execute(self, statement):
            if isinstance(statement, Select):
                table = statement.get_final_froms()[0].name
                order.append((table, statement._for_update_arg is not None))
                row = checkpoint if table == "evaluation_checkpoints" else run
                return SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: row))
            order.append(("write_run",))
            written.append(statement.compile().params["progress_json"])

    db = DB()
    evidence = SimpleNamespace(
        unknowns=(),
        resolutions=(),
        semantics=[{}],
        generations=[],
        dispatches=[],
        policy=data["policy"],
    )
    load = AsyncMock(return_value=evidence)
    monkeypatch.setattr(recovery, "load_resolution_evidence", load)
    monkeypatch.setattr(recovery, "decode_semantic_completion", Mock(return_value=target))
    project = Mock(return_value=())
    monkeypatch.setattr(recovery, "project_slots", project)
    action = Mock(
        return_value=SimpleNamespace(
            kind="semantic",
            candidate_id=value.candidate_id,
            execution_ordinal=2,
            cause="semantic_contract_recovery_approved",
        )
    )
    monkeypatch.setattr(recovery, "next_action", action)

    async def save(_db, state, expected):
        assert _db is db and expected == 7 and state.run_id == run_id
        order.append(("checkpoint_cas", state.version))

    save_checkpoint = AsyncMock(side_effect=save)
    monkeypatch.setattr(recovery, "save_checkpoint", save_checkpoint)
    return SimpleNamespace(**locals())


async def test_original_operator_recovery_keeps_lock_order_first_audit_and_immutable_evidence(
    original,
):
    p = original
    before = json.dumps(p.progress)
    state = await recovery.authorize_contract_recovery(
        p.db, p.run_id, 7, 1, p.identity.fingerprint(), p.value, confirm=True
    )
    assert state.version == 8 and state.checkpoint is None
    assert p.order == [
        ("evaluation_checkpoints", True),
        ("evaluation_runs", True),
        ("checkpoint_cas", 8),
        ("write_run",),
    ]
    assert json.dumps(p.progress) == before
    updated = p.written[0]
    assert updated["status"] == "collecting"
    assert updated["retained_field"] == p.progress["retained_field"]
    assert (
        updated["semantic_contract_recoveries"][0]["resolved_at"] == p.value.resolved_at.isoformat()
    )
    assert (
        updated["semantic_contract_recoveries"][0]["instruction_fingerprint"]
        == p.value.instruction_fingerprint
    )
    assert updated["transitions"][-1]["evidence_refs"] == [p.value.execution_id]
    assert p.data["semantics"][0] is p.target


@pytest.mark.parametrize(
    "fault", ["unconfirmed", "version", "release", "unknown", "fingerprint", "action"]
)
async def test_original_invalid_recovery_never_writes_checkpoint_or_progress(original, fault):
    p = original
    confirm, fingerprint, value = True, p.identity.fingerprint(), p.value
    if fault == "unconfirmed":
        confirm = False
    elif fault == "version":
        p.checkpoint["version"] += 1
    elif fault == "release":
        fingerprint = "sha256:" + "f" * 64
    elif fault == "unknown":
        p.evidence.unknowns = (object(),)
    elif fault == "fingerprint":
        value = replace(value, output_fingerprint="sha256:" + "f" * 64)
    else:
        p.action.return_value = SimpleNamespace(
            kind="terminal", candidate_id="", execution_ordinal=0, cause=""
        )
    with pytest.raises((ValueError, CheckpointConflict)):
        await recovery.authorize_contract_recovery(
            p.db, p.run_id, 7, 1, fingerprint, value, confirm=confirm
        )
    p.save_checkpoint.assert_not_called()
    assert not p.written
    if fault == "unconfirmed":
        assert not p.order


async def test_readonly_plan_never_locks_or_writes_and_keeps_original_audit_result(original):
    p = original
    plan = await recovery.plan_contract_recovery(
        p.db, p.run_id, 7, 1, p.identity.fingerprint(), p.value, confirm=True
    )
    assert p.order == [("evaluation_checkpoints", False), ("evaluation_runs", False)]
    assert plan.state.version == 8 and plan.state.checkpoint is None
    p.save_checkpoint.assert_not_called()
    assert not p.written
    changed = plan.progress()
    changed["retained_field"]["original"].append(3)
    assert plan.progress()["retained_field"] == {"original": [1, 2]}
    assert "retained_field" not in repr(plan)
    await recovery.authorize_contract_recovery(
        p.db, p.run_id, 7, 1, p.identity.fingerprint(), p.value, confirm=True
    )
    assert p.written[0] == plan.progress()
