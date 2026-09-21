from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.domain.evaluation.checkpoint import ExecutionCheckpoint
from qs_ai.infrastructure.persistence.mysql.evaluation_checkpoints import encode
from qs_ai.infrastructure.persistence.mysql.evaluation_slot_claims import (
    SlotClaim,
    terminal_dispatches,
)


def evidence():
    at = datetime(2026, 9, 21, tzinfo=UTC)
    cp = ExecutionCheckpoint(
        "exec:1",
        "generation",
        "case:1",
        1,
        "",
        1,
        "owner:1",
        "inv:1",
        "dispatching",
        at,
        at + timedelta(minutes=5),
        at,
    )
    row = {
        key: getattr(cp, key)
        for key in (
            "invocation_id",
            "execution_id",
            "kind",
            "case_id",
            "slot_ordinal",
            "candidate_id",
        )
    }
    row["checkpoint_json"] = encode(cp)
    return row, SlotClaim(uuid4(), 1, cp)


def test_exact_live_dispatch_can_wait_without_fabricating_terminal_evidence():
    row, claim = evidence()
    renewed = replace(
        claim,
        checkpoint=replace(
            claim.checkpoint,
            lease_expires_at=claim.checkpoint.lease_expires_at + timedelta(seconds=30),
        ),
    )
    assert terminal_dispatches([row], [], (renewed,)) == []
    assert terminal_dispatches([row], [{"invocation_id": "inv:1"}], ()) == [row]


@pytest.mark.parametrize(
    "field,value",
    [
        ("invocation_id", "other:1"),
        ("case_id", "other:1"),
        ("execution_id", "other:1"),
        ("slot_ordinal", 2),
    ],
)
def test_corrupt_ledger_index_cannot_be_hidden(field, value):
    row, claim = evidence()
    row[field] = value
    with pytest.raises(CheckpointConflict):
        terminal_dispatches([row], [], (claim,))


def test_unowned_or_missing_dispatch_requires_recovery():
    row, claim = evidence()
    with pytest.raises(CheckpointConflict):
        terminal_dispatches([row], [], ())
    with pytest.raises(CheckpointConflict):
        terminal_dispatches([], [], (claim,))
    with pytest.raises(CheckpointConflict):
        terminal_dispatches([row, row], [], (claim,))
