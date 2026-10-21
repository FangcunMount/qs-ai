"""Serial response facts use their own explicit ownership codec, never a SlotClaim."""

import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.interpretation.provider import ModelResponse
from qs_ai.domain.evaluation.checkpoint import ExecutionCheckpoint
from qs_ai.infrastructure.persistence.mysql import evaluation_serial_response_receipts as module
from qs_ai.infrastructure.persistence.mysql.evaluation_response_receipts import EvaluationResponse
from qs_ai.infrastructure.persistence.mysql.evaluation_response_receipts import (
    decode as candidate_decode,
)


@pytest.fixture
def original():
    at = datetime(2026, 10, 22, tzinfo=UTC)
    cp = ExecutionCheckpoint(
        "execution:first",
        "generation",
        "case:first",
        1,
        "",
        1,
        "worker:first",
        "invocation:first",
        "dispatching",
        at,
        at + timedelta(seconds=60),
        at,
    )
    ref = module.SerialResponseReference(uuid4(), 1, 5, cp)
    response = ModelResponse(
        cp.invocation_id,
        "request:first",
        "model:first",
        '{"原始":1}',
        '{"原始":1}',
        "none",
        None,
        None,
        100,
    )
    result = EvaluationResponse(response, None, at + timedelta(seconds=1))
    raw = module.encode(ref, result)
    digest = hashlib.sha256(raw.encode()).hexdigest()
    return SimpleNamespace(**locals())


def test_serial_first_response_codec_keeps_exact_original_outputs_and_unknown_usage(original):
    p = original
    assert module.decode(p.ref, p.raw, p.digest) == p.result
    assert module.encode(p.ref, module.decode(p.ref, p.raw, p.digest)) == p.raw
    assert p.result.response.input_tokens is p.result.response.output_tokens is None
    with pytest.raises(ValueError):
        candidate_decode(p.raw, p.digest)
    old = p.result.encode()
    with pytest.raises(ValueError):
        module.decode(p.ref, old, hashlib.sha256(old.encode()).hexdigest())


@pytest.mark.parametrize(
    "damage", ["extra", "duplicate", "whitespace", "version", "reference", "digest"]
)
def test_inexact_or_ambiguous_serial_evidence_cannot_be_used(original, damage):
    p = original
    value = json.loads(p.raw)
    if damage == "extra":
        value["extra"] = True
    elif damage == "reference":
        value["reference"]["version"] += 1
    elif damage == "version":
        value["schema_version"] = "qs-ai-evaluation-response/v1"
    raw = module._json(value)
    if damage == "whitespace":
        raw += " "
    elif damage == "duplicate":
        raw = raw[:-1] + ',"schema_version":"qs-ai-evaluation-serial-response/v1"}'
    digest = hashlib.sha256(raw.encode()).hexdigest() if damage != "digest" else "0" * 64
    with pytest.raises(ValueError):
        module.decode(p.ref, raw, digest)


@pytest.mark.parametrize(
    "field,value",
    [
        ("organization_id", True),
        ("organization_id", 0),
        ("organization_id", 2**63),
        ("version", True),
        ("version", 0),
        ("version", 2**63),
    ],
)
def test_serial_reference_rejects_wrong_scope_or_version_before_database(original, field, value):
    with pytest.raises(ValueError):
        replace(original.ref, **{field: value})


def test_serial_reference_never_fabricates_candidate_ownership(original):
    with pytest.raises(ValueError, match="candidate"):
        module.SerialResponseReference.from_prepared(1, SimpleNamespace(claim=object()))


def test_serial_response_requires_original_invocation_and_dispatch_clock(original, monkeypatch):
    p = original
    with pytest.raises(ValueError):
        module.encode(
            p.ref, replace(p.result, response=replace(p.response, invocation_id="other:invocation"))
        )
    with pytest.raises(ValueError):
        module.encode(p.ref, replace(p.result, finished_at=p.at - timedelta(seconds=1)))
    monkeypatch.setattr(module, "_MAX_BYTES", 16)
    with pytest.raises(ValueError):
        module.encode(p.ref, p.result)


@pytest.fixture
def writer(original, monkeypatch):
    p = original
    root = SimpleNamespace(is_active=True)
    db = AsyncMock(spec=AsyncSession)
    db.get_transaction = Mock(return_value=root)
    db.in_nested_transaction = Mock(return_value=False)
    binding = SimpleNamespace(validate=AsyncMock())
    monkeypatch.setattr(module, "bind", lambda _: binding)

    def row(value):
        result = Mock()
        result.mappings.return_value.one_or_none.return_value = value
        return result

    cp_row = dict(version=p.ref.version, checkpoint_json=module.encode_checkpoint(p.cp))
    run = dict(execution_mode="serial_v1", progress_json={})
    db.execute.side_effect = [row(cp_row), row(run), row(None), Mock()]
    db.scalar.return_value = module.encode_checkpoint(p.cp)
    return SimpleNamespace(**locals())


async def test_serial_response_stages_only_evidence_on_original_root_without_owning_it(writer):
    w, p = writer, writer.p
    await module.save_serial_response(w.db, p.ref, p.result)
    statement = w.db.execute.await_args.args[0]
    assert statement.is_insert and statement.table is module.evaluation_response_receipts
    assert w.db.execute.await_count == 4 and w.binding.validate.await_count == 2
    for method in (w.db.commit, w.db.rollback, w.db.connection):
        method.assert_not_called()
    assert w.db.get_transaction() is w.root and w.root.is_active


@pytest.mark.parametrize("different", [False, True])
async def test_first_serial_response_is_immutable_even_after_original_checkpoint_finished(
    writer, different
):
    w, p = writer, writer.p
    existing = dict(
        execution_id=p.cp.execution_id,
        claim_version=p.ref.version,
        definition_json=p.raw,
        sha256=p.digest,
    )
    w.db.execute.side_effect = [w.row(None), w.row(w.run), w.row(existing)]
    result = (
        replace(p.result, finished_at=p.result.finished_at + timedelta(seconds=1))
        if different
        else p.result
    )
    if different:
        with pytest.raises(CheckpointConflict, match="Immutable"):
            await module.save_serial_response(w.db, p.ref, result)
    else:
        await module.save_serial_response(w.db, p.ref, result)
    assert w.db.execute.await_count == 3
    w.db.scalar.assert_not_called()
    w.db.commit.assert_not_called()


@pytest.mark.parametrize("fault", ["nested", "ended", "inactive"])
async def test_serial_response_refuses_invalid_original_root_before_sql(writer, fault):
    from reliable_messaging.transaction import BindingError

    w, p = writer, writer.p
    if fault == "nested":
        w.db.in_nested_transaction.return_value = True
    elif fault == "ended":
        w.db.get_transaction.return_value = None
    else:
        w.root.is_active = False
    with pytest.raises(BindingError):
        await module.save_serial_response(w.db, p.ref, p.result)
    w.db.execute.assert_not_called()
