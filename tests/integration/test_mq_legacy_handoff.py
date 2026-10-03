"""Explicit single-row transfer on real MySQL; no model, Broker, or migration scanner."""

import hashlib
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from reliable_messaging.durable import HELD, MessageConflict
from reliable_messaging.protected import TrustedSigner
from reliable_messaging.sqlalchemy import TransactionBindingError
from reliable_messaging.wire import Envelope, decode, encode
from sqlalchemy import update

from qs_ai.infrastructure.persistence.mysql.messaging import outbox
from qs_ai.infrastructure.persistence.mysql.schema import jobs, model_calls, result_outbox
from qs_ai.infrastructure.workflow_transport.messaging import EVENTS, authenticate
from tests.integration.test_interpretation import kit as kit
from tests.integration.test_mq_admission import locked_result, saved
from tests.integration.test_mq_admission import mq_env as mq_env
from tests.integration.test_mq_storage import keys as keys
from tests.test_input_binding import bound_case

pytestmark = pytest.mark.integration


@pytest.fixture
async def legacy(kit, keys, mq_env):
    tx = kit.transactions
    tx.database.state_events = None
    await kit.service.start_external(
        kit.actor, "7", ("42",), "单行历史移交", str(uuid4()), bound_case()[1].items
    )
    row = next(
        r
        for r in await saved(tx, result_outbox)
        if r["payload"]["actor"] == {"org_id": kit.actor.org_id, "subject_id": kit.actor.subject_id}
    )
    tx.database.state_events = mq_env
    return tx, row["event_id"], mq_env, keys


@pytest.mark.parametrize("attempts", [0, 3, 8, 10])
@pytest.mark.parametrize("unknown_time", [False, True])
async def test_single_row_preserves_time_budget_and_reuses_first_wire(
    legacy, monkeypatch, attempts, unknown_time
):
    tx, event_id, recorder, keys = legacy
    available = datetime(2026, 9, 22, 11, 22, 33, 444555)
    occurred = None if unknown_time else datetime(2026, 9, 21, 1, 2, 3, 456789)
    async with tx.open() as db:
        await db.execute(
            update(result_outbox)
            .where(result_outbox.c.event_id == event_id)
            .values(attempts=attempts, available_at=available, created_at=occurred)
        )
        await db.commit()
    before = (await saved(tx, result_outbox))[0]
    async with tx.open() as db:
        await db.begin()
        assert await recorder.record_legacy_interpretation(db, await locked_result(db, event_id))
        assert db.in_transaction()
        await db.commit()
    after = (await saved(tx, result_outbox))[0]
    assert {k: v for k, v in after.items() if k != "mq_owned"} == {
        k: v for k, v in before.items() if k != "mq_owned"
    }
    assert after["mq_owned"] and not after["delivered"]
    first = (await saved(tx, outbox))[0]
    assert (first["attempts"], first["available_at"]) == (attempts, available)
    assert first["stage"] == (HELD if attempts >= 8 else "staged")
    assert first["error_code"] == ("delivery_budget_exhausted" if attempts >= 8 else "")
    envelope = authenticate(
        first["wire"],
        EVENTS,
        decrypt_keys={"qs.encrypt": keys["qs.encrypt"]},
        trusted_signers={"ai.sign": TrustedSigner("qs-ai", keys["ai.sign"])},
    )
    assert envelope.original_occurred_at == (
        occurred.replace(tzinfo=UTC).isoformat() if occurred else ""
    )
    assert envelope.message_id == event_id
    # Relay changes belong to the MQ row. A repeated owned source must not compare
    # its old budget/time with the current Relay state, nor seal with rotated keys.
    async with tx.open() as db:
        await db.execute(
            update(outbox).values(attempts=attempts + 1, available_at=datetime(2026, 10, 4))
        )
        await db.commit()
    current = await saved(tx, outbox)

    def forbid(*args, **kwargs):
        pytest.fail("duplicate transfer must not seal")

    monkeypatch.setattr("qs_ai.infrastructure.workflow_transport.state_events.prepare", forbid)
    async with tx.open() as db:
        await db.begin()
        assert not await recorder.record_legacy_interpretation(
            db, await locked_result(db, event_id)
        )
        await recorder.record_interpretation(db, await locked_result(db, event_id))
        await db.commit()
    assert await saved(tx, outbox) == current
    assert await saved(tx, jobs) == [] and await saved(tx, model_calls) == []


async def test_delivered_original_is_not_revived(legacy, monkeypatch):
    tx, event_id, recorder, _ = legacy
    async with tx.open() as db:
        await db.execute(
            update(result_outbox).where(result_outbox.c.event_id == event_id).values(delivered=True)
        )
        await db.commit()
    before = await saved(tx, result_outbox)
    monkeypatch.setattr(
        "qs_ai.infrastructure.workflow_transport.state_events.prepare",
        lambda *a, **k: pytest.fail("delivered transfer must not seal"),
    )
    async with tx.open() as db:
        await db.begin()
        assert not await recorder.record_legacy_interpretation(
            db, await locked_result(db, event_id)
        )
        await db.commit()
    assert await saved(tx, outbox) == [] and await saved(tx, result_outbox) == before


async def test_transfer_is_atomic_and_source_compare_is_exact(legacy):
    tx, event_id, recorder, _ = legacy
    before = await saved(tx, result_outbox)
    async with tx.open() as db:
        await db.begin()
        row = await locked_result(db, event_id)
        assert await recorder.record_legacy_interpretation(db, row)
        await db.rollback()
    assert await saved(tx, result_outbox) == before and await saved(tx, outbox) == []
    async with tx.open() as db:
        await db.begin()
        row = dict(await locked_result(db, event_id))
        row["attempts"] += 1
        with pytest.raises(MessageConflict, match="source changed"):
            await recorder.record_legacy_interpretation(db, row)
        await db.rollback()
    assert await saved(tx, result_outbox) == before and await saved(tx, outbox) == []
    async with tx.open() as db:
        with pytest.raises(TransactionBindingError):
            await recorder.record_legacy_interpretation(db, before[0])


@pytest.mark.parametrize(
    "damage", ["wire", "wire_digest", "wire_identity", "body", "aggregate", "owned_missing"]
)
async def test_mismatched_first_wire_or_body_never_resealed_or_repaired(
    legacy, monkeypatch, damage
):
    tx, event_id, recorder, _ = legacy
    async with tx.open() as db:
        await db.begin()
        await recorder.record_legacy_interpretation(db, await locked_result(db, event_id))
        await db.commit()
    first = (await saved(tx, outbox))[0]
    async with tx.open() as db:
        values = {}
        if damage == "wire":
            values["wire"] = b"corrupt"
        elif damage == "wire_digest":
            values["wire_sha256"] = "0" * 64
        elif damage == "wire_identity":
            frame = decode(first["wire"])
            wire = encode(Envelope(str(uuid4()), frame.payload, frame.metadata))
            values.update(wire=wire, wire_sha256=hashlib.sha256(wire).hexdigest())
        elif damage == "body":
            values.update(body=b"changed", body_sha256=hashlib.sha256(b"changed").hexdigest())
        elif damage == "aggregate":
            values["aggregate_key"] = str(uuid4())
        if damage == "owned_missing":
            await db.execute(outbox.delete())
        else:
            await db.execute(update(outbox).values(**values))
        await db.commit()
    before = await saved(tx, outbox)
    monkeypatch.setattr(
        "qs_ai.infrastructure.workflow_transport.state_events.prepare",
        lambda *a, **k: pytest.fail("conflict must not seal"),
    )
    async with tx.open() as db:
        await db.begin()
        with pytest.raises(MessageConflict):
            await recorder.record_legacy_interpretation(db, await locked_result(db, event_id))
        await db.rollback()
    assert await saved(tx, outbox) == before


@pytest.mark.parametrize("difference", [None, "attempts", "available_at", "exhausted_stage"])
async def test_unowned_first_wire_is_reused_only_with_matching_delivery_metadata(
    legacy, monkeypatch, difference
):
    tx, event_id, recorder, _ = legacy
    async with tx.open() as db:
        await db.begin()
        if difference == "exhausted_stage":
            await db.execute(update(result_outbox).values(attempts=8))
        await recorder.record_legacy_interpretation(db, await locked_result(db, event_id))
        await db.execute(update(result_outbox).values(mq_owned=False))
        if difference == "attempts":
            await db.execute(update(outbox).values(attempts=1))
        elif difference == "available_at":
            await db.execute(update(outbox).values(available_at=datetime(2026, 10, 4)))
        elif difference == "exhausted_stage":
            await db.execute(update(outbox).values(stage="staged", error_code=""))
        await db.commit()
    first = await saved(tx, outbox)
    before = await saved(tx, result_outbox)
    monkeypatch.setattr(
        "qs_ai.infrastructure.workflow_transport.state_events.prepare",
        lambda *a, **k: pytest.fail("existing wire must not seal"),
    )
    async with tx.open() as db:
        await db.begin()
        if difference is None:
            assert await recorder.record_legacy_interpretation(
                db, await locked_result(db, event_id)
            )
            await db.commit()
        else:
            with pytest.raises(MessageConflict, match="delivery metadata differs"):
                await recorder.record_legacy_interpretation(db, await locked_result(db, event_id))
            await db.rollback()
    assert await saved(tx, outbox) == first
    if difference is None:
        assert (await saved(tx, result_outbox))[0]["mq_owned"]
    else:
        assert await saved(tx, result_outbox) == before


async def test_storage_failure_rolls_back_single_row_transfer(legacy, monkeypatch):
    tx, event_id, recorder, _ = legacy
    before = await saved(tx, result_outbox)
    stage = recorder.store.stage

    async def fail_after_stage(*args, **kwargs):
        await stage(*args, **kwargs)
        raise RuntimeError("controlled storage failure")

    monkeypatch.setattr(recorder.store, "stage", fail_after_stage)
    async with tx.open() as db:
        await db.begin()
        with pytest.raises(RuntimeError, match="controlled storage failure"):
            await recorder.record_legacy_interpretation(db, await locked_result(db, event_id))
        await db.rollback()
    assert await saved(tx, result_outbox) == before and await saved(tx, outbox) == []


async def test_bad_source_identity_is_rejected_before_sealing(legacy, monkeypatch):
    tx, event_id, recorder, _ = legacy
    source = (await saved(tx, result_outbox))[0]
    async with tx.open() as db:
        await db.execute(
            update(result_outbox).values(payload={**source["payload"], "event_id": str(uuid4())})
        )
        await db.commit()
    before = await saved(tx, result_outbox)
    monkeypatch.setattr(
        "qs_ai.infrastructure.workflow_transport.state_events.prepare",
        lambda *a, **k: pytest.fail("bad identity must not seal"),
    )
    async with tx.open() as db:
        await db.begin()
        with pytest.raises(MessageConflict, match="identity invalid"):
            await recorder.record_legacy_interpretation(db, await locked_result(db, event_id))
        await db.rollback()
    assert await saved(tx, result_outbox) == before and await saved(tx, outbox) == []
