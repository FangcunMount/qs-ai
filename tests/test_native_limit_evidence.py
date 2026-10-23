import hashlib
import json

import pytest
from reliable_messaging.transaction import Binding, BindingError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from qs_ai.infrastructure.persistence.mysql.native_limit import MySQLNativeLimits
from qs_ai.infrastructure.workflow_transport.native_limit import (
    ERROR,
    native_notice_key,
    parse_native_limit_evidence,
)


def report():
    return {
        "topic": "TRANS_CHECK_MAX_TIME_TOPIC",
        "key_sha256": "1" * 64,
        "records": [
            {
                "broker_message_id": "A" * 32,
                "queue_id": 0,
                "queue_offset": 1,
                "commitlog_offset": 2,
                "index_hour": 3,
            }
        ],
        "raw_admin_sha256": "2" * 64,
        "read_only": True,
        "body_exported": False,
        "truncated_possible": False,
        "alert": "native_transaction_check_limit_reached",
        "severity": "critical",
    }


def parse(value):
    raw = json.dumps(value).encode()
    return parse_native_limit_evidence(raw, hashlib.sha256(raw).digest())


def test_exact_export_hash_is_separate_from_stable_native_index_identity():
    first = report()
    evidence = parse(first)
    first["raw_admin_sha256"] = "3" * 64
    repeated = parse(first)
    assert evidence.digest != repeated.digest and evidence.indices == repeated.indices
    first["records"][0]["broker_message_id"] = "a" * 32
    assert parse(first).indices == evidence.indices
    first["records"][0]["queue_offset"] += 1
    assert parse(first).indices != evidence.indices
    assert "11111111" not in repr(evidence)
    raw = json.dumps(report()).encode()
    with pytest.raises(ValueError, match=ERROR):
        parse_native_limit_evidence(raw + b"\n", hashlib.sha256(raw).digest())


@pytest.mark.parametrize(
    "field,value",
    [
        ("read_only", False),
        ("read_only", None),
        ("body_exported", True),
        ("severity", "warning"),
        ("topic", "wrong"),
        ("records", []),
        ("truncated_possible", True),
        ("key_sha256", "private-identity"),
    ],
)
def test_unsafe_or_empty_native_audit_is_not_a_control_signal(field, value):
    data = report()
    data[field] = value
    with pytest.raises(ValueError) as error:
        parse(data)
    assert str(error.value) == ERROR


@pytest.mark.parametrize("value", [True, None, -1, 0.0, "0", 2**63])
def test_missing_or_coerced_native_counters_are_rejected(value):
    data = report()
    data["records"][0]["queue_id"] = value
    with pytest.raises(ValueError, match=ERROR):
        parse(data)


def test_duplicate_fields_records_and_body_fields_are_rejected():
    raw = json.dumps(report()).encode()
    for altered in (
        b'{"read_only":false,' + raw[1:],
        raw + b" {}",
        raw.replace(b'"queue_id": 0', b'"queue_id":0,"queue_id":0'),
    ):
        with pytest.raises(ValueError, match=ERROR):
            parse_native_limit_evidence(altered, hashlib.sha256(altered).digest())
    data = report()
    data["records"].append(data["records"][0])
    with pytest.raises(ValueError, match=ERROR):
        parse(data)
    data = report()
    data["body"] = "private-body"
    with pytest.raises(ValueError) as error:
        parse(data)
    assert "private-body" not in str(error.value)


def test_native_set_is_order_independent_and_bounded():
    data = report()
    data["records"] = [
        dict(data["records"][0], broker_message_id=f"{index + 1:032x}", queue_offset=index)
        for index in range(64)
    ]
    data["truncated_possible"] = True
    first = parse(data)
    assert len(first.indices) == 64
    data["records"].reverse()
    assert parse(data).indices == first.indices
    data["records"].append(dict(data["records"][0], broker_message_id="f" * 32))
    with pytest.raises(ValueError, match=ERROR):
        parse(data)


def test_native_notice_uses_same_original_byte_vector_as_go():
    index = bytes.fromhex("826e83de4c78da591b9fdeb1571d7ad86be6355bb1b360430d576ca262b4a633")
    assert (
        native_notice_key("qs.mysql", "原库", "epoch-1", "m3-1", index).hex()
        == "f93835f735b87a3a4af30ceb3b0dae81b08815fb24c8705644f20e7ad5b608ff"
    )
    assert native_notice_key("qs.mysql", "A", "bc", "m3-1", index) != native_notice_key(
        "qs.mysql", "Ab", "c", "m3-1", index
    )


class NeverSession(AsyncSession):
    def __init__(self, *args, **kwargs):
        raise AssertionError("constructor/invalid budget opened a DB session")


def test_native_governance_constructor_does_not_open_session_or_accept_borrowed_handle():
    sessions = async_sessionmaker(class_=NeverSession)
    store = MySQLNativeLimits(sessions, Binding("ai.mysql", "original-primary", "epoch"), "m3-1")
    assert store.binding.owner == "ai.mysql"
    with pytest.raises(BindingError):
        MySQLNativeLimits(object(), store.binding, "m3-1")
    with pytest.raises(BindingError):
        MySQLNativeLimits(sessions, Binding("qs.mysql", "primary", "epoch"), "m3-1")


@pytest.mark.parametrize("budget", [0, -1, True, float("inf"), float("nan")])
async def test_invalid_budget_never_opens_primary_session(budget):
    store = MySQLNativeLimits(
        async_sessionmaker(class_=NeverSession), Binding("ai.mysql", "primary", "epoch"), "m3-1"
    )
    with pytest.raises(ValueError):
        await store.read_native_limit(budget)
    with pytest.raises(ValueError):
        await store.record(parse(report()), budget_seconds=budget)


def test_native_recheck_client_id_reuse_preserves_v1_physical_index_vectors():
    data = report()
    original = data["records"][0]
    copy_index = dict(original, queue_offset=2, commitlog_offset=3)
    data["records"].append(copy_index)
    evidence = parse(data)
    assert tuple(v.hex() for v in evidence.indices) == (
        "61922da276bd4e4b9eced322f30aac9224929b6614dce2205f5fed99df91a878",
        "c17eef089ffe8dcd195b09d54c8d7af26283362fc173a6226b964a4a4c0968d3",
    )
    copy_index.update(queue_offset=1, commitlog_offset=2, broker_message_id="a" * 32)
    with pytest.raises(ValueError, match=ERROR):
        parse(data)


@pytest.mark.parametrize(
    "state,at,receipt,valid",
    [
        (b"blocked", None, None, True),
        (b"resolved", "valid", b"r" * 32, True),
        (b"resolved", None, b"r" * 32, False),
        (b"resolved", "zero", b"r" * 32, False),
        (b"resolved", "valid", None, False),
        (b"resolved", "valid", bytes(32), False),
        (b"resolved", "valid", b"r", False),
        (b"blocked", "valid", b"r" * 32, False),
        (b"ignored", None, None, False),
    ],
)
def test_reviewed_native_resolution_requires_complete_receipt(state, at, receipt, valid):
    from datetime import datetime

    store = MySQLNativeLimits(
        async_sessionmaker(class_=NeverSession), Binding("ai.mysql", "primary", "epoch"), "m3-1"
    )
    index = b"i" * 32
    at = datetime(2026, 10, 24) if at == "valid" else datetime.min if at == "zero" else at
    row = dict(
        notice_key=native_notice_key("ai.mysql", "primary", "epoch", "m3-1", index),
        owner=b"ai.mysql",
        store_id=b"primary",
        writer_epoch=b"epoch",
        route_revision=b"m3-1",
        native_index_sha256=index,
        evidence_sha256=b"e" * 32,
        status=state,
        schema_version=1,
        resolved_at=at,
        resolution_receipt_sha256=receipt,
    )
    if valid:
        assert store._validate(row) == index
        row["native_index_sha256"] = b"x" * 32
    with pytest.raises(BindingError):
        store._validate(row)
