"""Private approval bytes and history-reader version boundaries; no fake empty state."""

import hashlib
import json
from datetime import UTC, datetime, timedelta

import pytest

from qs_ai.maintenance.empty_publisher_retirement import _review
from qs_ai.maintenance.schema_layout import (
    handoff_tables,
    require_audit_head,
    require_handoff_head,
    require_known_head,
    retained_physical_table,
)


def reviewed(tmp_path, monkeypatch):
    monkeypatch.setenv("QS_AI_RELEASE_SHA", "a" * 40)
    protection, restore = tmp_path / "protection.json", tmp_path / "restore.json"
    for p in (protection, restore):
        p.write_bytes(b'{"test_only_claim":true}')
        p.chmod(0o600)
    candidate = {
        "schema": "ai-empty-publisher-review/v1",
        "source_sha": "a" * 40,
        "operation_id": "test-window",
        "mysql_identity_sha256": "b" * 64,
        "table_ddl_sha256": "c" * 64,
        "initial_head": "0043_native_transaction_limit",
        "target_head": "0044_empty_publisher",
        "writers_excluded": True,
        "data_owner": "fixture",
        "recovery_owner": "fixture",
        "expires_at": (datetime.now(UTC) + timedelta(minutes=30)).isoformat(),
        "protection_manifest": str(protection),
        "restore_receipt": str(restore),
        "protection_sha256": hashlib.sha256(protection.read_bytes()).hexdigest(),
        "restore_sha256": hashlib.sha256(restore.read_bytes()).hexdigest(),
    }
    path = tmp_path / "review.json"
    path.write_text(json.dumps(candidate))
    path.chmod(0o600)
    return path, candidate


def test_new_head_is_audit_only_and_never_expands_legacy_exchange():
    head = "0044_empty_publisher"
    assert require_audit_head([head]) == head
    assert retained_physical_table(head, "model_calls") == "execution_model_calls"
    for function in (require_handoff_head, require_known_head):
        with pytest.raises(ValueError):
            function([head])
    with pytest.raises(ValueError):
        handoff_tables(head)


@pytest.mark.parametrize(
    "field,value",
    [
        ("source_sha", "d" * 40),
        ("mysql_identity_sha256", "d" * 64),
        ("table_ddl_sha256", "d" * 64),
        ("initial_head", "0042_transaction_event_receipts"),
        ("target_head", "0040_module_table_names"),
        ("writers_excluded", False),
        ("expires_at", "2020-01-01T00:00:00+00:00"),
        ("recovery_owner", ""),
        ("data_owner", ""),
        ("force", True),
    ],
)
def test_approval_refuses_exact_window_drift(tmp_path, monkeypatch, field, value):
    path, candidate = reviewed(tmp_path, monkeypatch)
    candidate[field] = value
    path.write_text(json.dumps(candidate))
    with pytest.raises(ValueError):
        _review(str(path), hashlib.sha256(path.read_bytes()).hexdigest(), "b" * 64, "c" * 64)


def test_review_binds_protection_bytes_permissions_and_does_not_consume(tmp_path, monkeypatch):
    path, candidate = reviewed(tmp_path, monkeypatch)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    _review(str(path), digest, "b" * 64, "c" * 64)
    assert not path.with_suffix(".json.consumed").exists()
    protection = tmp_path / "protection.json"
    protection.write_bytes(b"changed")
    with pytest.raises(ValueError):
        _review(str(path), digest, "b" * 64, "c" * 64)
    protection.write_bytes(b'{"test_only_claim":true}')
    protection.chmod(0o644)
    with pytest.raises(ValueError):
        _review(str(path), digest, "b" * 64, "c" * 64)
