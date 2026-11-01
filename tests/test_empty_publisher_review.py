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


def write_json(path, value):
    path.write_text(json.dumps(value))
    path.chmod(0o600)
    return hashlib.sha256(path.read_bytes()).hexdigest()


def reviewed(tmp_path, monkeypatch):
    """Explicit synthetic unit fixtures; never production protection receipts."""
    monkeypatch.setenv("QS_AI_RELEASE_SHA", "a" * 40)
    candidate = {
        "schema": "ai-empty-publisher-review/v2",
        "source_sha": "a" * 40,
        "operation_id": "test-window",
        "mysql_identity_sha256": "b" * 64,
        "table_ddl_sha256": "",
        "initial_head": "0043_native_transaction_limit",
        "target_head": "0044_empty_publisher",
        "writers_excluded": True,
        "data_owner": "shujie",
        "recovery_owner": "shujie",
        "expires_at": (datetime.now(UTC) + timedelta(minutes=30)).isoformat(),
        "protection_manifest": str(tmp_path / "protection.json"),
        "restore_receipt": str(tmp_path / "restore.json"),
        "protection_sha256": "",
        "restore_sha256": "",
    }
    ddl = "CREATE TABLE messaging_outbox (id INT PRIMARY KEY) ENGINE=InnoDB"
    frozen = {
        "schema": "ai-publisher-snapshot/v1",
        "state": "43_intact",
        "head": "0043_native_transaction_limit",
        "mysql_identity_sha256": "b" * 64,
        "target": {
            "present": True,
            "rows": 0,
            "ddl": ddl,
            "ddl_sha256": hashlib.sha256(ddl.encode()).hexdigest(),
        },
        "ledger": {"heads": ["0043_native_transaction_limit"]},
        "retained": {"fixture_only": {}},
        "captured_at": datetime.now(UTC).isoformat(),
    }
    path = tmp_path / "review.json"
    bind_review(path, candidate, frozen)
    return path, candidate


def bind_review(path, candidate, frozen):
    """Native tests pass actual isolated snapshots; unit tests pass marked fixtures."""
    candidate.update(
        mysql_identity_sha256=frozen["mysql_identity_sha256"],
        table_ddl_sha256=frozen["target"]["ddl_sha256"],
    )
    now = datetime.now(UTC)
    writer = {
        "schema": "ai-publisher-writer-exclusion/v1",
        "source_sha": candidate["source_sha"],
        "operation_id": candidate["operation_id"],
        "mysql_identity_sha256": frozen["mysql_identity_sha256"],
        "writers_excluded": True,
        "all_writer_scopes_reviewed": True,
        "broker_pending_preserved": True,
        "business_reopened": False,
        "running_writers": 0,
        "unfinished_work": 0,
        "data_owner": "shujie",
        "recovery_owner": "shujie",
        "inventory_sha256": "d" * 64,
        "candidate_image_digest": "e" * 64,
        "stopped_container_ids": ["f" * 64],
        "recorded_at": (now - timedelta(minutes=1)).isoformat(),
        "expires_at": (now + timedelta(minutes=30)).isoformat(),
    }
    writer_path = path.parent / "writers.json"
    writer_sha = write_json(writer_path, writer)
    protection = {
        "schema": "ai-publisher-protection/v1",
        "source_sha": candidate["source_sha"],
        "operation_id": candidate["operation_id"],
        "snapshot": frozen,
        "frozen": True,
        "consistency_basis": "snapshot-plus-owner-writer-attestation",
        "writer_receipt": str(writer_path),
        "writer_sha256": writer_sha,
    }
    restore = {
        "schema": "ai-publisher-restore-qualification/v1",
        "source_sha": candidate["source_sha"],
        "table_ddl_sha256": candidate["table_ddl_sha256"],
        "isolated": True,
        "restored_rows": 0,
        "ddl_roundtrip_equal": True,
        "retained_unchanged": True,
        "head43_to44_passed": True,
        "unknown_ddl_convergence_passed": True,
        "evidence_sha256": "d" * 64,
        "test_fixture_only": True,
    }
    candidate["protection_sha256"] = write_json(path.parent / "protection.json", protection)
    candidate["restore_sha256"] = write_json(path.parent / "restore.json", restore)
    return write_json(path, candidate)


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
        _review(
            str(path),
            hashlib.sha256(path.read_bytes()).hexdigest(),
            "b" * 64,
            candidate["table_ddl_sha256"],
        )


def test_review_binds_protection_bytes_permissions_and_does_not_consume(tmp_path, monkeypatch):
    path, candidate = reviewed(tmp_path, monkeypatch)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    _review(str(path), digest, "b" * 64, candidate["table_ddl_sha256"])
    assert not path.with_suffix(".json.consumed").exists()
    protection = tmp_path / "protection.json"
    protection.write_bytes(b"changed")
    with pytest.raises(ValueError):
        _review(str(path), digest, "b" * 64, candidate["table_ddl_sha256"])
    protection.chmod(0o644)
    with pytest.raises(ValueError):
        _review(str(path), digest, "b" * 64, candidate["table_ddl_sha256"])


@pytest.mark.parametrize("mutation", ["live", "no_writers", "unqualified_restore", "v1"])
def test_review_rejects_semantically_unqualified_material(tmp_path, monkeypatch, mutation):
    path, candidate = reviewed(tmp_path, monkeypatch)
    if mutation == "v1":
        candidate["schema"] = "ai-empty-publisher-review/v1"
    else:
        name = "restore" if mutation == "unqualified_restore" else "protection"
        material = tmp_path / f"{name}.json"
        value = json.loads(material.read_text())
        if mutation == "live":
            value["frozen"] = False
        elif mutation == "no_writers":
            writers = tmp_path / "writers.json"
            writer = json.loads(writers.read_text())
            writer["running_writers"] = 1
            value["writer_sha256"] = write_json(writers, writer)
        else:
            value["unknown_ddl_convergence_passed"] = False
        candidate[f"{name}_sha256"] = write_json(material, value)
    sha = write_json(path, candidate)
    with pytest.raises(ValueError):
        _review(str(path), sha, "b" * 64, candidate["table_ddl_sha256"])


def test_fingerprint_preserves_null_empty_binary_and_field_boundaries():
    from qs_ai.maintenance.publisher_material import row_digest

    values = [[None], [""], ["00"], ["FF"], ["00", "FF"], ["00FF"], ["", None]]
    assert len({row_digest(row) for row in values}) == len(values)


def test_durable_private_consumption_never_overwrites_or_follows_link(tmp_path):
    from qs_ai.maintenance.publisher_material import write_private

    path = tmp_path / "approval.consumed"
    write_private(str(path), b"approved\n")
    with pytest.raises(FileExistsError):
        write_private(str(path), b"different\n")
    alias = tmp_path / "alias"
    alias.symlink_to(path)
    with pytest.raises(FileExistsError):
        write_private(str(alias), b"different\n")
    assert path.read_bytes() == b"approved\n"
    tmp_path.chmod(0o755)
    with pytest.raises(ValueError, match="0700"):
        write_private(str(tmp_path / "new"), b"not written")


def test_capture_never_promotes_live_readonly_to_frozen(tmp_path, monkeypatch):
    from qs_ai.maintenance.publisher_window import protection

    _, candidate = reviewed(tmp_path, monkeypatch)
    frozen = json.loads((tmp_path / "protection.json").read_text())["snapshot"]
    result = protection(frozen, source="a" * 40, operation="live")
    assert result["frozen"] is False and result["execution_approval"] is False
    assert result["retained_fingerprints_are_backup"] is False
