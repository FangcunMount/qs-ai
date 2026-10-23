"""Pure validation and original private-plan boundaries; no service dependencies."""

import hashlib
import json
from datetime import UTC, datetime, timedelta

import pytest

from qs_ai.maintenance.transaction_binding import (
    HEAD,
    SCHEMA,
    identity,
    read_plan,
    validate_plan,
    write_plan,
)


def plan():
    return {
        "schema": SCHEMA,
        "source_sha": "a" * 40,
        "operation_id": "reviewed-op",
        "owner": "ai.mysql",
        "store_id": "original-primary",
        "writer_epoch": "m3-1",
        "route_revision": "m3-1",
        "schema_head": HEAD,
        "expires_at": (datetime.now(UTC) + timedelta(minutes=30)).isoformat(),
        "database": "owned_test",
        "identity_sha256": "b" * 64,
        "ddl_sha256": "c" * 64,
        "migration_resources_sha256": "d" * 64,
    }


@pytest.mark.parametrize(
    "field,value",
    [
        ("owner", "qs.mysql"),
        ("source_sha", "b" * 40),
        ("route_revision", "m3-2"),
        ("schema_head", "0040_module_table_names"),
        ("store_id", " "),
        ("writer_epoch", "x" * 129),
        ("identity_sha256", "0" * 64),
        ("ddl_sha256", "bad"),
        ("operation_id", "bad\x00id"),
        ("expires_at", "expired"),
        ("expires_at", "far_future"),
    ],
)
def test_exact_initial_plan_refuses_drift(field, value):
    candidate = plan()
    if field == "expires_at":
        delta = timedelta(seconds=-1) if value == "expired" else timedelta(hours=3)
        value = (datetime.now(UTC) + delta).isoformat()
    candidate[field] = value
    with pytest.raises(ValueError):
        validate_plan(candidate, "a" * 40, datetime.now(UTC))


def test_plan_rejects_unknown_fields_and_byte_length():
    candidate = plan()
    candidate["force"] = True
    with pytest.raises(ValueError):
        validate_plan(candidate, "a" * 40, datetime.now(UTC))
    with pytest.raises(ValueError):
        identity("界" * 43)
    assert identity("界" * 42) == "界" * 42


def test_private_plan_is_raw_digest_bound_exclusive_and_no_symlink(tmp_path):
    path = tmp_path / "review.json"
    candidate = plan()
    approved = write_plan(str(path), candidate)
    assert path.stat().st_mode & 0o777 == 0o600
    assert read_plan(str(path), approved, "a" * 40)[0] == candidate
    with pytest.raises(FileExistsError):
        write_plan(str(path), candidate)
    with pytest.raises(ValueError):
        read_plan(str(path), "f" * 64, "a" * 40)
    path.chmod(0o644)
    with pytest.raises(ValueError):
        read_plan(str(path), approved, "a" * 40)
    path.chmod(0o600)
    link = tmp_path / "link.json"
    link.symlink_to(path)
    with pytest.raises(OSError):
        read_plan(str(link), approved, "a" * 40)
    raw = json.dumps(candidate).encode()[:-1] + b',"owner":"ai.mysql"}'
    path.write_bytes(raw)
    with pytest.raises(ValueError, match="Duplicate"):
        read_plan(str(path), hashlib.sha256(raw).hexdigest(), "a" * 40)
    path.write_bytes(b" " * 16385)
    with pytest.raises(ValueError):
        read_plan(str(path), hashlib.sha256(path.read_bytes()).hexdigest(), "a" * 40)
