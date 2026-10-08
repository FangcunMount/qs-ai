"""Private artifact boundaries, SQL NULL and grant capabilities fail closed."""

import hashlib
import json
from pathlib import Path

import pytest

from qs_ai.maintenance.schema_refactor import backup
from qs_ai.maintenance.schema_refactor import backup_format as fmt


def test_native_records_distinguish_null_json_null_and_original_bytes():
    values = [None, b"null", b"\x00\xff\n", ' { "原始" : "😀" }\n'.encode(), b"Tail ", b""]
    assert fmt.decode(fmt.encode(values), len(values)) == values
    assert fmt.native_digest([None]) != fmt.native_digest([b"null"])
    assert fmt.native_digest([b"Tail "]) != fmt.native_digest([b"Tail"])
    with pytest.raises(ValueError):
        fmt.decode(["$not-base64"], 1)
    with pytest.raises(ValueError):
        fmt.decode(["bnVsbA=="], 2)


def test_artifact_publication_is_private_exclusive_and_complete(tmp_path):
    path = tmp_path / "backup.jsonl.gz"
    fmt.write_private(path, {"safe": "metadata"})
    assert path.stat().st_mode & 0o777 == 0o600
    with fmt.private_file(path) as stream:
        assert json.load(stream) == {"safe": "metadata"}
    before = path.read_bytes()
    with pytest.raises(ValueError, match="already exists"):
        fmt.write_private(path, {"replace": True})
    assert path.read_bytes() == before
    assert not list(tmp_path.glob(".backup-*"))


def test_interrupted_publication_does_not_create_partial_artifact(tmp_path):
    path = tmp_path / "backup.jsonl.gz"
    with pytest.raises(RuntimeError):
        with fmt.unpublished(path) as (_, stream):
            stream.write(b"original private body")
            raise RuntimeError("interrupted")
    assert not path.exists()
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize(
    "damage", ["parent_permissions", "file_permissions", "symlink", "hardlink"]
)
def test_private_file_rejects_exposed_or_redirected_artifact(tmp_path, damage):
    path = tmp_path / "backup"
    path.write_bytes(b"safe")
    path.chmod(0o600)
    if damage == "parent_permissions":
        tmp_path.chmod(0o755)
    elif damage == "file_permissions":
        path.chmod(0o644)
    elif damage == "symlink":
        original = path.with_name("original")
        path.rename(original)
        path.symlink_to(original)
    else:
        import os

        os.link(path, path.with_name("another"))
    with pytest.raises((ValueError, OSError)):
        with fmt.private_file(path):
            pytest.fail("must reject artifact")


def test_capabilities_do_not_confuse_schema_all_with_global_process():
    grants = ["GRANT ALL PRIVILEGES ON `ai`.* TO `maint`@`host`"]
    assert backup.SOURCE_PRIVILEGES <= backup._privileges(grants, "ai", False)
    assert "PROCESS" not in backup._privileges(grants, "", False, global_only=True)
    assert not backup._privileges(grants, "ai_refactor_rehearsal_a", False)
    all_schemas = ["GRANT ALL PRIVILEGES ON `%`.* TO `maint`@`host`"]
    assert "PROCESS" not in backup._privileges(all_schemas, "", False, global_only=True)


def test_random_prefix_capability_requires_actual_wildcard_not_one_example_database():
    grants = ["GRANT ALL PRIVILEGES ON `ai\\_refactor\\_stage\\_%`.* TO `maint`@`host`"]
    assert backup.STAGE_PRIVILEGES <= backup._privileges(
        grants, "ai_refactor_stage_", False, prefix=True
    )
    assert not backup._privileges(grants, "ai_refactor_stage_", True, prefix=True)
    exact = ["GRANT ALL PRIVILEGES ON `ai_refactor_stage_000000000000`.* TO `maint`@`host`"]
    assert not backup._privileges(exact, "ai_refactor_stage_", False, prefix=True)


def test_partial_revoke_overrides_a_global_grant():
    grants = [
        "GRANT ALL PRIVILEGES ON *.* TO `maint`@`host`",
        "REVOKE DROP ON `ai`.* FROM `maint`@`host`",
    ]
    assert "DROP" not in backup._privileges(grants, "ai", True)
    assert "DROP" in backup._privileges(grants, "ai_refactor_rehearsal_a", True)


def test_receipt_inspection_never_echoes_invalid_payload(tmp_path, capsys, monkeypatch):
    path = tmp_path / "body.jsonl.gz"
    fmt.write_private(path, {"body": "DO_NOT_PRINT_PRIVATE_BODY"})
    fmt.write_private(
        fmt.receipt_path(path),
        {
            "format": fmt.FORMAT,
            "backup_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "bytes": path.stat().st_size,
        },
    )
    monkeypatch.setattr("sys.argv", ["backup", "inspect", "--schema", "ai", "--path", str(path)])
    with pytest.raises(SystemExit) as error:
        backup.main()
    assert error.value.code == 1
    output = capsys.readouterr().out
    assert "DO_NOT_PRINT_PRIVATE_BODY" not in output
    assert json.loads(output)["inspect"] == "failed"


def test_restore_rejects_active_or_unknown_target_before_reading_artifact():
    class Idle:
        def in_transaction(self):
            return False

    for target in ("ai", "iam", "qs", "ai_backup_any", "other"):
        with pytest.raises(ValueError, match="isolated"):
            backup.restore(Idle(), "ai", Path("/private/not-present"), target, "0" * 64)


def test_expected_checksum_is_required_and_strict():
    for value in (None, "", "sha256:" + "a" * 64, "A" * 64):
        with pytest.raises(ValueError):
            backup._sha(value)
