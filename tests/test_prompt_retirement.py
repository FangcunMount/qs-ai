import copy
import json
from datetime import UTC, datetime, timedelta

import pytest

from qs_ai.maintenance.prompt_retirement import service, snapshot
from qs_ai.maintenance.prompt_retirement.__main__ import parser
from qs_ai.maintenance.schema_refactor import backup_format as fmt
from qs_ai.maintenance.schema_refactor.layouts import NEW_HEAD, OLD_HEAD

REVISION = "1" * 40
IMAGE = "sha256:" + "2" * 64


def value():
    result = {
        "format": service.FORMAT,
        "scope": service.SCOPE,
        "execution": service.execution(REVISION, IMAGE),
        "inventory": {
            "source": {"source_schema": "ai", "head": NEW_HEAD, "source_server_uuid": "uuid"},
            "candidates": [],
            "retained": [],
        },
    }
    result["plan_sha256"] = fmt.digest(result)
    return result


def test_private_artifacts_and_plan_bindings(tmp_path):
    path = tmp_path / "plan.json"
    fmt.write_private(path, value())
    assert service.load_plan(path, "ai", REVISION, IMAGE)["scope"] == service.SCOPE
    with pytest.raises(ValueError, match="binding"):
        service.load_plan(path, "qs", REVISION, IMAGE)
    with pytest.raises(ValueError, match="binding"):
        service.load_plan(path, "ai", "3" * 40, IMAGE)
    with pytest.raises(ValueError, match="binding"):
        service.load_plan(path, "ai", REVISION, "sha256:" + "3" * 64)
    path.chmod(0o644)
    with pytest.raises(ValueError, match="private"):
        service.load_plan(path, "ai", REVISION, IMAGE)


@pytest.mark.parametrize(
    "legacy",
    [
        {"format_version": 2},
        {"format": "qs-ai-native-backup/v1"},
        {"format": service.RECEIPT_FORMAT},
        {"format": service.JOURNAL_FORMAT},
    ],
)
def test_old_or_wrong_evidence_cannot_be_a_plan(tmp_path, legacy):
    path = tmp_path / "wrong.json"
    fmt.write_private(path, legacy)
    with pytest.raises(ValueError, match="interchanged"):
        service.load_plan(path, "ai", REVISION, IMAGE)


def test_scope_tamper_is_rejected_even_after_resigning(tmp_path):
    record = copy.deepcopy(value())
    record["scope"]["versions"].append("v6")
    record["plan_sha256"] = fmt.digest({k: v for k, v in record.items() if k != "plan_sha256"})
    path = tmp_path / "changed.json"
    fmt.write_private(path, record)
    with pytest.raises(ValueError, match="binding"):
        service.load_plan(path, "ai", REVISION, IMAGE)


def test_backup_receipt_expiry_and_cross_plan_refusal(tmp_path):
    record = value()
    receipt = {
        "format": service.RECEIPT_FORMAT,
        "plan_sha256": record["plan_sha256"],
        "source": record["inventory"]["source"],
        "execution": record["execution"],
        "restore_verified": True,
        "expires_at": (datetime.now(UTC) - timedelta(days=1)).isoformat(),
    }
    path = tmp_path / "receipt.json"
    fmt.write_private(path, receipt)
    with pytest.raises(ValueError, match="expired"):
        service.check_receipt(record, tmp_path / "not-opened.gz", path)


class DeleteConnection:
    def __init__(self, rowcount=1):
        self.rowcount = rowcount
        self.calls = []

    def execute(self, sql, values):
        self.calls.append((str(sql), values))
        return self


def test_delete_is_parameterized_and_complete():
    head = NEW_HEAD
    row = {
        "asset_kind": "prompt",
        "owner_organization_id": 0,
        "template_id": service.policy.TEMPLATE_ID,
        "version": "v1",
        "row_id": 9,
        "fingerprint": "sha256:" + "f" * 64,
        "package_sha256": "a" * 64,
        "body_sha256": "b" * 64,
    }
    conn = DeleteConnection()
    service._delete(conn, "ai", head, row)
    sql, bound = conn.calls[0]
    assert "SHA2(" in sql and ":body_sha256" in sql
    assert ":fingerprint" in sql and ":package_sha256" in sql and ":version" in sql
    assert row["template_id"] not in sql and bound is row
    if head == NEW_HEAD:
        assert "asset_kind='prompt'" in sql and "owner_organization_id=0" in sql
        assert "asset_row_id=:row_id" in sql and ":template_id" in sql
    with pytest.raises(ValueError, match="exactly one"):
        service._delete(DeleteConnection(0), "ai", head, row)
    with pytest.raises(ValueError, match="whitelist"):
        service._delete(conn, "ai", head, {**row, "version": "v6"})
    with pytest.raises(ValueError, match="whitelist"):
        service._delete(conn, "ai", head, {**row, "asset_kind": "semantic_prompt"})


@pytest.mark.parametrize("head", [OLD_HEAD, "0039_data_consolidation"])
def test_old_or_intermediate_plan_is_rejected_even_if_resigned(tmp_path, head):
    record = value()
    record["inventory"]["source"]["head"] = head
    record["plan_sha256"] = fmt.digest({k: v for k, v in record.items() if k != "plan_sha256"})
    path = tmp_path / "wrong-layout.json"
    fmt.write_private(path, record)
    with pytest.raises(ValueError, match="0040"):
        service.load_plan(path, "ai", REVISION, IMAGE)
    row = {
        "asset_kind": "prompt",
        "owner_organization_id": 0,
        "template_id": service.policy.TEMPLATE_ID,
        "version": "v1",
    }
    with pytest.raises(ValueError, match="0040"):
        service._delete(DeleteConnection(), "ai", head, row)


def test_native_null_and_json_null_remain_distinct():
    assert fmt.native_digest([None]) != fmt.native_digest([b"null"])
    assert snapshot.structure({"captured_at": "now", "sql_mode": "strict"}) == {
        "sql_mode": "strict"
    }


def test_cli_has_no_generic_asset_scope_or_live_restore():
    args = parser().parse_args(
        ["plan", "--plan", "/private/plan.json", "--revision", REVISION, "--image-id", IMAGE]
    )
    assert args.schema == "ai"
    with pytest.raises(SystemExit):
        parser().parse_args(
            ["plan", "--plan", "p", "--revision", REVISION, "--image-id", IMAGE, "--kind", "route"]
        )


def test_apply_requires_the_original_stop_clock():
    base = [
        "apply",
        "--plan",
        "p",
        "--backup",
        "b",
        "--receipt",
        "r",
        "--journal",
        "j",
        "--revision",
        REVISION,
        "--image-id",
        IMAGE,
        "--writers-stopped",
    ]
    with pytest.raises(SystemExit):
        parser().parse_args(base)
    stopped_at = "2026-10-08T00:00:00+00:00"
    args = parser().parse_args([*base, "--stopped-at", stopped_at])
    assert args.stopped_at == stopped_at and args.writers_stopped


def test_expired_new_apply_never_opens_a_transaction(tmp_path, monkeypatch):
    record = value()
    record["inventory"]["candidates"] = [{"version": "v1"}]
    monkeypatch.setattr(service, "load_plan", lambda *args: record)
    monkeypatch.setattr(service, "check_receipt", lambda *args: {})

    class Idle:
        def in_transaction(self):
            return False

    with pytest.raises(TimeoutError, match="deadline"):
        service.apply(
            Idle(),
            "ai",
            tmp_path / "p",
            tmp_path / "b",
            tmp_path / "r",
            tmp_path / "j",
            **{"revision": REVISION, "image_id": IMAGE},
            writers_stopped=True,
            stopped_at="2000-01-01T00:00:00+00:00",
        )
    with pytest.raises(SystemExit):
        parser().parse_args(["apply", "--plan", "p", "--revision", REVISION, "--image-id", IMAGE])


def test_application_journal_is_private_and_preserves_plan_binding(tmp_path):
    path = tmp_path / "application.json"
    original = {"format": service.JOURNAL_FORMAT, "phase": "pending", "plan_sha256": "a" * 64}
    fmt.write_private(path, original)
    service._save_journal(path, {**original, "phase": "commit_pending"})
    assert json.loads(path.read_text())["phase"] == "commit_pending"
    assert path.stat().st_mode & 0o077 == 0
    with pytest.raises(ValueError, match="immutable binding"):
        service._save_journal(path, {**original, "plan_sha256": "b" * 64})


def test_unknown_commit_classification_is_not_reported_verified():
    result = service._classified(value(), "verify", "unknown")
    assert result["prompt_retirement"] == "failed" and result["verified"] is False
    known = service._classified(value(), "verify", "not_applied")
    assert known["prompt_retirement"] == "ok" and known["application_repeated"] is False


def test_candidate_generated_profile_key_must_be_native_sql_null(monkeypatch):
    table = "governance_asset_versions"
    columns = ["asset_kind", "asset_row_id", "owner_organization_id", "asset_id", "version"]
    header = {"tables": {table: {"columns": columns, "generated": ["profile_id_key"]}}}
    monkeypatch.setattr(snapshot.native, "_templates", lambda *args: {table: {}})
    monkeypatch.setattr(
        snapshot.native,
        "_native_rows",
        lambda *args: iter(
            [[b"prompt", b"9", b"0", service.policy.TEMPLATE_ID.encode(), b"v1", b"not-null"]]
        ),
    )
    row = {"template_id": service.policy.TEMPLATE_ID, "version": "v1", "row_id": 9}

    class Connection:
        info = {}

    with pytest.raises(ValueError, match="SQL NULL"):
        snapshot.physical(Connection(), "ai", NEW_HEAD, header, [row])
