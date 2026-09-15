import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta

import pytest

from scripts.retirement import core
from scripts.retirement.policy import COLLECTIONS, EVENTS, old_event


class Store:
    name = "test"
    identity = "server/database"

    def __init__(self):
        self.data = {"legacy": {"rows": ["private report"], "count": 1, "sha256": "before"}}
        self.restored = False
        self.applied = False

    def snapshot(self):
        return deepcopy(self.data)

    def verify_restore(self, snapshot):
        assert snapshot == self.data
        self.restored = True

    def apply(self, snapshot):
        assert self.restored
        self.applied = True

    def verify_deleted(self, snapshot):
        pass


@pytest.fixture
def prepared(tmp_path):
    store = Store()
    directory = tmp_path / "private"
    sha = core.plan(directory, [store])
    return directory, store, sha


def evidence():
    return dict(
        acceptance_verified=True,
        writers_stopped=True,
        old_events_drained=True,
        qs_sha="test-qs",
        ai_sha="test-ai",
        evidence_reference="isolated-test-only",
    )


def test_fixed_whitelist_and_payload_ownership():
    assert len(COLLECTIONS) == 9
    assert len(EVENTS) == 6
    assert old_event({"event_type": EVENTS[0]})
    assert old_event({"payload_json": '{"type":"' + EVENTS[-1] + '"}'}, payload=True)
    for payload in ("bad", "[]", '{"type":"other"}', '{"data":{"type":"' + EVENTS[0] + '"}}'):
        assert not old_event({"payload_json": payload}, payload=True)
    assert not old_event({"event_type": EVENTS[0] + ".future"})


@pytest.mark.parametrize("event", EVENTS)
def test_persisted_domain_event_envelope_is_selected(event):
    assert old_event({"payload_json": json.dumps({"eventType": event})}, payload=True)
    assert old_event(
        {"payload_json": json.dumps({"eventType": event, "type": event})}, payload=True
    )


@pytest.mark.parametrize(
    "envelope",
    [
        {"eventType": "interpretation.report.generated"},
        {"eventType": "evaluation.requested"},
        {"eventType": "answersheet.submitted"},
        {"eventType": EVENTS[0], "type": "interpretation.report.generated"},
        {"eventType": "interpretation.report.generated", "type": EVENTS[0]},
        {"eventType": EVENTS[0], "type": EVENTS[1]},
        {"eventType": [EVENTS[0]]},
        {"data": {"eventType": EVENTS[0]}},
    ],
)
def test_shared_or_ambiguous_domain_events_are_preserved(envelope):
    assert not old_event({"payload_json": json.dumps(envelope)}, payload=True)


def test_private_backup_must_be_restored_before_apply(prepared):
    directory, store, sha = prepared
    assert "private report" not in (directory / "plan.json").read_text()
    receipt = core.backup(directory, [store], sha)
    assert store.restored
    assert datetime.fromisoformat(receipt["expires_at"]) > datetime.now(UTC) + timedelta(days=6)
    assert directory.stat().st_mode & 0o777 == 0o700
    assert all(path.stat().st_mode & 0o777 == 0o600 for path in directory.iterdir())
    assert core.apply(directory, [store], sha, evidence())["status"] == "completed"
    assert store.applied
    with pytest.raises(core.Stop, match="already attempted"):
        core.apply(directory, [store], sha, evidence())


@pytest.mark.parametrize("stage", ["backup", "apply"])
def test_content_or_reference_drift_aborts_before_mutation(prepared, stage):
    directory, store, sha = prepared
    if stage == "apply":
        core.backup(directory, [store], sha)
    store.data["legacy"]["references"] = ["new_reference"]
    with pytest.raises(core.Stop, match="changed"):
        if stage == "apply":
            core.apply(directory, [store], sha, evidence())
        else:
            core.backup(directory, [store], sha)
    assert not store.applied


@pytest.mark.parametrize("damage", ["archive", "expired", "target", "sha", "evidence"])
def test_damaged_or_unapproved_plan_cannot_apply(prepared, damage):
    directory, store, sha = prepared
    core.backup(directory, [store], sha)
    proof = evidence()
    if damage == "archive":
        core.write(directory / "backup.json", {})
    elif damage == "expired":
        path = directory / "restore-verification.json"
        value = core.read(path)
        value["expires_at"] = (datetime.now(UTC) - timedelta(days=1)).isoformat()
        core.write(path, value)
    elif damage == "target":
        store.identity = "another-server"
    elif damage == "sha":
        sha = "different"
    else:
        proof["old_events_drained"] = False
    with pytest.raises((core.Stop, KeyError)):
        core.apply(directory, [store], sha, proof)
    assert not store.applied


def test_failed_restore_does_not_issue_receipt(prepared, monkeypatch):
    directory, store, sha = prepared

    def fail(snapshot):
        raise core.Stop("injected restore failure")

    monkeypatch.setattr(store, "verify_restore", fail)
    with pytest.raises(core.Stop):
        core.backup(directory, [store], sha)
    assert not (directory / "restore-verification.json").exists()
    assert (directory / "backup.json").exists()


def test_backup_inside_worktree_refused(tmp_path):
    (tmp_path / ".git").write_text("worktree marker")
    with pytest.raises(core.Stop, match="outside a Git"):
        core.plan(tmp_path / "backup", [Store()])


def test_expired_retention_does_not_prevent_recovery(prepared):
    directory, store, sha = prepared
    core.backup(directory, [store], sha)
    path = directory / "restore-verification.json"
    value = core.read(path)
    value["expires_at"] = (datetime.now(UTC) - timedelta(days=1)).isoformat()
    core.write(path, value)
    core.checked_backup(directory, [store], sha, require_current=False)
    with pytest.raises(core.Stop, match="expired"):
        core.checked_backup(directory, [store], sha)
