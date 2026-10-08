"""Ordinary release paths cannot cross the data conversion boundary."""

import json

import pytest

from tests.test_deployment import load, setup_release, simulate_apply

OLD = "0038_messaging_observations"
NEW = "0040_module_table_names"


@pytest.mark.parametrize(
    ("current", "expected"),
    [
        ([OLD], [NEW]),
        ([NEW], [OLD]),
        (["0039_data_consolidation"], [NEW]),
        (["0039_data_consolidation"], ["0039_data_consolidation"]),
    ],
)
def test_schema_conversion_is_never_an_ordinary_deploy_or_rollback(current, expected):
    remote = load("deploy/serverA/deploy.py")
    with pytest.raises(remote.DeploymentError, match="schema_refactor"):
        remote.guard_schema_transition(current, expected)


@pytest.mark.parametrize(
    ("current", "expected"),
    [([OLD], [OLD]), ([NEW], [NEW]), (["0037_workflow_messaging"], [OLD]), ([], [NEW])],
)
def test_existing_compatible_release_and_fresh_database_paths_remain(current, expected):
    load("deploy/serverA/deploy.py").guard_schema_transition(current, expected)


@pytest.mark.parametrize(
    ("current", "expected"),
    [
        (["unknown"], ["unknown"]),
        ([OLD], []),
        ([OLD], ["unknown"]),
        ([OLD, NEW], [NEW]),
        (None, [NEW]),
    ],
)
def test_unknown_or_ambiguous_schema_contract_fails_closed(current, expected):
    remote = load("deploy/serverA/deploy.py")
    with pytest.raises(remote.DeploymentError, match="contract unavailable"):
        remote.guard_schema_transition(current, expected)


def test_cross_schema_apply_stops_before_migration_and_service_replacement(tmp_path, monkeypatch):
    remote = load("deploy/serverA/deploy.py")
    release, manifest = setup_release(remote, tmp_path, monkeypatch)
    calls = simulate_apply(remote, monkeypatch, manifest)
    monkeypatch.setattr(remote, "probe", lambda *_args: {"current": [OLD], "expected": [NEW]})
    monkeypatch.setattr(remote, "stop_release", lambda _path: pytest.fail("must preserve service"))
    with pytest.raises(remote.DeploymentError, match="schema_refactor"):
        remote.apply(release, {"current": "b" * 40 + "-1-1"})
    assert "migration" not in calls
    assert not (tmp_path / "state.json").exists()


def test_cross_schema_rollback_checks_target_contract_without_old_database_probe(
    tmp_path, monkeypatch
):
    remote = load("deploy/serverA/deploy.py")
    monkeypatch.setattr(remote, "ROOT", tmp_path)
    state = {"current": "a" * 40 + "-1-1", "previous": "b" * 40 + "-1-1"}
    monkeypatch.setattr(remote, "release_image_id", lambda *_a, **_kw: "sha256:" + "a" * 64)
    calls = []

    def probe(path, _require_head):
        calls.append(path.name)
        assert path.name == state["current"]
        return {"current": [NEW], "expected": [NEW]}

    monkeypatch.setattr(remote, "probe", probe)
    monkeypatch.setattr(remote, "expected_heads", lambda _path: [OLD])
    monkeypatch.setattr(remote, "stop_release", lambda _path: pytest.fail("must preserve service"))
    with pytest.raises(remote.DeploymentError, match="schema_refactor"):
        remote.restore(state)
    assert calls == [state["current"]]
    assert not (tmp_path / "state.json").exists()


def test_target_head_read_is_database_independent(tmp_path, monkeypatch):
    remote = load("deploy/serverA/deploy.py")
    calls = []

    def run(phase, args):
        calls.append((phase, args))
        return "qs-ai" if phase == "compose services" else json.dumps([OLD])

    monkeypatch.setattr(remote, "run", run)
    assert remote.expected_heads(tmp_path) == [OLD]
    assert calls[-1][0] == "image schema contract"
    assert "database_check" not in calls[-1][1][-1]
    assert "create_engine" not in calls[-1][1][-1]
