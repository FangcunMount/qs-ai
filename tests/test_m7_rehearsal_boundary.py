"""The historical gRPC rehearsal cannot mutate the current MQ storage layout."""

import json
import sys

import pytest

from tests.test_deployment import load


@pytest.mark.parametrize("heads", [["0038_messaging_observations"], ["unknown"], [], None])
def test_unsupported_image_head_is_rejected_before_resources(tmp_path, monkeypatch, heads):
    script = load("scripts/ci/rehearse_m7_cutover.py")
    calls = []

    def run(*args):
        calls.append(args)
        return json.dumps(heads)

    monkeypatch.setattr(script, "run", run)
    monkeypatch.setattr(sys, "argv", ["rehearsal", "old-image", "new-image", str(tmp_path / "out")])
    with pytest.raises(RuntimeError, match="complete 0040 layout"):
        script.main()
    assert len(calls) == 1
    assert calls[0][:6] == ("docker", "run", "--rm", "--network", "none", "--read-only")
    assert not (tmp_path / "out").exists()


def test_same_layout_is_checked_offline_but_archived_grpc_flow_is_disabled(tmp_path, monkeypatch):
    script = load("scripts/ci/rehearse_m7_cutover.py")
    calls = []

    def run(*args):
        calls.append(args)
        return json.dumps([script.SCHEMA_HEAD])

    monkeypatch.setattr(script, "run", run)
    monkeypatch.setattr(sys, "argv", ["rehearsal", "old-image", "new-image", str(tmp_path / "out")])
    monkeypatch.setattr(
        script, "archived_grpc_rehearsal", lambda *_args: pytest.fail("archived flow must not run")
    )
    with pytest.raises(RuntimeError, match="mq_fault_acceptance"):
        script.main()
    assert [args[6] for args in calls] == ["old-image", "new-image"]
    assert all(args[4] == "none" for args in calls)
    assert all("alembic" not in args[7] for args in calls)
    assert not (tmp_path / "out").exists()
