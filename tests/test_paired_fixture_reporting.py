"""One report owner across separately registered copies of a session fixture."""

import json
import logging
from importlib import metadata
from types import SimpleNamespace

import pytest
from rocketmq.v5.log import log_config

import tests.integration.rocketmq_paired_host as paired


@pytest.mark.parametrize("faults", [False, True])
def test_paired_report_and_private_logger_have_one_session_owner(
    tmp_path, monkeypatch, capsys, faults
):
    request, config, path, logger = context(tmp_path, monkeypatch, faults)
    first = paired.required_pair.__wrapped__(request)
    second = paired.required_pair.__wrapped__(request)
    try:
        original = next(first)
        assert next(second) is original
        assert len(logger.handlers) == 1
        second.close()
        assert len(logger.handlers) == 1
    finally:
        second.close()
        first.close()
    marker = "M3_PAIRED_BASELINE_RECEIPT " if faults else "M3_PAIRED_HOST_RECEIPT "
    lines = [line for line in capsys.readouterr().out.splitlines() if marker in line]
    assert len(lines) == 1
    assert json.loads(lines[0].split(marker, 1)[1])["failed"] == 0
    assert json.loads(lines[0].split(marker, 1)[1])["sdk_go"] == config["sdk_go_version"]
    assert not logger.handlers


def test_shared_paired_fixture_refuses_configuration_replacement(tmp_path, monkeypatch, capsys):
    request, config, path, logger = context(tmp_path, monkeypatch, True)
    first = paired.required_pair.__wrapped__(request)
    second = paired.required_pair.__wrapped__(request)
    try:
        next(first)
        config["project"] = "rocketmq-m2-different"
        path.write_text(json.dumps(config))
        with pytest.raises(AssertionError, match="Shared paired session configuration changed"):
            next(second)
        assert len(logger.handlers) == 1
    finally:
        second.close()
        first.close()
    assert capsys.readouterr().out.count("M3_PAIRED_BASELINE_RECEIPT ") == 1


def test_paired_fixture_refuses_module_version_different_from_frozen_profile(tmp_path, monkeypatch):
    request, config, path, _ = context(tmp_path, monkeypatch, False)
    config["qs_sdk_module"]["Version"] = "v0.5.0-rocketmq-tx.2"
    path.write_text(json.dumps(config))
    fixture = paired.required_pair.__wrapped__(request)
    with pytest.raises(AssertionError):
        next(fixture)


def context(tmp_path, monkeypatch, faults):
    private = tmp_path / "private"
    private.mkdir(mode=0o700)
    config = dict(
        sdk_python_version=metadata.version("fangcun-reliable-messaging"),
        endpoint="127.0.0.1:1",
        project="rocketmq-m2-fixture-report",
        sdk_go_version="v0.5.0-rocketmq-tx.4",
        qs_sdk_module=dict(Version="v0.5.0-rocketmq-tx.4"),
        private_directory=str(private),
        client_log=str(private / "client.log"),
        paired_profile="full",
        paired_faults=faults,
    )
    path = private / "config.json"
    path.write_text(json.dumps(config))
    path.chmod(0o600)
    monkeypatch.setenv("QS_AI_M3_BROKER_CONFIG", str(path))
    logger = logging.Logger("paired-report-unit")
    monkeypatch.setattr(log_config, "logger", logger)
    request = SimpleNamespace(
        session=SimpleNamespace(testsfailed=0),
        config=SimpleNamespace(
            stash=pytest.Stash(),
            pluginmanager=SimpleNamespace(get_plugin=lambda _: SimpleNamespace(stats={})),
        ),
    )
    return request, config, path, logger
