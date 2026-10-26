"""The old result-transfer command cannot parse an apply request or open resources."""

import argparse
import sys
from unittest.mock import Mock

import pytest

from qs_ai.maintenance import messaging_handoff as cli


@pytest.mark.parametrize(
    "args",
    [[], ["--action", "dry-run"], ["--action", "apply", "--manifest", "private-missing-plan"]],
)
def test_retired_result_transfer_refuses_before_arguments_database_or_keys(
    monkeypatch, capsys, args
):
    parse = Mock(side_effect=AssertionError("Retired command parsed external input"))
    monkeypatch.setattr(argparse, "ArgumentParser", parse)
    monkeypatch.setattr(sys, "argv", ["messaging_handoff", *args])
    with pytest.raises(SystemExit) as error:
        cli.main()
    assert error.value.code == 2
    assert "Legacy message transfer is retired" in capsys.readouterr().err
    parse.assert_not_called()
    assert not any(hasattr(cli, n) for n in ("Database", "ResultHandoff", "StateEventRecorder"))
