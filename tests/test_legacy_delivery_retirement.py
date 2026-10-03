"""The obsolete one-shot entry must fail before loading settings or touching storage."""

import subprocess
import sys

import pytest


@pytest.mark.parametrize("arguments", [[], ["deliver"], ["deliver", "--continuous"], ["serve"]])
def test_retired_entry_cannot_revive_delivery(arguments):
    result = subprocess.run(
        [sys.executable, "-m", "qs_ai.bootstrap.integration", *arguments],
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode != 0
    assert result.stdout == ""
    assert result.stderr.strip() == (
        "Legacy gRPC delivery is retired; use bootstrap.server with MQ enabled"
    )
