"""An obsolete image probe cannot create dependencies before refusing the profile."""

import os
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.parametrize("arguments", [[], ["old-image:fixture", "extra"]])
def test_retired_nsq_image_probe_rejects_before_external_commands(tmp_path, arguments):
    root = Path(__file__).resolve().parents[1]
    log = tmp_path / "external-calls"
    for name in ("docker", "openssl", "git"):
        p = tmp_path / name
        p.write_text('#!/bin/sh\nprintf called >> "$CALL_LOG"\nexit 90\n')
        p.chmod(0o700)
    result = subprocess.run(
        [sys.executable, str(root / "scripts/ci/smoke_single_process.py"), *arguments],
        env={**os.environ, "PATH": str(tmp_path), "CALL_LOG": str(log)},
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode == 2 and "Retired NSQ image smoke" in result.stderr
    assert not log.exists()
