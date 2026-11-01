"""The optional image job cannot reach production or select mutable candidates."""

from pathlib import Path

import yaml


def test_image44_job_is_manual_hosted_and_has_no_business_credentials():
    raw = Path(".github/workflows/ci.yml").read_text()
    workflow = yaml.safe_load(raw)
    job = workflow["jobs"]["image44-qualification"]
    assert "github.event_name == 'workflow_dispatch'" in job["if"]
    assert "inputs.image44_digest != ''" in job["if"]
    assert job["runs-on"] == "ubuntu-latest" and "environment" not in job
    assert not any(
        "MYSQL" in k or "QS_AI_" in k or "SSH" in k
        for step in job["steps"]
        for k in step.get("env", {})
    )
    steps = job["steps"]
    gate = next(i for i, step in enumerate(steps) if "successful main CI" in step.get("name", ""))
    login = next(i for i, step in enumerate(steps) if step.get("uses") == "docker/login-action@v3")
    assert gate < login
    assert "^sha256:[0-9a-f]{64}$" in steps[gate]["run"]
    assert 'git merge-base --is-ancestor "$SERVICE_SHA" FETCH_HEAD' in steps[gate]["run"]
    assert "--branch main --event push" in steps[gate]["run"]
    assert "docker pull --platform linux/amd64" in raw
    assert "ai44_image_start.py" in raw and '--source-sha "$SERVICE_SHA"' in raw
    assert not any("deploy.py" in step.get("run", "") for step in steps)
