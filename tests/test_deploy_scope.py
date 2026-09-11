import subprocess
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/cd/deploy_scope.py"


def git(repo, *args):
    return subprocess.check_output(
        ["git", "-c", "user.name=Test", "-c", "user.email=test@example.invalid", *args],
        cwd=repo,
        text=True,
        stderr=subprocess.DEVNULL,
    ).strip()


def commit(repo, path, text):
    file = repo / path
    file.parent.mkdir(parents=True, exist_ok=True)
    file.write_text(text)
    git(repo, "add", ".")
    git(repo, "commit", "-m", "test")
    return git(repo, "rev-parse", "HEAD")


def scope(repo, revision):
    return subprocess.check_output(
        [sys.executable, str(SCRIPT), revision], cwd=repo, text=True
    ).strip()


def test_merge_includes_runtime_before_final_docs_commit(tmp_path):
    git(tmp_path, "init", "--initial-branch=main")
    initial = commit(tmp_path, "README.md", "initial")
    assert scope(tmp_path, initial) == "false"
    git(tmp_path, "switch", "-c", "feature")
    commit(tmp_path, "src/qs_ai/runtime.py", "runtime")
    docs = commit(tmp_path, "docs/evidence.md", "evidence")
    assert scope(tmp_path, docs) == "false"
    git(tmp_path, "switch", "main")
    git(tmp_path, "merge", "--no-ff", "feature", "-m", "merge feature")
    assert scope(tmp_path, git(tmp_path, "rev-parse", "HEAD")) == "true"
    tests = commit(tmp_path, "tests/test_case.py", "tests only")
    assert scope(tmp_path, tests) == "false"
    git(tmp_path, "mv", "src/qs_ai/runtime.py", "tests/retired.py")
    git(tmp_path, "commit", "-m", "remove runtime")
    assert scope(tmp_path, git(tmp_path, "rev-parse", "HEAD")) == "true"


def test_manual_deploy_and_both_jobs_use_scope_gate():
    workflow = yaml.safe_load((ROOT / ".github/workflows/deploy.yml").read_text())
    jobs = workflow["jobs"]
    command = jobs["gate"]["steps"][-1]["run"]
    assert '"$TRIGGER" = workflow_dispatch' in command
    assert 'echo "deploy=true"' in command
    for name in ("build", "deploy"):
        assert "needs.gate.outputs.deploy == 'true'" in jobs[name]["if"]


def test_configuration_migration_and_image_inputs_require_release(tmp_path):
    git(tmp_path, "init", "--initial-branch=main")
    commit(tmp_path, "README.md", "initial")
    for path in (
        "configs/production.yaml",
        "migrations/versions/next.py",
        "integrations/qs_server/prompts/v7.json",
        "Dockerfile",
        "uv.lock",
        "deploy/serverA/compose.yaml",
        ".dockerignore",
        ".github/workflows/deploy.yml",
    ):
        assert scope(tmp_path, commit(tmp_path, path, "changed")) == "true"
    assert scope(tmp_path, commit(tmp_path, ".github/workflows/ci.yml", "tests")) == "false"


def test_docs_after_pending_runtime_use_deployed_baseline(tmp_path):
    git(tmp_path, "init", "--initial-branch=main")
    deployed = commit(tmp_path, "src/qs_ai/runtime.py", "old")
    runtime = commit(tmp_path, "src/qs_ai/runtime.py", "new")
    docs = commit(tmp_path, "README.md", "follow-up")

    def since(target, baseline):
        return subprocess.check_output(
            [sys.executable, str(SCRIPT), target, baseline], cwd=tmp_path, text=True
        ).strip()

    assert since(docs, deployed) == "true"
    assert since(docs, runtime) == "false"
    assert since(deployed, runtime) == "false"
