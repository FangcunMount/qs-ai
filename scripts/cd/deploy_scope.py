"""Classify a main commit's runtime changes; never infer production acceptance."""

import subprocess
import sys

RUNTIME_PREFIXES = ("src/", "configs/", "migrations/", "integrations/", "deploy/", "scripts/cd/")
RUNTIME_FILES = {
    "Dockerfile",
    ".dockerignore",
    "pyproject.toml",
    "uv.lock",
    "alembic.ini",
    ".github/workflows/deploy.yml",
}


def requires_deploy(revision: str) -> bool:
    parents = subprocess.check_output(
        ["git", "rev-list", "--parents", "-n", "1", revision], text=True
    ).split()
    # First-parent comparison includes every runtime commit in a merged PR,
    # even when the final PR commit itself only updates documentation.
    if len(parents) > 1:
        command = ["git", "diff", "--name-only", "--no-renames", parents[1], parents[0]]
    else:
        command = ["git", "show", "--pretty=format:", "--name-only", "--no-renames", parents[0]]
    paths = subprocess.check_output(command, text=True).splitlines()
    return any(path in RUNTIME_FILES or path.startswith(RUNTIME_PREFIXES) for path in paths)


if __name__ == "__main__":
    print("true" if requires_deploy(sys.argv[1]) else "false")
