"""Run on serverA. Read the database URL from stdin, never from command arguments."""

import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path


def secret_override(database_url: str) -> dict:
    if not database_url.startswith("mysql+asyncmy://") or any(
        char in database_url for char in "\r\n\x00"
    ):
        raise ValueError("Invalid database secret")
    # Compose interpolates values even in JSON; escape dollar signs literally.
    return {
        "services": {
            "api": {
                "environment": {
                    "QS_AI_DATABASE_URL": database_url.replace("$", "$$"),
                }
            }
        }
    }


def main() -> None:
    revision = sys.argv[1]
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("A full commit SHA is required")
    database_url = sys.stdin.read().rstrip("\n")
    override = secret_override(database_url)
    root = Path(__file__).resolve().parents[2]
    # Temporary directory is mode 0700 and removed on success or failure.
    with tempfile.TemporaryDirectory(prefix="qs-ai-deploy-") as temporary:
        directory = Path(temporary)
        metadata = directory / "image.env"
        metadata.write_text(f"QS_AI_IMAGE=qs-ai:{revision}\n")
        secrets = directory / "secrets.json"
        secrets.write_text(json.dumps(override))
        secrets.chmod(0o600)
        docker_config = directory / "docker"
        docker_config.mkdir()
        commands = [
            (
                "image build",
                [
                    "sudo",
                    "-n",
                    "docker",
                    "--config",
                    str(docker_config),
                    "build",
                    "--label",
                    f"org.opencontainers.image.revision={revision}",
                    "-t",
                    f"qs-ai:{revision}",
                    str(root),
                ],
            ),
        ]
        compose = [
            "sudo",
            "-n",
            "docker",
            "compose",
            "--env-file",
            str(metadata),
            "-f",
            str(root / "deploy/serverA/compose.yaml"),
            "-f",
            str(secrets),
        ]
        commands.extend(
            [
                (
                    "migration",
                    [
                        *compose,
                        "run",
                        "--rm",
                        "--no-deps",
                        "api",
                        "/app/.venv/bin/alembic",
                        "upgrade",
                        "head",
                    ],
                ),
                ("service readiness", [*compose, "up", "-d", "--wait", "--wait-timeout", "90"]),
            ]
        )
        for phase, command in commands:
            result = subprocess.run(command, capture_output=True, timeout=1200)
            if result.returncode:
                # Migration errors can contain connection details; never print raw output.
                raise RuntimeError(f"Deployment failed during {phase}; inspect on serverA")
            print(f"{phase}: passed", flush=True)
    print(f"Deployed {revision}; database readiness passed")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("Deployment failed; sensitive command output was suppressed", file=sys.stderr)
        raise SystemExit(1) from None
