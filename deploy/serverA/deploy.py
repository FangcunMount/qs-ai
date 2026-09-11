"""Server-side release transaction; standard library only, run as deploy user."""

import fcntl
import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path


class DeploymentError(RuntimeError):
    pass


ROOT = Path("/opt/qs-ai")


def secret_override(database_url: str) -> dict:
    if not database_url.startswith("mysql+asyncmy://") or any(
        char in database_url for char in "\r\n\x00"
    ):
        raise ValueError("Invalid database secret")
    return {
        "services": {
            "api": {
                "environment": {
                    "QS_AI_DATABASE_URL": database_url.replace("$", "$$"),
                }
            }
        }
    }


def write_json(path: Path, value: dict) -> None:
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.chmod(0o600)
    temporary.replace(path)


def run(phase: str, args: list[str]) -> str:
    result = subprocess.run(args, capture_output=True, text=True, timeout=1200)
    if result.returncode:
        # Do not expose raw driver/Compose errors, which may contain credentials.
        detail = ""
        if phase == "database probe":
            try:
                diagnostic = json.loads(result.stdout.strip().splitlines()[-1])
                code = diagnostic.get("driver_code")
                if isinstance(code, int):
                    detail = f", database driver code {code}"
            except (ValueError, IndexError):
                pass
        raise DeploymentError(f"{phase} failed (exit {result.returncode}{detail})")
    return result.stdout


def compose(release: Path, *args: str) -> list[str]:
    return [
        "sudo",
        "-n",
        "docker",
        "compose",
        "-p",
        "qs-ai",
        "--env-file",
        str(release / "image.env"),
        "-f",
        str(release / "compose.yaml"),
        "-f",
        str(release / "runtime.json"),
        *args,
    ]


def probe(release: Path, require_head: bool = False) -> dict:
    output = run(
        "database probe",
        compose(
            release,
            "run",
            "--rm",
            "--no-deps",
            "-T",
            "api",
            "/app/.venv/bin/python",
            "-m",
            "qs_ai.bootstrap.database_check",
            *(["--require-head"] if require_head else []),
        ),
    )
    return json.loads(output.strip().splitlines()[-1])


def verify(release: Path) -> dict:
    manifest = json.loads((release / "manifest.json").read_text())
    run(
        "service readiness",
        compose(
            release,
            "up",
            "-d",
            "--remove-orphans",
            "--pull",
            "never",
            "--wait",
            "--wait-timeout",
            "120",
        ),
    )
    services = run("compose services", compose(release, "config", "--services")).split()
    if not services:
        raise DeploymentError("No release services configured")
    for service in services:
        container = run("running container", compose(release, "ps", "-q", service)).strip()
        if not container:
            raise DeploymentError("Missing running service")
        image_id = run(
            "running image",
            ["sudo", "-n", "docker", "inspect", container, "--format", "{{.Image}}"],
        ).strip()
        if image_id != manifest["image_id"]:
            raise DeploymentError("Running image does not match release")
    return probe(release, True)


def release_path(name: str) -> Path:
    if not re.fullmatch(r"[0-9a-f]{40}-[0-9]+-[0-9]+", name):
        raise ValueError("Invalid release identifier")
    return ROOT / "releases" / name


def restore(state: dict) -> None:
    previous = state.get("previous")
    if not previous:
        raise DeploymentError("No previous successful release")
    target = release_path(previous)
    # Old image must recognize the current schema and require its own exact head.
    probe(target, True)
    verify(target)
    write_json(ROOT / "state.json", {"current": previous, "previous": state["current"]})
    print(json.dumps({"rollback": "passed", "release": previous}))


def apply(release: Path, state: dict) -> None:
    manifest = json.loads((release / "manifest.json").read_text())
    revision = manifest["revision"]
    if not re.fullmatch(r"[0-9a-f]{40}", revision) or not release.name.startswith(revision + "-"):
        raise ValueError("Invalid release revision")
    archive = release / "image.tar.gz"
    checksum = hashlib.sha256()
    with archive.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            checksum.update(block)
    digest = checksum.hexdigest()
    if digest != manifest["archive_sha256"]:
        raise DeploymentError("Image archive checksum mismatch")
    run("image load", ["sudo", "-n", "docker", "load", "-i", str(archive)])
    inspected = json.loads(
        run("image identity", ["sudo", "-n", "docker", "image", "inspect", f"qs-ai:{revision}"])
    )[0]
    if (
        inspected["Id"] != manifest["image_id"]
        or inspected["Architecture"] != "amd64"
        or inspected["Config"]["Labels"].get("org.opencontainers.image.revision") != revision
    ):
        raise DeploymentError("Image identity or architecture mismatch")
    services = run("compose services", compose(release, "config", "--services")).split()
    if "grpc" in services:
        run(
            "TLS file preflight",
            compose(
                release,
                "run",
                "--rm",
                "--no-deps",
                "-T",
                "grpc",
                "/app/.venv/bin/python",
                "-m",
                "qs_ai.bootstrap.grpc_probe",
                "--check-files",
            ),
        )
    before = probe(release)
    write_json(release / "verification.json", {"phase": "preflight", "database": before})
    # A migration failure never replaces a healthy service. MySQL DDL may be partial.
    run(
        "migration",
        compose(
            release,
            "run",
            "--rm",
            "--no-deps",
            "-T",
            "api",
            "/app/.venv/bin/alembic",
            "upgrade",
            "head",
        ),
    )
    after = probe(release, True)
    try:
        verify(release)
    except Exception:
        if state.get("current") and before["current"] == after["current"]:
            previous = release_path(state["current"])
            probe(previous, True)
            verify(previous)
            print("Service restored to previous successful release", flush=True)
        elif not state.get("current"):
            run("first release cleanup", compose(release, "down"))
        else:
            print("Schema changed; application rollback requires compatibility review", flush=True)
        raise
    write_json(
        release / "verification.json",
        {"phase": "ready", "database": after, "image_id": manifest["image_id"]},
    )
    write_json(ROOT / "state.json", {"current": release.name, "previous": state.get("current")})
    # Loaded images and release metadata remain available for rollback.
    archive.unlink()
    print(json.dumps({"deployed": revision, "database": after, "release": release.name}))


def main() -> None:
    os.umask(0o077)
    ROOT.mkdir(exist_ok=True)
    with (ROOT / "deploy.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        path = ROOT / "state.json"
        state = json.loads(path.read_text()) if path.exists() else {}
        if sys.argv[1] == "rollback":
            restore(state)
        else:
            release = release_path(sys.argv[1])
            try:
                apply(release, state)
            except Exception as error:
                write_json(
                    release / "failure.json",
                    {
                        "status": "failed",
                        "error_type": type(error).__name__,
                        "phase": str(error) if isinstance(error, DeploymentError) else "unexpected",
                    },
                )
                raise


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        message = str(error) if isinstance(error, DeploymentError) else type(error).__name__
        print(f"Deployment failed: {message}", file=sys.stderr)
        raise SystemExit(1) from None
