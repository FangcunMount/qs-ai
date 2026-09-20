"""Server-side release transaction; standard library only, run as deploy user."""

import contextlib
import fcntl
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path


class DeploymentError(RuntimeError):
    pass


ROOT = Path("/opt/qs-ai")
RETENTION_ROOT = Path("/var/lib/fangcun-image-retention")


def secret_override(database_url: str) -> dict:
    if not database_url.startswith("mysql+asyncmy://") or any(
        char in database_url for char in "\r\n\x00"
    ):
        raise ValueError("Invalid database secret")
    return {
        "services": {
            "qs-ai": {
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


def runtime_service(release: Path) -> str:
    services = run("compose services", compose(release, "config", "--services")).split()
    return "qs-ai" if "qs-ai" in services else "api"


def stop_release(release: Path) -> None:
    # Stop admission first. Then stop old consumers together and wait for drain.
    services = run("compose services", compose(release, "config", "--services")).split()
    ingress = [s for s in ("grpc", "api") if s in services]
    if ingress:
        run("stop old admission", compose(release, "stop", "-t", "210", *ingress))
    run("drain release", compose(release, "stop", "-t", "210"))
    running = run("verify stopped", compose(release, "ps", "--status", "running", "-q")).strip()
    if running:
        raise DeploymentError("Previous release still running")


def probe(release: Path, require_head: bool = False) -> dict:
    output = run(
        "database probe",
        compose(
            release,
            "run",
            "--rm",
            "--no-deps",
            "-T",
            runtime_service(release),
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
    if "qs-ai" in services:
        run(
            "live mTLS probe",
            compose(
                release,
                "exec",
                "-T",
                "qs-ai",
                "/app/.venv/bin/python",
                "-m",
                "qs_ai.bootstrap.grpc_probe",
            ),
        )
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
    current = release_path(state["current"])
    try:
        stop_release(current)
        verify(target)
    except Exception:
        stop_release(target)
        probe(current, True)
        verify(current)
        raise
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
    if "grpc" in services or "qs-ai" in services:
        run(
            "TLS file preflight",
            compose(
                release,
                "run",
                "--rm",
                "--no-deps",
                "-T",
                "qs-ai" if "qs-ai" in services else "grpc",
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
            runtime_service(release),
            "/app/.venv/bin/alembic",
            "upgrade",
            "head",
        ),
    )
    after = probe(release, True)
    try:
        if state.get("current"):
            stop_release(release_path(state["current"]))
        verify(release)
    except Exception:
        if state.get("current") and before["current"] == after["current"]:
            previous = release_path(state["current"])
            stop_release(release)
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


@contextlib.contextmanager
def global_deploy_lock():
    directory = str(RETENTION_ROOT)
    path = RETENTION_ROOT / "deploy.lock"
    run("retention directory", ["sudo", "-n", "mkdir", "-p", directory])
    run("retention directory mode", ["sudo", "-n", "chmod", "0755", directory])
    if not path.exists():
        # sudoers permits chown/chmod/ln, not touch. Publish a root-owned inode
        # without replacing a concurrent initializer's file or held flock.
        # ROOT and RETENTION_ROOT must reside on the same filesystem.
        with tempfile.TemporaryDirectory(prefix=".retention-lock-", dir=ROOT) as temporary:
            candidate = Path(temporary) / "lock"
            candidate.touch(exist_ok=False)
            run("deployment lock owner", ["sudo", "-n", "chown", "root:root", str(candidate)])
            run("deployment lock mode", ["sudo", "-n", "chmod", "0666", str(candidate)])
            try:
                run("deployment lock", ["sudo", "-n", "ln", "--", str(candidate), str(path)])
            except DeploymentError:
                if not path.is_file():
                    raise
    with open(path, "r+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield


def retain_successful_image(release):
    # Cleanup has its own failure status; never trigger application rollback.
    try:
        revision = json.loads((release / "manifest.json").read_text())["revision"]
        script = Path(__file__)
        helper = (
            script.with_name("image-retention.py")
            if script.name == "deploy.py"
            else (script.with_name(script.stem + "-retention.py"))
        )
        previous = json.loads((ROOT / "state.json").read_text()).get("previous")
        protected = []
        if previous:
            previous_manifest = json.loads((release_path(previous) / "manifest.json").read_text())
            protected = ["--protect-image-id", previous_manifest["image_id"]]
        run(
            "image retention",
            [
                "sudo",
                "-n",
                "python3",
                str(helper),
                "--service",
                "qs-ai",
                "--image-ref",
                f"qs-ai:{revision}",
                "--apply",
                "--deployment-locked",
                *protected,
            ],
        )
    except Exception:
        print("::warning::Deployment succeeded, but image retention failed; inspect server audit.")


def main() -> None:
    os.umask(0o077)
    ROOT.mkdir(exist_ok=True)
    with global_deploy_lock(), (ROOT / "deploy.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        path = ROOT / "state.json"
        state = json.loads(path.read_text()) if path.exists() else {}
        if sys.argv[1] == "rollback":
            restore(state)
            retain_successful_image(release_path(state["previous"]))
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
            retain_successful_image(release)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        message = str(error) if isinstance(error, DeploymentError) else type(error).__name__
        print(f"Deployment failed: {message}", file=sys.stderr)
        raise SystemExit(1) from None
