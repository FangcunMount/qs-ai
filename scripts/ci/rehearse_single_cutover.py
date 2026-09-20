"""Exercise actual release stop/verify functions across old/new Docker layouts.

Requires disposable images built from the old and new source, and a private test
MySQL/TLS environment. Never accepts production release directories.
"""

import importlib.util
import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from uuid import uuid4

import yaml

ROOT = Path(__file__).resolve().parents[2]


def run(*args):
    result = subprocess.run(args, text=True, capture_output=True, timeout=300)
    if result.returncode:
        raise RuntimeError(result.stdout + result.stderr)
    return result.stdout


def main():
    old_image, new_image = sys.argv[1:3]
    prefix = "qs-ai-rehearsal-" + uuid4().hex[:8]
    db_name = prefix + "-db"
    spec = importlib.util.spec_from_file_location("release", ROOT / "deploy/serverA/deploy.py")
    release = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(release)
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        root.chmod(0o755)
        tls = root / "tls"
        tls.mkdir(mode=0o755)
        run(
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=qs-ai.svc",
            "-addext",
            "subjectAltName=DNS:localhost,DNS:qs-ai-grpc",
            "-keyout",
            str(tls / "tls.key"),
            "-out",
            str(tls / "tls.crt"),
        )
        (tls / "tls.key").chmod(0o644)
        environment = {
            "QS_AI_ENVIRONMENT": "production",
            "QS_AI_DATABASE_URL": f"mysql+asyncmy://root:rehearsal_only@{db_name}:3306/qs_ai",
            "QS_AI_GRPC__CA_FILE": "/tls/tls.crt",
            "QS_AI_GRPC__CERT_FILE": "/tls/tls.crt",
            "QS_AI_GRPC__KEY_FILE": "/tls/tls.key",
            "QS_AI_GRPC__ACCESS_ADDRESS": "localhost:59999",
            "QS_AI_GRPC__RESULT_ADDRESS": "localhost:59999",
            "QS_AI_GENERATION__ENABLED": "true",
            "QS_AI_EVALUATION__ENABLED": "true",
            "QS_AI_GENERATION__ENDPOINT": "https://synthetic.invalid/responses",
            "QS_AI_MODEL_API_KEY": "synthetic-only-no-model-requests",
        }
        commands = {
            "api": ["qs_ai.bootstrap.http"],
            "grpc": ["qs_ai.bootstrap.integration", "serve"],
            "worker": ["qs_ai.bootstrap.worker", "--serve"],
            "evaluation": ["qs_ai.bootstrap.evaluation", "--serve"],
            "delivery": ["qs_ai.bootstrap.integration", "deliver", "--continuous"],
        }
        old, new = root / "old", root / "new"
        for path, image, entries in (
            (old, old_image, commands),
            (new, new_image, {"qs-ai": ["qs_ai.bootstrap.server"]}),
        ):
            path.mkdir()
            services = {}
            for name, command in entries.items():
                if name in {"api", "qs-ai"}:
                    health = [
                        "-c",
                        "import urllib.request; urllib.request.urlopen('http://localhost:8000/readyz')",
                    ]
                elif name == "grpc":
                    health = ["-m", "qs_ai.bootstrap.grpc_probe"]
                else:
                    health = [
                        "-m",
                        "qs_ai.bootstrap.daemon_health",
                        f"/tmp/qs-ai-{name}-health.json",
                    ]
                services[name] = {
                    "healthcheck": {
                        "test": ["CMD", "/app/.venv/bin/python", *health],
                        "interval": "2s",
                        "timeout": "5s",
                        "retries": 30,
                    },
                    "image": image,
                    "command": ["/app/.venv/bin/python", "-m", *command],
                    "environment": environment,
                    "volumes": [f"{tls}:/tls:ro"],
                    "read_only": True,
                    "tmpfs": ["/tmp"],
                    "networks": {
                        "backend": {"aliases": ["qs-ai-grpc"] if name in {"grpc", "qs-ai"} else []}
                    },
                }
            (path / "compose.yaml").write_text(
                yaml.safe_dump(
                    {
                        "services": services,
                        "networks": {"backend": {"external": True, "name": prefix}},
                    }
                )
            )
            (path / "runtime.json").write_text('{"services":{}}')
            (path / "image.env").write_text("")
            (path / "manifest.json").write_text(
                json.dumps(
                    {"image_id": json.loads(run("docker", "image", "inspect", image))[0]["Id"]}
                )
            )
        original = release.compose

        def compose(path, *args):
            command = original(path, *args)[2:]  # Test host needs no sudo.
            command[command.index("-p") + 1] = prefix
            return command

        release.compose = compose

        def release_run(phase, args):
            if args[:2] == ["sudo", "-n"]:
                args = args[2:]
            return run(*args)

        release.run = release_run

        def healthy(path):
            service = "qs-ai" if path == new else "api"
            for _ in range(60):
                try:
                    run(
                        *compose(
                            path,
                            "exec",
                            "-T",
                            service,
                            "/app/.venv/bin/python",
                            "-c",
                            "import urllib.request; urllib.request.urlopen('http://localhost:8000/readyz')",
                        )
                    )
                    return
                except RuntimeError:
                    time.sleep(0.2)
            raise RuntimeError("Runtime readiness timed out")

        try:
            run("docker", "network", "create", prefix)
            run(
                "docker",
                "run",
                "-d",
                "--name",
                db_name,
                "--network",
                prefix,
                "-e",
                "MYSQL_ROOT_PASSWORD=rehearsal_only",
                "-e",
                "MYSQL_DATABASE=qs_ai",
                "mysql:8.4",
            )
            for _ in range(90):
                try:
                    run(
                        "docker",
                        "exec",
                        db_name,
                        "mysql",
                        "--protocol=tcp",
                        "-h127.0.0.1",
                        "-prehearsal_only",
                        "-Dqs_ai",
                        "-eSELECT 1",
                    )
                    break
                except RuntimeError:
                    time.sleep(1)
            run(
                *compose(
                    new, "run", "--rm", "-T", "qs-ai", "/app/.venv/bin/alembic", "upgrade", "head"
                )
            )
            release.verify(old)
            healthy(old)
            release.stop_release(old)
            release.verify(new)
            healthy(new)
            # A verification failure triggers the same stop-new/restore-old sequence as apply().
            release.stop_release(new)
            release.verify(old)
            healthy(old)
            assert len(run(*compose(old, "ps", "--status", "running", "-q")).split()) == 5
            release.stop_release(old)
            release.verify(new)
            healthy(new)
            assert len(run(*compose(new, "ps", "--status", "running", "-q")).split()) == 1
            container = run(*compose(new, "ps", "-q", "qs-ai")).strip()
            assert len(run("docker", "top", container, "-eo", "pid,args").strip().splitlines()) == 2
            print(
                json.dumps(
                    {
                        "old_to_new": "passed",
                        "new_to_old": "passed",
                        "old_to_new_again": "passed",
                        "old_count": 5,
                        "new_count": 1,
                        "model_calls": 0,
                        "database": "MySQL 8.4",
                    }
                )
            )
        finally:
            subprocess.run(compose(new, "down", "--remove-orphans"), capture_output=True)
            subprocess.run(["docker", "rm", "-f", "-v", db_name], capture_output=True)
            subprocess.run(["docker", "network", "rm", prefix], capture_output=True)


if __name__ == "__main__":
    main()
