"""Disposable container smoke: real MySQL, TLS, one Python PID and graceful stop.

No production credentials or model requests. All objects use a unique test prefix.
"""

import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from uuid import uuid4


def run(*args: str) -> str:
    try:
        return subprocess.check_output(args, text=True, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as error:
        # This helper only handles its own synthetic, disposable test resources.
        print(error.output)
        raise


def main() -> None:
    image = sys.argv[1]
    prefix = "qs-ai-smoke-" + uuid4().hex[:10]
    network, mysql, app = prefix, prefix + "-mysql", prefix + "-app"
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        root.chmod(0o755)
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
            str(root / "tls.key"),
            "-out",
            str(root / "tls.crt"),
        )
        (root / "tls.key").chmod(0o644)  # Synthetic test material only, never a real secret.
        environment = [
            "-e",
            "QS_AI_ENVIRONMENT=production",
            "-e",
            f"QS_AI_DATABASE_URL=mysql+asyncmy://root:smoke_test_only@{mysql}:3306/qs_ai",
            "-e",
            "QS_AI_GRPC__CA_FILE=/tls/tls.crt",
            "-e",
            "QS_AI_GRPC__CERT_FILE=/tls/tls.crt",
            "-e",
            "QS_AI_GRPC__KEY_FILE=/tls/tls.key",
            "-e",
            "QS_AI_GRPC__RESULT_ADDRESS=localhost:59999",
            "-v",
            f"{root}:/tls:ro",
        ]
        try:
            run("docker", "network", "create", network)
            run(
                "docker",
                "run",
                "-d",
                "--name",
                mysql,
                "--network",
                network,
                "-e",
                "MYSQL_ROOT_PASSWORD=smoke_test_only",
                "-e",
                "MYSQL_DATABASE=qs_ai",
                "mysql:8.4",
            )
            for _ in range(90):
                check = subprocess.run(
                    [
                        "docker",
                        "exec",
                        mysql,
                        "mysql",
                        "--protocol=tcp",
                        "-h127.0.0.1",
                        "-Dqs_ai",
                        "-eSELECT 1",
                        "-psmoke_test_only",
                    ],
                    capture_output=True,
                )
                if check.returncode == 0:
                    break
                time.sleep(1)
            else:
                raise RuntimeError("Disposable MySQL did not start")
            run(
                "docker",
                "run",
                "--rm",
                "--network",
                network,
                *environment,
                image,
                "/app/.venv/bin/alembic",
                "upgrade",
                "head",
            )
            run(
                "docker",
                "run",
                "-d",
                "--name",
                app,
                "--network",
                network,
                "--network-alias",
                "qs-ai-grpc",
                "--read-only",
                "--tmpfs",
                "/tmp",
                "--cpus",
                "2",
                "--memory",
                "1g",
                *environment,
                image,
            )
            for _ in range(60):
                check = subprocess.run(
                    [
                        "docker",
                        "exec",
                        app,
                        "/app/.venv/bin/python",
                        "-c",
                        "import urllib.request; urllib.request.urlopen('http://localhost:8000/readyz')",
                    ],
                    capture_output=True,
                )
                if check.returncode == 0:
                    break
                time.sleep(1)
            else:
                raise RuntimeError("Unified process did not become ready")
            run(
                "docker",
                "exec",
                app,
                "/app/.venv/bin/python",
                "-m",
                "qs_ai.bootstrap.grpc_probe",
                "--address",
                "localhost:50061",
            )
            # Probe process has exited; no supervisor/worker child processes may remain.
            processes = run("docker", "top", app, "-eo", "pid,args").strip().splitlines()
            assert len(processes) == 2, processes
            assert "qs_ai.bootstrap.server" in processes[1], processes
            start = time.monotonic()
            run("docker", "stop", "-t", "210", app)
            state = json.loads(run("docker", "inspect", app))[0]["State"]
            assert state["ExitCode"] == 0 and not state["OOMKilled"], state
            print(
                json.dumps(
                    {
                        "single_process": True,
                        "mtls": "passed",
                        "database": "MySQL 8.4",
                        "graceful_exit": state["ExitCode"],
                        "stop_seconds": round(time.monotonic() - start, 3),
                    }
                )
            )
        finally:
            for name in (app, mysql):
                subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)
            subprocess.run(["docker", "network", "rm", network], capture_output=True)


if __name__ == "__main__":
    main()
