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
    network, mysql, app, nsq = prefix, prefix + "-mysql", prefix + "-app", prefix + "-nsq"
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
                nsq,
                "--network",
                network,
                "nsqio/nsq:v1.3.0",
                "/nsqd",
                "--broadcast-address=" + nsq,
            )
            # Generate disposable JOSE keys with the actual image dependency. The
            # ordinary app user owns private fixtures; no real credentials are read.
            fixture_code = """
import json, os, pathlib
from jwcrypto import jwk
from reliable_messaging.wire import failed_topic, FAILED_CHANNEL
p=pathlib.Path('/tls')
for kid in ('ai.sign', 'ai.encrypt', 'qs.sign', 'qs.encrypt'):
    key=jwk.JWK.generate(kty='EC', crv='P-256', kid=kid)
    private=kid.startswith('ai.')
    path=p/(kid+'.json')
    path.write_text(key.export(private_key=private))
    os.chown(path,10001,10001); path.chmod(0o600 if private else 0o644)
topology={'qs.ai.commands.v1':'qs-ai.commands.v1',
          'qs.ai.events.v1':'qs-server.ai-events.v1',
          'qs.ai.acks.v1':'qs-ai.acks.v1'}
topology.update({failed_topic(t,c):FAILED_CHANNEL for t,c in list(topology.items())})
(p/'topology.json').write_text(json.dumps(topology))
"""
            run(
                "docker",
                "run",
                "--rm",
                "--network",
                "none",
                "--user",
                "0:0",
                "-v",
                f"{root}:/tls:rw",
                "--entrypoint",
                "/app/.venv/bin/python",
                image,
                "-c",
                fixture_code,
            )
            provision_code = """
import json, sys, time, urllib.request, urllib.parse
origin='http://'+sys.argv[1]+':4151'
for attempt in range(30):
    try:
        with urllib.request.urlopen(origin+'/info', timeout=2) as r:
            assert json.load(r)['version']=='1.3.0'
        break
    except OSError:
        time.sleep(1)
else:
    raise RuntimeError('Disposable NSQ did not start')
for topic,channel in json.load(open('/tls/topology.json')).items():
    query=urllib.parse.urlencode({'topic':topic, 'channel':channel})
    for endpoint in ('topic/create', 'channel/create'):
        request=urllib.request.Request(origin+'/'+endpoint+'?'+query, data=b'', method='POST')
        with urllib.request.urlopen(request, timeout=5):
            pass
"""
            run(
                "docker",
                "run",
                "--rm",
                "--network",
                network,
                "-v",
                f"{root}:/tls:ro",
                "--entrypoint",
                "/app/.venv/bin/python",
                image,
                "-c",
                provision_code,
                nsq,
            )
            environment += [
                "-e",
                "QS_AI_GRPC__ACCESS_ADDRESS=qs-ai-grpc:50061",
                "-e",
                "QS_AI_GENERATION__ENABLED=false",
                "-e",
                "QS_AI_EVALUATION__ENABLED=false",
                "-e",
                "QS_AI_MESSAGING="
                + json.dumps(
                    {
                        "enabled": True,
                        "nsqd": {nsq + ":4150": "http://" + nsq + ":4151"},
                        "signing_key_file": "/tls/ai.sign.json",
                        "decrypt_key_files": {"ai.encrypt": "/tls/ai.encrypt.json"},
                        "qs_signer_files": {"qs.sign": "/tls/qs.sign.json"},
                        "qs_recipient_key_file": "/tls/qs.encrypt.json",
                        "max_in_flight": 1,
                    }
                ),
            ]
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
            counters = run(
                "docker",
                "exec",
                "-e",
                "MYSQL_PWD=smoke_test_only",
                mysql,
                "mysql",
                "--protocol=tcp",
                "-h127.0.0.1",
                "-Dqs_ai",
                "-N",
                "-B",
                "-eSELECT (SELECT COUNT(*) FROM execution_model_calls),"
                "(SELECT COUNT(*) FROM evaluation_dispatches)",
            )
            assert counters.strip() == "0\t0", "Smoke must not create model execution records"
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
                        "broker": "NSQ 1.3.0",
                        "messaging": "required MQ topology and lifecycle",
                        "model_calls_created": 0,
                        "graceful_exit": state["ExitCode"],
                        "stop_seconds": round(time.monotonic() - start, 3),
                    }
                )
            )
        finally:
            for name in (app, mysql, nsq):
                subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)
            subprocess.run(["docker", "network", "rm", network], capture_output=True)


if __name__ == "__main__":
    main()
