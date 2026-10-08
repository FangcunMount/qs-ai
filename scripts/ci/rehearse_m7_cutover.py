"""Archived M7 gRPC receipt rehearsal; fail closed before creating any resources.

The retained probe documents the historical synthetic gRPC receiver. Current
runtime requires MQ, so this entry point cannot certify current delivery behavior.
Use tests/probes/mq_fault_acceptance.py for MQ and maintenance.schema_refactor for
the full 0038/0040 forward/reverse storage rehearsal. Images are inspected offline
before rejecting this entry point; no database, migration or network action runs.
"""

import asyncio
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from uuid import uuid4

from sqlalchemy import update

SCHEMA_HEAD = "0040_module_table_names"


def run(*args: str) -> str:
    return subprocess.check_output(args, text=True, stderr=subprocess.PIPE)


def image_heads(image: str) -> list[str]:
    heads = json.loads(
        run(
            "docker",
            "run",
            "--rm",
            "--network",
            "none",
            "--read-only",
            image,
            "/app/.venv/bin/python",
            "-c",
            "import json; from alembic.config import Config; "
            "from alembic.script import ScriptDirectory; "
            "print(json.dumps(sorted(ScriptDirectory.from_config(Config('/app/alembic.ini')).get_heads())))",
        )
    )
    if not isinstance(heads, list) or heads != [SCHEMA_HEAD]:
        raise RuntimeError("M7 rehearsal requires both images to use the complete 0040 layout")
    return heads


def require_same_layout(old: str, new: str) -> None:
    if image_heads(old) != image_heads(new):
        raise RuntimeError("Cross-layout rehearsal requires maintenance.schema_refactor")


async def probe(command: str) -> None:
    from sqlalchemy import func, insert, select, text
    from sqlalchemy.ext.asyncio import create_async_engine

    from qs_ai.domain.interpretation.model import Actor, Session, Status
    from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
    from qs_ai.infrastructure.persistence.mysql.result_outbox import stage_state
    from qs_ai.infrastructure.persistence.mysql.schema import (
        evaluation_runs,
        execution_configurations,
        jobs,
        model_calls,
        result_outbox,
        sessions,
    )

    database = Database(os.environ["QS_AI_DATABASE_URL"])
    receiver = create_async_engine(os.environ["M7_RECEIVER_DATABASE_URL"])
    try:
        if command == "setup":
            async with receiver.begin() as db:
                await db.execute(
                    text(
                        "CREATE TABLE receipts (event_id VARCHAR(36) PRIMARY KEY, "
                        "wire LONGBLOB NOT NULL, deliveries INT NOT NULL)"
                    )
                )
                await db.execute(
                    text("CREATE TABLE control (id INT PRIMARY KEY, mode VARCHAR(16))")
                )
                await db.execute(text("INSERT INTO control VALUES (1, 'lost')"))
        elif command in ("lost", "normal"):
            async with receiver.begin() as db:
                await db.execute(text("UPDATE control SET mode=:mode"), {"mode": command})
        elif command == "seed":
            session = Session(
                str(uuid4()),
                Actor("7", "m7-synthetic"),
                "7",
                ("42",),
                "M7 无启动通知 Unicode 🙂",
                status=Status.CREATED,
            )
            async with Transactions(database).open() as db:
                await db.execute(
                    insert(sessions).values(
                        id=session.id,
                        org_id=7,
                        owner_subject_id=session.actor.subject_id,
                        testee_id=7,
                        assessment_ids=["42"],
                        goal=session.goal,
                        status=session.status,
                        version=session.version,
                        workflow_version=session.workflow_version,
                        created_at=func.utc_timestamp(6),
                        updated_at=func.utc_timestamp(6),
                    )
                )
                await db.execute(
                    update(sessions)
                    .where(sessions.c.id == session.id)
                    .values(request_id=str(uuid4()))
                )
                await stage_state(db, session)
                # A second staging must preserve the first identity/payload/time.
                await stage_state(db, session)
                await db.commit()
        elif command == "snapshot":
            assert database.engine is not None
            async with database.engine.connect() as db:
                rows = (
                    (await db.execute(select(result_outbox).order_by(result_outbox.c.event_id)))
                    .mappings()
                    .all()
                )
                result = {"outbox": [dict(row) for row in rows], "counts": {}}
                for logical_name, table in (
                    ("execution_jobs", jobs),
                    ("model_calls", model_calls),
                    ("execution_configurations", execution_configurations),
                    ("evaluation_runs", evaluation_runs),
                ):
                    result["counts"][logical_name] = await db.scalar(
                        select(func.count()).select_from(table)
                    )
                result["schema"] = await db.scalar(text("SELECT version_num FROM alembic_version"))
            async with receiver.connect() as db:
                rows = (
                    (await db.execute(text("SELECT * FROM receipts ORDER BY event_id")))
                    .mappings()
                    .all()
                )
                result["receipts"] = [
                    {
                        "event_id": row["event_id"],
                        "wire_sha256": hashlib.sha256(row["wire"]).hexdigest(),
                        "deliveries": row["deliveries"],
                    }
                    for row in rows
                ]
            print(json.dumps(result, default=str, ensure_ascii=False))
        else:
            raise ValueError(command)
    finally:
        await database.close()
        await receiver.dispose()


async def serve() -> None:
    import grpc
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine

    from qs_ai.contracts.workflow import workflow_pb2 as pb
    from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc

    engine = create_async_engine(os.environ["M7_RECEIVER_DATABASE_URL"])

    class Receiver(rpc.ResultsServicer):
        async def Accept(self, request, context):
            wire = request.SerializeToString(deterministic=True)
            async with engine.begin() as db:
                await db.execute(
                    text(
                        "INSERT INTO receipts VALUES (:id,:wire,1) "
                        "ON DUPLICATE KEY UPDATE deliveries=deliveries+1"
                    ),
                    {"id": request.event_id, "wire": wire},
                )
                stored = await db.scalar(
                    text("SELECT wire FROM receipts WHERE event_id=:id"), {"id": request.event_id}
                )
                if stored != wire:
                    raise RuntimeError("Original wire changed on repeat")
                mode = await db.scalar(text("SELECT mode FROM control WHERE id=1"))
            # The committed durable effect exists before the injected lost reply.
            if mode == "lost":
                await context.abort(grpc.StatusCode.UNAVAILABLE, "M7 lost durable reply")
            return pb.Acknowledgement(event_id=request.event_id)

    root = Path("/tls")
    credentials = grpc.ssl_server_credentials(
        [((root / "tls.key").read_bytes(), (root / "tls.crt").read_bytes())],
        root_certificates=(root / "tls.crt").read_bytes(),
        require_client_auth=True,
    )
    server = grpc.aio.server()
    rpc.add_ResultsServicer_to_server(Receiver(), server)
    assert server.add_secure_port("0.0.0.0:50062", credentials)
    await server.start()
    try:
        await server.wait_for_termination()
    finally:
        await server.stop(0)
        await engine.dispose()


def main() -> None:
    old, new, destination = sys.argv[1:]
    require_same_layout(old, new)
    raise RuntimeError(
        "Archived gRPC M7 rehearsal is disabled for the MQ runtime; use "
        "tests/probes/mq_fault_acceptance.py and maintenance.schema_refactor"
    )


def archived_grpc_rehearsal(old: str, new: str, destination: str) -> None:
    """Historical synthetic receiver flow; main never invokes it."""
    output = Path(destination).resolve()
    output.mkdir(parents=True, exist_ok=True)
    prefix = "m7-cutover-" + uuid4().hex[:10]
    network, mysql, receiver, app = prefix, prefix + "-mysql", prefix + "-receiver", prefix + "-app"
    own = (app, receiver, mysql)
    events = []
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
            "/CN=m7-disposable",
            "-addext",
            "subjectAltName=DNS:localhost,DNS:m7-receiver",
            "-keyout",
            str(root / "tls.key"),
            "-out",
            str(root / "tls.crt"),
        )
        (root / "tls.key").chmod(0o644)  # Disposable synthetic key only.
        environment = [
            "-e",
            "QS_AI_ENVIRONMENT=production",
            "-e",
            "TZ=Asia/Shanghai",
            "-e",
            f"QS_AI_DATABASE_URL=mysql+asyncmy://root:m7_test_only@{mysql}:3306/qs_ai",
            "-e",
            f"M7_RECEIVER_DATABASE_URL=mysql+asyncmy://root:m7_test_only@{mysql}:3306/m7_receiver",
            "-e",
            "QS_AI_GENERATION__ENABLED=false",
            "-e",
            "QS_AI_EVALUATION__ENABLED=false",
            "-e",
            "QS_AI_EVALUATION__CANDIDATE_MODE_ENABLED=false",
            "-e",
            "QS_AI_GRPC__CA_FILE=/tls/tls.crt",
            "-e",
            "QS_AI_GRPC__CERT_FILE=/tls/tls.crt",
            "-e",
            "QS_AI_GRPC__KEY_FILE=/tls/tls.key",
            "-e",
            "QS_AI_GRPC__RESULT_ADDRESS=m7-receiver:50062",
            "-v",
            f"{root}:/tls:ro",
            "-v",
            f"{Path(__file__).resolve()}:/m7.py:ro",
        ]

        def check(command, image=new):
            return run(
                "docker",
                "run",
                "--rm",
                "--network",
                network,
                *environment,
                image,
                "/app/.venv/bin/python",
                "/m7.py",
                "probe",
                command,
            )

        def snapshot(label):
            data = json.loads(check("snapshot"))
            assert not any(data["counts"].values()), data
            assert data["schema"] == SCHEMA_HEAD, data
            (output / f"{label}.json").write_text(
                json.dumps(data, ensure_ascii=False, indent=2) + "\n"
            )
            return data

        def wait_for(predicate, label):
            end = time.monotonic() + 60
            while time.monotonic() < end:
                data = snapshot(label)
                if predicate(data):
                    return data
                time.sleep(1)
            raise RuntimeError(f"Timed out: {label}")

        def start(image):
            assert (
                not subprocess.run(["docker", "inspect", app], capture_output=True).returncode == 0
            )
            run(
                "docker",
                "run",
                "-d",
                "--name",
                app,
                "--network",
                network,
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
            end = time.monotonic() + 90
            while time.monotonic() < end:
                ready = subprocess.run(
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
                if ready.returncode == 0:
                    break
                time.sleep(1)
            else:
                raise RuntimeError("Single application did not become ready")
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
            processes = run("docker", "top", app, "-eo", "pid,args").strip().splitlines()
            assert len(processes) == 2 and "qs_ai.bootstrap.server" in processes[1], processes
            events.append(
                {
                    "started": image,
                    "single_python_process": True,
                    "ready": True,
                    "grpc_mtls_probe": True,
                }
            )

        def stop(label):
            start_time = time.monotonic()
            run("docker", "stop", "-t", "210", app)
            state = json.loads(run("docker", "inspect", app))[0]["State"]
            assert state["ExitCode"] == 0 and not state["OOMKilled"], state
            (output / f"{label}.log").write_text(run("docker", "logs", app))
            events.append(
                {"stopped": label, "exit": 0, "seconds": round(time.monotonic() - start_time, 3)}
            )
            run("docker", "rm", "-v", app)

        def unchanged(before, after):
            previous = {row["event_id"]: row for row in before["outbox"]}
            current = {row["event_id"]: row for row in after["outbox"]}
            assert previous.keys() <= current.keys()
            for key, row in previous.items():
                for field in ("session_id", "version", "payload", "created_at"):
                    assert row[field] == current[key][field], (key, field)
            receipts = {row["event_id"]: row for row in after["receipts"]}
            for row in before["receipts"]:
                assert receipts[row["event_id"]]["wire_sha256"] == row["wire_sha256"]

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
                "MYSQL_ROOT_PASSWORD=m7_test_only",
                "-e",
                "MYSQL_DATABASE=qs_ai",
                "mysql:8.4",
            )
            for _ in range(90):
                ready = subprocess.run(
                    [
                        "docker",
                        "exec",
                        mysql,
                        "mysql",
                        "--protocol=tcp",
                        "-h127.0.0.1",
                        "-pm7_test_only",
                        "-eSELECT 1",
                    ],
                    capture_output=True,
                )
                if ready.returncode == 0:
                    break
                time.sleep(1)
            else:
                raise RuntimeError("Disposable MySQL did not start")
            run("docker", "exec", mysql, "mysql", "-pm7_test_only", "-eCREATE DATABASE m7_receiver")
            run(
                "docker",
                "run",
                "--rm",
                "--network",
                network,
                *environment,
                new,
                "/app/.venv/bin/alembic",
                "upgrade",
                "head",
            )
            check("setup")
            run(
                "docker",
                "run",
                "-d",
                "--name",
                receiver,
                "--network",
                network,
                "--network-alias",
                "m7-receiver",
                *environment,
                new,
                "/app/.venv/bin/python",
                "/m7.py",
                "serve",
            )
            start(old)
            check("seed", old)  # No wake to the running old application.
            first = wait_for(
                lambda d: len(d["receipts"]) == 1 and d["outbox"][0]["attempts"] > 0,
                "old-lost-reply",
            )
            assert not first["outbox"][0]["delivered"]
            stop("old-before-upgrade")
            check("normal")
            start(new)
            recovered = wait_for(
                lambda d: all(r["delivered"] for r in d["outbox"]), "new-recovered-old"
            )
            unchanged(first, recovered)
            assert recovered["receipts"][0]["deliveries"] >= 2
            check("lost")
            check("seed")  # No wake to the running new application.
            second = wait_for(
                lambda d: (
                    len(d["receipts"]) == 2
                    and any(r["attempts"] > 0 and not r["delivered"] for r in d["outbox"])
                ),
                "new-lost-reply",
            )
            stop("new-before-rollback")
            check("normal")
            start(old)
            final = wait_for(
                lambda d: len(d["outbox"]) == 2 and all(r["delivered"] for r in d["outbox"]),
                "old-recovered-new",
            )
            unchanged(second, final)
            assert len(final["receipts"]) == 2 and all(
                r["deliveries"] >= 2 for r in final["receipts"]
            )
            stop("old-after-rollback")
            (output / "result.json").write_text(
                json.dumps(
                    {
                        "passed": True,
                        "old": old,
                        "new": new,
                        "database": "MySQL 8.4; two disposable databases",
                        "receiver": "synthetic durable mTLS; not qs-server",
                        "mode": "single old -> single new -> single old; no simultaneous writer",
                        "identity_payload_time_preserved": True,
                        "no_notification_scan": True,
                        "lost_ack_deduplicated": True,
                        "model_calls": 0,
                        "candidate_mode_enabled": False,
                        "events": events,
                    },
                    indent=2,
                )
                + "\n"
            )
            print("M7 isolated old/new/old receipt recovery passed")
        finally:
            for name in own:
                logs = subprocess.run(["docker", "logs", name], capture_output=True, text=True)
                if logs.returncode == 0:
                    (output / f"{name}.log").write_text(logs.stdout + logs.stderr)
                subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)
            subprocess.run(["docker", "network", "rm", network], capture_output=True)


if __name__ == "__main__":
    if sys.argv[1] == "probe":
        asyncio.run(probe(sys.argv[2]))
    elif sys.argv[1] == "serve":
        asyncio.run(serve())
    else:
        main()
