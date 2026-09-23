"""QS client -> mTLS -> actual MySQL capability, without model or admission writes."""

import asyncio
import json
import os
import shutil
import tempfile
from dataclasses import asdict
from pathlib import Path

import grpc
import pytest

from qs_ai.bootstrap.container import create_container
from qs_ai.config import Settings
from qs_ai.contracts.workflow import workflow_pb2_grpc as rpc
from qs_ai.infrastructure.persistence.mysql.publications import MySQLPublications
from qs_ai.infrastructure.qs_server.evaluation_suite import V6_PUBLISHED
from qs_ai.transport.grpc.commands import Commands
from tests.integration.test_delivery import certificates
from tests.integration.test_eligibility import dispatched as dispatched
from tests.integration.test_eligibility import freeze_creation as freeze_creation
from tests.integration.test_eligibility import judge as judge
from tests.integration.test_eligibility import passing_reviewable as passing_reviewable
from tests.integration.test_eligibility import passing_semantics as passing_semantics
from tests.integration.test_eligibility import persisted_assets as persisted_assets
from tests.integration.test_eligibility import ready as ready
from tests.integration.test_eligibility import reviewable as reviewable
from tests.integration.test_eligibility import setup_run as setup_run
from tests.test_input_binding import bound_case
from tests.test_mbti_runtime import mbti_case

pytestmark = [
    pytest.mark.integration,
    pytest.mark.interop,
    pytest.mark.parametrize("freeze_creation", [V6_PUBLISHED], indirect=True),
]


async def process(*args, input=None, cwd=None):
    child = await asyncio.create_subprocess_exec(
        *args,
        cwd=cwd,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        output, error = await asyncio.wait_for(child.communicate(input), 120)
        assert child.returncode == 0, error.decode()
        return output
    finally:
        if child.returncode is None:
            child.kill()
            await child.wait()


async def test_go_capability_reads_and_mtls_permissions(ready, tmp_path):
    source = os.getenv("QS_AI_GOVERNANCE_SOURCE")
    if not source or not shutil.which("go"):
        pytest.skip("Requires isolated QS governance checkout and Go")
    root = Path(source)
    binary = tmp_path / "eligibility"
    with tempfile.TemporaryDirectory(prefix="qs_ai_eligibility_", dir=root / "scripts") as folder:
        program = Path(folder) / "main.go"
        shutil.copyfile(Path(__file__).parents[1] / "fixtures/go_eligibility.go", program)
        await process("go", "build", "-o", str(binary), str(program), cwd=root)
    tx, scope, command, at = ready
    await MySQLPublications(tx).apply(scope, command, at)
    certificates(tmp_path)
    container = create_container(
        Settings(
            database_url=os.environ["QS_AI_TEST_MYSQL_DSN"].replace(
                "mysql://", "mysql+asyncmy://", 1
            )
        )
    )
    server = grpc.aio.server()
    rpc.add_CommandsServicer_to_server(Commands(container), server)
    ca, cert, key = [(tmp_path / f).read_bytes() for f in ("ca.pem", "ai.pem", "ai.key")]
    port = server.add_secure_port(
        "localhost:0",
        grpc.ssl_server_credentials([(key, cert)], root_certificates=ca, require_client_auth=True),
    )
    await server.start()
    try:
        session, scale, _ = bound_case()
        _, mbti, _ = mbti_case()
        for identity, evidence, expected in (
            ("qs", scale, {"status": "available"}),
            ("qs", mbti, {"status": "unavailable", "reason_code": "publication_missing"}),
            ("other", mbti, None),
        ):
            output = await process(
                str(binary),
                f"localhost:{port}",
                str(tmp_path / "ca.pem"),
                str(tmp_path / f"{identity}.pem"),
                str(tmp_path / f"{identity}.key"),
                input=json.dumps(
                    {
                        "Actor": asdict(session.actor),
                        "TesteeID": "7",
                        "AssessmentIDs": ["42"],
                        "Evidence": [asdict(item) for item in evidence.items],
                    },
                    ensure_ascii=False,
                ).encode(),
            )
            # The real QS client also emits structured lifecycle logs.
            records = [json.loads(line) for line in output.splitlines()]
            results = [record for record in records if "Code" in record]
            assert len(results) == 1
            result = results[0]
            assert result["Code"] == ("OK" if identity == "qs" else "PERMISSION_DENIED")
            assert result["Result"] == expected
    finally:
        await server.stop(0)
        await container.close()
