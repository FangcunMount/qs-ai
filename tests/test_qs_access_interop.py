"""Actual Go handler/use case; synthetic IAM relations, never production data."""

import asyncio
import os

import pytest

from qs_ai.application.interpretation.ports import AccessDenied, DependencyUnavailable
from qs_ai.domain.interpretation.model import Actor
from qs_ai.infrastructure.qs_server.access import QSAccessSource
from qs_ai.infrastructure.qs_server.report_probe import mtls_channel
from tests.integration.test_delivery import certificates


@pytest.mark.interop
async def test_go_python_current_access_and_revocation(tmp_path):
    binary = os.getenv("QS_AI_ACCESS_PROBE_BIN")
    if not binary:
        pytest.skip("Requires compiled Go accessprobe")
    certificates(tmp_path)
    state = tmp_path / "relation"
    state.write_text("allowed")
    process = await asyncio.create_subprocess_exec(
        binary,
        *(str(tmp_path / name) for name in ("ca.pem", "qs.pem", "qs.key")),
        str(state),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        port = int(await asyncio.wait_for(process.stdout.readline(), 15))
        ca, cert, key = [(tmp_path / name).read_bytes() for name in ("ca.pem", "ai.pem", "ai.key")]
        actor = Actor("1", "parent")
        async with mtls_channel(f"localhost:{port}", ca, key, cert) as channel:
            source = QSAccessSource(channel)
            await source.authorize(actor, "7", ("42",))
            state.write_text("revoked")
            with pytest.raises(AccessDenied):
                await source.authorize(actor, "7", ("42",))
            state.write_text("allowed")
            await source.authorize(actor, "7", ("42",))
            for other, testee, ids in [
                (Actor("2", "parent"), "7", ("42",)),
                (Actor("1", "other"), "7", ("42",)),
                (actor, "8", ("42",)),
                (actor, "7", ("43",)),
            ]:
                with pytest.raises(AccessDenied):
                    await source.authorize(other, testee, ids)
            state.unlink()
            with pytest.raises(DependencyUnavailable):
                await source.authorize(actor, "7", ("42",))
        # A different CA-signed workload passes TLS but cannot invoke the use case.
        async with mtls_channel(
            f"localhost:{port}",
            ca,
            (tmp_path / "other.key").read_bytes(),
            (tmp_path / "other.pem").read_bytes(),
        ) as channel:
            with pytest.raises(AccessDenied):
                await QSAccessSource(channel).authorize(actor, "7", ("42",))
    finally:
        if process.returncode is None:
            process.kill()
        await process.communicate()
