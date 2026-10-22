"""Real loopback/process counter protection; not business-worker crash acceptance."""

import json
import os
import stat
import subprocess
import sys
import urllib.error
import urllib.request
from uuid import uuid4

import httpx
import pytest

from qs_ai.infrastructure.qs_server.responses import DeepSeekResponses
from tests.probes.finalization_provider import ProviderProbe
from tests.test_deepseek_request import prepared, route, schema


def post(probe, invocation):
    request = urllib.request.Request(
        probe.endpoint,
        json.dumps({"model": "synthetic"}).encode(),
        headers={"Content-Type": "application/json", "X-Test-Invocation": invocation},
    )
    with urllib.request.urlopen(request, timeout=3) as response:
        return json.loads(response.read())


def test_http_ledger_never_deduplicates_repeated_invocation_and_excludes_body(tmp_path):
    invocation = str(uuid4())
    with ProviderProbe(tmp_path / "provider", {"private-output": "synthetic-only"}) as probe:
        assert probe.process.pid != os.getpid()
        first, second = post(probe, invocation), post(probe, invocation)
        assert first["id"] != second["id"]
        assert probe.counts() == {invocation: 2}
        assert "synthetic-only" not in probe.ledger.read_bytes().decode(errors="ignore")
        assert stat.S_IMODE(probe.ledger.stat().st_mode) == 0o600
    assert probe.process is None


def test_http_attempt_survives_calling_process_sigkill_and_provider_restart(tmp_path):
    invocation = str(uuid4())
    probe = ProviderProbe(tmp_path / "provider", {"answer": "synthetic"})
    client = None
    with probe:
        code = """import json,sys,urllib.request
r=urllib.request.Request(sys.argv[1],json.dumps({'model':'synthetic'}).encode(),
 headers={'Content-Type':'application/json','X-Test-Invocation':sys.argv[2]})
with urllib.request.urlopen(r,timeout=3) as response:
 assert response.status==200
print('response_received',flush=True)
sys.stdin.read()
"""
        try:
            client = subprocess.Popen(
                [sys.executable, "-c", code, probe.endpoint, invocation],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
            )
            assert client.stdout.readline().strip() == "response_received"
            client.kill()
            client.wait(timeout=3)
            assert probe.counts() == {invocation: 1}
        finally:
            if client is not None:
                if client.poll() is None:
                    client.kill()
                    client.wait(timeout=3)
                client.stdin.close()
                client.stdout.close()
    with probe:
        assert probe.counts() == {invocation: 1}
        post(probe, invocation)
        assert probe.counts() == {invocation: 2}


async def test_original_deepseek_adapter_uses_real_http_and_external_invocation_counter(tmp_path):
    invocation = str(uuid4())
    output = {"answer": "synthetic response"}
    with ProviderProbe(tmp_path / "provider", output) as probe:

        async def correlate(request):
            # Test-only correlation, not a provider idempotency or retrieval key.
            request.headers["X-Test-Invocation"] = invocation

        async with httpx.AsyncClient(event_hooks={"request": [correlate]}) as client:
            actual = await DeepSeekResponses(
                client, probe.endpoint, "isolated-test-token"
            ).generate(prepared(), route(), schema(), invocation)
        assert actual.invocation_id == invocation
        assert json.loads(actual.raw_output) == output
        assert actual.request_id == "synthetic-response-1"
        assert probe.counts() == {invocation: 1}


def test_unattributed_or_malformed_posts_cannot_disappear_from_the_counter(tmp_path):
    with ProviderProbe(tmp_path / "provider", {"answer": "synthetic"}) as probe:
        request = urllib.request.Request(probe.endpoint, b"not-json")
        with pytest.raises(urllib.error.HTTPError) as failed:
            urllib.request.urlopen(request, timeout=3)
        assert failed.value.code == 400
        assert probe.counts() == {"unattributed": 1}
