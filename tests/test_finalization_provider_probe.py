"""Real loopback/process counter protection; not business-worker crash acceptance."""

import json
import os
import stat
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path
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


async def test_semantic_http_reply_matches_frozen_obligations_without_deduplication(tmp_path):
    from qs_ai.application.interpretation.prompts import PromptMessages

    invocation = str(uuid4())
    assertions = [dict(type="semantic", scope="dimension:1", ordinal=3)]
    messages = PromptMessages(
        "Return JSON",
        "Evaluate JSON",
        "Synthetic fixture",
        json.dumps(dict(candidate_output={"synthetic": True}, assertions=assertions)),
    )
    with ProviderProbe(tmp_path / "semantic", {"generation": True}) as probe:

        async def correlate(request):
            request.headers["X-Test-Invocation"] = invocation

        async with httpx.AsyncClient(event_hooks={"request": [correlate]}) as client:
            transport = DeepSeekResponses(client, probe.endpoint, "isolated-test-token")
            for _ in range(2):
                response = await transport.generate_messages(
                    messages, route(), schema(), invocation
                )
                value = json.loads(response.raw_output)
                assert value["schema_version"] == "ai-explanation-semantic-evaluation-output/v1"
                assert value["decisions"] == [
                    dict(**assertions[0], status="passed", detail="Synthetic semantic acceptance")
                ]
        assert probe.counts() == {invocation: 2}


def test_unattributed_or_malformed_posts_cannot_disappear_from_the_counter(tmp_path):
    with ProviderProbe(tmp_path / "provider", {"answer": "synthetic"}) as probe:
        request = urllib.request.Request(probe.endpoint, b"not-json")
        with pytest.raises(urllib.error.HTTPError) as failed:
            urllib.request.urlopen(request, timeout=3)
        assert failed.value.code == 400
        assert probe.counts() == {"unattributed": 1}


def test_worker_key_export_preserves_jwk_identity_and_recipient_public_boundary(tmp_path):
    from types import SimpleNamespace

    from jwcrypto import jwk
    from reliable_messaging.transaction import Binding

    from tests.integration.rocketmq_finalization_host import inputs
    from tests.route_fixtures import legacy_routes

    keys = {k: jwk.JWK.generate(kty="EC", crv="P-256", kid=k) for k in ("ai.sign", "qs.encrypt")}
    host = SimpleNamespace(
        keys=keys,
        kit=SimpleNamespace(dsn="mysql://isolated@127.0.0.1/test"),
        store=SimpleNamespace(binding=Binding("ai.mysql", "isolated", "m3-1")),
    )
    broker = {"endpoint": "127.0.0.1:12345", "ai": {"username": "isolated", "password": "isolated"}}
    config, _ = inputs(
        host,
        broker,
        SimpleNamespace(endpoint="http://127.0.0.1:12346/responses"),
        tmp_path / "worker",
        pause=False,
        routes=legacy_routes(),
    )
    value = json.loads(config.read_text())
    signing = jwk.JWK.from_json(Path(value["signing_key"]).read_text())
    recipient = jwk.JWK.from_json(Path(value["recipient_key"]).read_text())
    assert signing.get("kid") == "ai.sign" and signing.has_private
    assert recipient.get("kid") == "qs.encrypt" and not recipient.has_private
    assert signing.thumbprint() == keys["ai.sign"].thumbprint()
    assert recipient.thumbprint() == keys["qs.encrypt"].thumbprint()
