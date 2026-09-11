import asyncio
from dataclasses import replace

import pytest

from qs_ai.application.execution.generation import DurableGeneration, FrozenGeneration
from qs_ai.application.interpretation.provider import ModelResponse, ProviderFailure
from qs_ai.infrastructure.persistence.model_call_codec import JSONModelCallCodec
from tests.integration.test_interpretation import expire
from tests.integration.test_interpretation import kit as kit
from tests.test_deepseek_request import prepared, route, schema

pytestmark = pytest.mark.integration


class Gateway:
    def __init__(self, error=None):
        self.calls = 0
        self.error = error

    async def generate(self, source, model_route, output_schema, invocation_id):
        self.calls += 1
        if self.error is not None:
            raise self.error
        return ModelResponse(
            invocation_id, "response-1", model_route.model, "{}", "{}", "none", 2, 3, 4
        )


def request():
    return FrozenGeneration(prepared(), route(), schema())


async def test_recovery_reuses_response_and_original_release(kit):
    receipt = await kit.queued()
    claim = await kit.store.claim(60)
    gateway = Gateway()
    execution = DurableGeneration(kit.store, gateway, JSONModelCallCodec())
    original = request()
    result = await execution.execute(claim, original)
    await expire(kit, receipt.session_id)
    replacement = await kit.store.claim(60)
    changed = replace(original, route=replace(original.route, model="different"))
    recovered = await execution.execute(replacement, changed)
    assert recovered == result
    assert recovered.request == original
    assert gateway.calls == 1


@pytest.mark.parametrize("unknown", [False, True])
async def test_failure_recovery_does_not_redispatch_even_when_retryable(kit, unknown):
    receipt = await kit.queued()
    claim = await kit.store.claim(60)
    gateway = Gateway(ProviderFailure("provider_timeout", retryable=True, result_unknown=unknown))
    execution = DurableGeneration(kit.store, gateway, JSONModelCallCodec())
    with pytest.raises(ProviderFailure, match="provider_timeout"):
        await execution.execute(claim, request())
    await expire(kit, receipt.session_id)
    replacement = await kit.store.claim(60)
    with pytest.raises(ProviderFailure) as error:
        await execution.execute(replacement, request())
    assert error.value.result_unknown == unknown
    assert gateway.calls == 1


async def test_cancellation_after_dispatch_is_unknown_on_recovery(kit):
    receipt = await kit.queued()
    claim = await kit.store.claim(60)
    gateway = Gateway(asyncio.CancelledError())
    execution = DurableGeneration(kit.store, gateway, JSONModelCallCodec())
    with pytest.raises(asyncio.CancelledError):
        await execution.execute(claim, request())
    await expire(kit, receipt.session_id)
    replacement = await kit.store.claim(60)
    with pytest.raises(ProviderFailure, match="provider_result_unknown"):
        await execution.execute(replacement, request())
    assert gateway.calls == 1


async def test_response_commit_failure_does_not_authorize_another_send(kit, monkeypatch):
    receipt = await kit.queued()
    claim = await kit.store.claim(60)
    gateway = Gateway()
    execution = DurableGeneration(kit.store, gateway, JSONModelCallCodec())

    async def unavailable(*args, **kwargs):
        raise ConnectionError("Database unavailable")

    with monkeypatch.context() as patch:
        patch.setattr(kit.store, "record_model_response", unavailable)
        with pytest.raises(ConnectionError):
            await execution.execute(claim, request())
    await expire(kit, receipt.session_id)
    replacement = await kit.store.claim(60)
    with pytest.raises(ProviderFailure, match="provider_result_unknown"):
        await execution.execute(replacement, request())
    assert gateway.calls == 1


@pytest.mark.parametrize("boundary", ["dispatched", "response_received"])
async def test_process_kill_recovers_durable_call_without_another_send(kit, boundary):
    import os
    import sys

    receipt = await kit.queued()
    child = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "tests.probes.model_crash",
        receipt.session_id,
        boundary,
        env={**os.environ, "QS_AI_TEST_MYSQL_DSN": kit.dsn},
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )
    try:
        assert await asyncio.wait_for(child.stdout.readline(), 20) == f"{boundary}\n".encode()
        # Abrupt OS termination, not a coroutine exception or graceful cleanup.
        child.kill()
        assert await asyncio.wait_for(child.wait(), 10) != 0
        await expire(kit, receipt.session_id)
        replacement = await kit.store.claim(60)
        assert replacement.session.id == receipt.session_id
        gateway = Gateway()
        execution = DurableGeneration(kit.store, gateway, JSONModelCallCodec())
        if boundary == "dispatched":
            with pytest.raises(ProviderFailure, match="provider_result_unknown"):
                await execution.execute(replacement, request())
        else:
            result = await execution.execute(replacement, request())
            assert result.response.request_id == "response-1"
            assert result.request == request()
        assert gateway.calls == 0
    finally:
        if child.returncode is None:
            child.kill()
        await asyncio.wait_for(child.wait(), 10)
