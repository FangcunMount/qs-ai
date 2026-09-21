import pytest

from qs_ai.application.interpretation.provider import ModelResponse, ProviderFailure
from qs_ai.config import Settings
from scripts.probe_model_bindings import TARGETS, probe, probe_route


def test_only_six_declared_model_purpose_pairs():
    assert sum(len(v[2]) for v in TARGETS.values()) == 6
    for key, (_, model, purposes) in TARGETS.items():
        for purpose in purposes:
            binding, route = probe_route(key, purpose)
            assert route.model == model
            assert route.binding_id == binding.binding_id
            assert route.max_output_tokens == 1024
    with pytest.raises(ValueError):
        probe_route("zhipu/glm-5.3-flash", "semantic")


@pytest.mark.parametrize("failure", [False, True])
async def test_probe_one_attempt_no_output_body_or_quality_claim(monkeypatch, failure):
    calls = []

    async def generate(self, messages, route, schema, invocation):
        calls.append(invocation)
        if failure:
            raise ProviderFailure("provider_timeout", result_unknown=True)
        return ModelResponse(
            invocation,
            "synthetic-receipt",
            route.model,
            '{"ok":true}',
            '{"ok":true}',
            "unchanged",
            None,
            None,
            1,
        )

    monkeypatch.setattr(
        "scripts.probe_model_bindings.ModelGatewayRouter.generate_messages", generate
    )
    result = await probe("zhipu/glm-5.3", "generation", Settings())
    assert len(calls) == 1
    assert result["quality_approved"] is False
    assert result["status"] == ("failed" if failure else "succeeded")
    assert "synthetic-receipt" not in str(result)
    if failure:
        assert result["result_unknown"]
    else:
        assert result["input_tokens"] is None
