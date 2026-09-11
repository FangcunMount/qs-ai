import pytest

from qs_ai.application.evaluation.provider_failure import classify_provider_failure
from qs_ai.application.interpretation.provider import ProviderFailure
from qs_ai.infrastructure.qs_server.evaluation_policies import load_execution_policy


@pytest.mark.parametrize("stage", ["generation", "semantic"])
def test_rate_limit_uses_stage_specific_allowed_selector(stage):
    failure = classify_provider_failure(
        stage, "execution:1", ProviderFailure("provider_rate_limited", retryable=True)
    )
    policy = load_execution_policy()
    allowed = (
        policy.allows_automatic_generation_recovery
        if stage == "generation"
        else policy.allows_automatic_semantic_recovery
    )
    assert allowed(failure)
    assert failure.code == (
        "provider_rate_limited" if stage == "generation" else "semantic_provider_rate_limited"
    )


@pytest.mark.parametrize("stage", ["generation", "semantic"])
def test_unknown_always_overrides_retryable_transport_flag(stage):
    failure = classify_provider_failure(
        stage,
        "execution:1",
        ProviderFailure("provider_timeout", retryable=True, result_unknown=True),
    )
    assert failure.result_unknown and not failure.retryable
    assert failure.disposition == "manual_acknowledgement"
    policy = load_execution_policy()
    assert not policy.allows_automatic_generation_recovery(failure)
    assert not policy.allows_automatic_semantic_recovery(failure)
    assert failure.provider_diagnostics.code == "provider_timeout"


@pytest.mark.parametrize(
    "code",
    [
        "provider_authentication_failed",
        "provider_connect_failed",
        "provider_output_cardinality_invalid",
    ],
)
def test_generic_errors_do_not_gain_automatic_retry_without_policy_evidence(code):
    policy = load_execution_policy()
    for stage in ("generation", "semantic"):
        failure = classify_provider_failure(
            stage, "execution:1", ProviderFailure(code, retryable=True)
        )
        allowed = (
            policy.allows_automatic_generation_recovery
            if stage == "generation"
            else policy.allows_automatic_semantic_recovery
        )
        assert not allowed(failure)


def test_arbitrary_error_text_is_not_copied_into_evidence():
    failure = classify_provider_failure(
        "semantic", "execution:1", ProviderFailure("<secret provider body>")
    )
    assert "secret" not in str(failure)
    assert failure.provider_diagnostics.code == "provider_execution_failed"
