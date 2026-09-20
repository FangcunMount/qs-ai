from dataclasses import asdict, replace

import pytest

from qs_ai.application.evaluation.capacity import EvaluationCapacityPolicy
from qs_ai.application.execution.capacity import ParticipantCapacityPolicy
from qs_ai.application.governance.quotas import QuotaBaseline, QuotaValues


def values():
    return QuotaValues(ParticipantCapacityPolicy(), EvaluationCapacityPolicy())


def test_default_resolution_and_organization_limits():
    baseline = QuotaBaseline(values(), values())
    assert baseline.resolve(None, 0)["source"] == "deployment_default"
    configured = replace(values(), participant=replace(values().participant, daily_org=100))
    result = baseline.resolve(configured, 1)
    assert result["effective"]["participant"]["daily_org"] == 100
    assert result["configured"] == asdict(configured)
    assert result["constrained_fields"] == []


def test_lowered_deployment_ceiling_clamps_without_rewriting_saved_values():
    ceiling = replace(values(), evaluation=EvaluationCapacityPolicy(200, 1))
    result = QuotaBaseline(ceiling, ceiling).resolve(values(), 7)
    assert result["effective"]["evaluation"]["daily_provider_calls"] == 200
    assert result["configured"]["evaluation"]["daily_provider_calls"] == 1024
    assert result["revision"] == 7
    assert result["constrained_fields"] == ["evaluation.daily_provider_calls"]
    with pytest.raises(ValueError):
        values().validate_write(ceiling)


@pytest.mark.parametrize("invalid", [True, 0, -1, 1.5, "1", None])
def test_quota_input_rejects_non_positive_integers(invalid):
    raw = asdict(values())
    raw["participant"]["daily_org"] = invalid
    with pytest.raises(ValueError):
        QuotaValues.parse(raw)


def test_missing_unknown_fields_and_corrupt_pointer_do_not_fall_back():
    raw = asdict(values())
    del raw["participant"]["daily_org"]
    with pytest.raises(ValueError):
        QuotaValues.parse(raw)
    raw = asdict(values())
    raw["evaluation"]["secret"] = "not-a-setting"
    with pytest.raises(ValueError):
        QuotaValues.parse(raw)
    baseline = QuotaBaseline(values(), values())
    with pytest.raises(ValueError):
        baseline.resolve(None, 2)
    with pytest.raises(ValueError):
        baseline.resolve(values(), 0)


def test_deployment_defaults_cannot_exceed_explicit_ceiling():
    from qs_ai.config import Settings

    ceilings = asdict(values())
    ceilings["participant"]["daily_org"] = 1
    with pytest.raises(ValueError, match="defaults exceed"):
        Settings(quota_ceilings=ceilings)
    settings = Settings(quota_ceilings=asdict(values()))
    assert settings.quota_ceilings.participant.daily_org == 500
