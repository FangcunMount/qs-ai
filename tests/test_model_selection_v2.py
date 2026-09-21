from dataclasses import replace

import pytest

from qs_ai.application.governance.solution_models import edited_route, selection
from qs_ai.application.interpretation.provider import ModelRoute
from qs_ai.infrastructure.persistence.mysql.solutions import MySQLSolutions
from qs_ai.model_configuration import ModelCapability
from tests.test_model_route_v2 import route
from tests.test_multi_provider_gateway import settings


def configuration():
    return settings().models.model_copy(
        update={
            "v2_writes_enabled": True,
            "catalog": (
                ModelCapability(
                    model_key="zhipu/quality",
                    model_id="glm-5.3",
                    catalog_revision="20260921-1",
                    binding_id="zhipu-official",
                    binding_revision="v1",
                    purposes=("generation", "semantic"),
                    verified=True,
                    evidence_ref="test-only",
                    reasoning_efforts=("low", "high"),
                ),
            ),
        }
    )


def test_selection_freezes_binding_and_roundtrips():
    source = route()
    result = edited_route(source, selection(source), "new", (), configuration(), "semantic")
    assert result == replace(source, revision="new")
    assert result.fingerprint() != source.fingerprint()


@pytest.mark.parametrize(
    "changes,reason",
    [
        ({"catalog_revision": "stale"}, "model_capability_changed"),
        ({"model": "other"}, "model_identity_mismatch"),
        ({"reasoning_effort": "max"}, "model_parameter_invalid"),
    ],
)
def test_invalid_selection_rejected_before_freezing(changes, reason):
    with pytest.raises(ValueError, match=reason):
        edited_route(route(), replace(selection(route()), **changes), "new", (), configuration())


def test_disabled_write_and_unverified_model_rejected():
    config = configuration()
    for changed in (
        config.model_copy(update={"v2_writes_enabled": False}),
        config.model_copy(
            update={"catalog": (config.catalog[0].model_copy(update={"verified": False}),)}
        ),
    ):
        with pytest.raises(ValueError):
            edited_route(route(), selection(route()), "new", (), changed)


def test_generation_only_model_cannot_be_used_as_judge():
    config = configuration()
    entry = config.catalog[0].model_copy(update={"purposes": ("generation",)})
    with pytest.raises(ValueError, match="model_not_enabled"):
        edited_route(
            route(),
            selection(route()),
            "new",
            (),
            config.model_copy(update={"catalog": (entry,)}),
            "semantic",
        )


def test_catalog_is_redacted_and_has_availability():
    value = settings().model_copy(update={"models": configuration()})
    result = MySQLSolutions(None, value).capabilities()
    assert result["catalog"][0]["available"]
    assert "synthetic-zhipu" not in str(result)
    assert "open.bigmodel.cn" not in str(result)
    value = value.model_copy(update={"zhipu_api_key": None})
    assert (
        MySQLSolutions(None, value).capabilities()["catalog"][0]["unavailable_reason"]
        == "credential_unconfigured"
    )


def test_v1_selection_stays_v1():
    source = ModelRoute(
        "route",
        "v1",
        "deepseek",
        "deepseek-v4-pro",
        "responses",
        "json_schema",
        120000,
        12000,
        "none",
    )
    assert (
        type(edited_route(source, selection(source), "v2", (source.model,), configuration()))
        is ModelRoute
    )


def test_new_admission_rejects_removed_catalog_without_mutating_frozen_route():
    from qs_ai.application.governance.solution_models import validate_v2_admission
    from qs_ai.model_configuration import ModelConfiguration

    frozen = route()
    original = frozen.definition_json()
    validate_v2_admission(frozen, configuration(), "generation")
    with pytest.raises(ValueError, match="model_not_enabled"):
        validate_v2_admission(frozen, ModelConfiguration(), "generation")
    assert frozen.definition_json() == original


@pytest.mark.parametrize(
    "changes",
    [
        {"thinking": "disabled", "reasoning_effort": "low"},
        {"temperature": 0.5},
        {"top_p": 0.9},
    ],
)
def test_catalog_rejects_unverified_parameter_modes_on_save_and_admission(changes):
    from qs_ai.application.governance.solution_models import validate_v2_admission

    with pytest.raises(ValueError):
        edited_route(route(), replace(selection(route()), **changes), "next", (), configuration())
    # Route-level validity alone is not model-specific capability approval.
    if "thinking" not in changes:
        frozen = replace(route(), **changes)
        with pytest.raises(ValueError, match="model_parameter_invalid"):
            validate_v2_admission(frozen, configuration(), "generation")
