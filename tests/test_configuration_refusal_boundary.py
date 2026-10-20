"""Planning and root binding must keep the original refusal/technical boundary."""

from unittest.mock import AsyncMock, Mock

import pytest
from jsonschema.exceptions import SchemaError

from qs_ai.application.interpretation.input import InvalidInput
from qs_ai.application.interpretation.ports import AdmissionRejected, NotFound
from qs_ai.domain.interpretation.model import RuleViolation
from qs_ai.infrastructure.persistence.mysql import interpretation
from qs_ai.infrastructure.persistence.mysql.execution_configurations import (
    ConfigurationPlanConflict,
)
from tests.test_input_binding import bound_case


@pytest.mark.parametrize(
    ("error", "code"),
    [
        (RuleViolation("configuration_unavailable"), "configuration_unavailable"),
        (RuleViolation("invalid_original_source"), "admission_input_invalid"),
        (InvalidInput("input"), "admission_input_invalid"),
        (ValueError("configuration"), "admission_configuration_invalid"),
        (NotFound(), "admission_configuration_invalid"),
        (SchemaError("schema"), "admission_configuration_invalid"),
        (KeyError("asset"), "admission_configuration_invalid"),
        (TypeError("asset"), "admission_configuration_invalid"),
    ],
    ids=["unavailable", "invalid-source", "input", "value", "missing", "schema", "key", "type"],
)
async def test_original_configuration_refusal_codes_are_preserved(monkeypatch, error, code):
    monkeypatch.setattr(interpretation, "bind_configuration", AsyncMock(side_effect=error))
    session, evidence, _ = bound_case()
    with pytest.raises(AdmissionRejected) as refused:
        await interpretation.MySQLUnitOfWork(Mock()).bind_configuration(session, evidence)
    assert refused.value.code == code


@pytest.mark.parametrize(
    "error",
    [ConnectionError("database"), ConfigurationPlanConflict("stale")],
    ids=["database", "stale-plan"],
)
async def test_technical_or_stale_configuration_is_not_converted_to_business_refusal(
    monkeypatch, error
):
    monkeypatch.setattr(interpretation, "bind_configuration", AsyncMock(side_effect=error))
    session, evidence, _ = bound_case()
    with pytest.raises(type(error)):
        await interpretation.MySQLUnitOfWork(Mock()).bind_configuration(session, evidence)
