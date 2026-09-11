import hashlib
import json
from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from qs_ai.application.evaluation.completion import validate_generation_completion
from qs_ai.application.interpretation.manifest import ManifestUnavailable
from qs_ai.bootstrap.import_routes import baseline_assets as routes_baseline
from qs_ai.bootstrap.import_schemas import baseline_assets as schemas_baseline
from qs_ai.domain.evaluation.failure import ClassifiedFailure
from qs_ai.domain.evaluation.identity import FrozenContractRef
from tests.test_evaluation_identity import identity
from tests.test_generation_completion import completion
from tests.test_output_validation import candidate


def assets():
    route = routes_baseline()[1][0]
    schema = schemas_baseline()[1][1]
    release = replace(
        identity(),
        generation_route=FrozenContractRef(route.route, route.revision, route.fingerprint),
        output_schema=FrozenContractRef(
            schema.schema_id, schema.schema_id + "/" + schema.version, schema.fingerprint
        ),
    )
    definition = json.loads(route.definition_json)
    value = completion()
    raw = json.dumps(candidate(), ensure_ascii=False).encode()
    value = replace(
        value,
        receipt=replace(value.receipt, provider=definition["provider"], model=definition["model"]),
        raw_output=raw,
        normalized_output=raw,
        normalized_fingerprint="sha256:" + hashlib.sha256(raw).hexdigest(),
    )
    return (
        release,
        value,
        AsyncMock(get=AsyncMock(return_value=route)),
        AsyncMock(get=AsyncMock(return_value=schema)),
    )


@pytest.mark.asyncio
async def test_completion_uses_exact_frozen_route_and_original_output_schema():
    release, value, routes, schemas = assets()
    await validate_generation_completion(release, value, routes, schemas)
    routes.get.assert_awaited_once_with(
        release.generation_route.id, release.generation_route.version
    )
    schemas.get.assert_awaited_once_with("ai-explanation-output", "v1")


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["provider", "model"])
async def test_wrong_provider_or_model_is_rejected(field):
    release, value, routes, schemas = assets()
    value = replace(value, receipt=replace(value.receipt, **{field: "other"}))
    with pytest.raises(ValueError, match="receipt"):
        await validate_generation_completion(release, value, routes, schemas)


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["generation_route", "output_schema"])
async def test_asset_drift_and_missing_assets_are_rejected(field):
    release, value, routes, schemas = assets()
    changed = replace(
        release, **{field: replace(getattr(release, field), fingerprint="sha256:" + "0" * 64)}
    )
    with pytest.raises(ManifestUnavailable):
        await validate_generation_completion(changed, value, routes, schemas)
    store = routes if field == "generation_route" else schemas
    store.get.return_value = None
    with pytest.raises(ManifestUnavailable):
        await validate_generation_completion(release, value, routes, schemas)


@pytest.mark.asyncio
async def test_invalid_structure_is_failure_evidence_but_cannot_be_success():
    release, value, routes, schemas = assets()
    raw = b'{"private_invalid_field":"must-not-appear-in-error"}'
    value = replace(
        value,
        normalized_output=raw,
        normalized_fingerprint="sha256:" + hashlib.sha256(raw).hexdigest(),
    )
    with pytest.raises(ValueError, match="violates frozen schema") as exc:
        await validate_generation_completion(release, value, routes, schemas)
    assert "private_invalid_field" not in str(exc.value)
    failure = ClassifiedFailure(
        "output_validation",
        "output_contract_conformance",
        "invalid_output",
        False,
        False,
        "replace_generation",
        "Invalid output",
        ("execution:1",),
    )
    value = replace(value, status="failed", failure=failure)
    schemas.reset_mock()
    await validate_generation_completion(release, value, routes, schemas)
    schemas.get.assert_not_awaited()
    with pytest.raises(ValueError, match="receipt"):
        await validate_generation_completion(
            release, replace(value, receipt=replace(value.receipt, model="other")), routes, schemas
        )
