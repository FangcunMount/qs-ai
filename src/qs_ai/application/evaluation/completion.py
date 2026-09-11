"""Validate terminal evidence against the Run's immutable generation assets."""

import json

from jsonschema import Draft202012Validator

from qs_ai.application.interpretation.manifest import ManifestUnavailable
from qs_ai.application.interpretation.route_assets import RouteAssets
from qs_ai.application.interpretation.schema_assets import SchemaAssets
from qs_ai.domain.evaluation.completion import GenerationCompletion
from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity


async def validate_generation_completion(
    release: EvidenceReleaseIdentity,
    completion: GenerationCompletion,
    routes: RouteAssets,
    schemas: SchemaAssets,
) -> None:
    """Check asset binding and structure, not factual correctness or Run acceptance."""
    ref = release.generation_route
    route = await routes.get(ref.id, ref.version)
    if route is None or (route.route, route.revision, route.fingerprint) != (
        ref.id,
        ref.version,
        ref.fingerprint,
    ):
        raise ManifestUnavailable("Evaluation generation route missing or mismatched")
    definition = json.loads(route.definition_json)
    if completion.receipt is not None and (
        completion.receipt.provider,
        completion.receipt.model,
    ) != (definition["provider"], definition["model"]):
        raise ValueError("Provider receipt does not match frozen generation route")
    # Invalid output is legitimate evidence of a failed call; never promote it to success.
    if completion.status != "succeeded":
        return
    ref = release.output_schema
    prefix = ref.id + "/"
    if not ref.version.startswith(prefix):
        raise ManifestUnavailable("Evaluation output schema version is not fully qualified")
    schema = await schemas.get(ref.id, ref.version.removeprefix(prefix))
    if schema is None or (
        schema.schema_id,
        schema.schema_id + "/" + schema.version,
        schema.fingerprint,
    ) != (ref.id, ref.version, ref.fingerprint):
        raise ManifestUnavailable("Evaluation output schema missing or mismatched")
    definition = json.loads(schema.definition_json)
    Draft202012Validator.check_schema(definition)
    validator = Draft202012Validator(definition)
    if not validator.is_valid(json.loads(completion.normalized_output)):
        # Do not leak provider output through jsonschema's detailed exception text.
        raise ValueError("Successful generation output violates frozen schema")
