"""Resolve explicit immutable versions without choosing latest or changing publication state."""

import hashlib
import json

from qs_ai.application.interpretation.profile_assets import ProfileAssets
from qs_ai.application.interpretation.prompt_assets import PromptAssets
from qs_ai.application.interpretation.route_assets import RouteAssets
from qs_ai.application.interpretation.schema_assets import SchemaAssets
from qs_ai.domain.governance.manifest import AssetReference, GenerationManifest


class ManifestUnavailable(ValueError):
    pass


async def build_generation_manifest(
    profiles: ProfileAssets,
    prompts: PromptAssets,
    routes: RouteAssets,
    schemas: SchemaAssets,
    *,
    profile_id: str,
    profile_version: str,
    route_revision: str,
) -> GenerationManifest:
    profile = await profiles.get(profile_id, profile_version)
    if profile is None or (profile.profile_id, profile.version) != (profile_id, profile_version):
        raise ManifestUnavailable("Profile asset missing or mismatched")
    policy = json.loads(profile.definition_json)["generation_policy"]
    template, version = policy["prompt_template_id"], policy["prompt_version"]
    prompt = await prompts.get(template, version)
    if prompt is None or (prompt.template_id, prompt.version) != (template, version):
        raise ManifestUnavailable("Prompt asset missing or mismatched")
    route_name = policy["provider_route"]
    route = await routes.get(route_name, route_revision)
    if route is None or (route.route, route.revision) != (route_name, route_revision):
        raise ManifestUnavailable("Route asset missing or mismatched")
    refs = []
    for field, expected_id in (
        ("input_schema_version", "ai-explanation-input"),
        ("output_schema_version", "ai-explanation-output"),
    ):
        identity, separator, schema_version = policy[field].rpartition("/")
        if not separator or identity != expected_id:
            raise ManifestUnavailable("Unsupported schema reference")
        schema = await schemas.get(identity, schema_version)
        if schema is None or (schema.schema_id, schema.version) != (identity, schema_version):
            raise ManifestUnavailable("Schema asset missing or mismatched")
        refs.append(
            AssetReference(
                identity,
                schema_version,
                schema.fingerprint,
                hashlib.sha256(schema.definition_json.encode()).hexdigest(),
            )
        )
    return GenerationManifest(
        AssetReference(
            profile_id,
            profile_version,
            profile.fingerprint,
            hashlib.sha256(profile.definition_json.encode()).hexdigest(),
        ),
        AssetReference(template, version, prompt.fingerprint, prompt.package_sha256),
        AssetReference(
            route_name,
            route_revision,
            route.fingerprint,
            hashlib.sha256(route.definition_json.encode()).hexdigest(),
        ),
        refs[0],
        refs[1],
    )
