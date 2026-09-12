"""Resolve the generation half of an evaluation identity against immutable assets."""

from qs_ai.application.interpretation.manifest import ManifestUnavailable, build_generation_manifest
from qs_ai.application.interpretation.profile_assets import ProfileReader
from qs_ai.application.interpretation.prompt_assets import PromptReader
from qs_ai.application.interpretation.route_assets import RouteReader
from qs_ai.application.interpretation.schema_assets import SchemaReader
from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity, FrozenContractRef
from qs_ai.domain.governance.manifest import GenerationManifest
from qs_ai.domain.governance.route import RouteAsset


async def resolve_generation_assets(
    release: EvidenceReleaseIdentity,
    profiles: ProfileReader,
    prompts: PromptReader,
    routes: RouteReader,
    schemas: SchemaReader,
) -> GenerationManifest:
    manifest = await build_generation_manifest(
        profiles,
        prompts,
        routes,
        schemas,
        profile_id=release.profile.id,
        profile_version=release.profile.version,
        route_revision=release.generation_route.version,
    )
    validate_generation_manifest(release, manifest)
    return manifest


def validate_generation_manifest(
    release: EvidenceReleaseIdentity, manifest: GenerationManifest
) -> None:
    for name in ("profile", "prompt", "generation_route", "input_schema", "output_schema"):
        asset = getattr(manifest, name)
        # QS evaluation uses full schema versions; asset storage uses the suffix.
        version = f"{asset.identity}/{asset.version}" if name.endswith("schema") else asset.version
        expected = FrozenContractRef(asset.identity, version, asset.fingerprint)
        if getattr(release, name) != expected:
            raise ManifestUnavailable(f"Evaluation {name} reference does not match frozen asset")


async def resolve_semantic_route(
    release: EvidenceReleaseIdentity, routes: RouteReader
) -> RouteAsset:
    """Resolve the judge's own reference, even when both stages intentionally share a model."""
    ref = release.semantic_route
    route = await routes.get(ref.id, ref.version)
    if route is None or (route.route, route.revision, route.fingerprint) != (
        ref.id,
        ref.version,
        ref.fingerprint,
    ):
        raise ManifestUnavailable("Evaluation semantic route missing or mismatched")
    return route
