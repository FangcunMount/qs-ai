"""Resolve every original release component before creating an evaluation Run."""

import asyncio
import json

from qs_ai.application.evaluation.release import resolve_generation_assets, resolve_semantic_route
from qs_ai.application.interpretation.manifest import ManifestUnavailable
from qs_ai.application.interpretation.profile_assets import ProfileReader
from qs_ai.application.interpretation.prompt_assets import PromptReader
from qs_ai.application.interpretation.route_assets import RouteReader
from qs_ai.application.interpretation.schema_assets import SchemaReader
from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity
from qs_ai.domain.governance.manifest import GenerationManifest
from qs_ai.infrastructure.qs_server.evaluation_input import validate_suite_inputs
from qs_ai.infrastructure.qs_server.evaluation_policies import (
    load_execution_policy,
    load_gate_policy,
)
from qs_ai.infrastructure.qs_server.evaluation_suite import FrozenSuite, resolve_suite, suite_prompt
from qs_ai.infrastructure.qs_server.semantic_assets import load_semantic_assets


async def validate_release_assets(
    release: EvidenceReleaseIdentity,
    profiles: ProfileReader,
    prompts: PromptReader,
    routes: RouteReader,
    schemas: SchemaReader,
    *,
    frozen_suite: FrozenSuite | None = None,
) -> GenerationManifest:
    suite = await asyncio.to_thread(resolve_suite, release.suite, frozen_suite)
    (await asyncio.to_thread(validate_suite_inputs, suite, release.input_schema))
    execution, gate = (
        (await asyncio.to_thread(load_execution_policy)),
        (await asyncio.to_thread(load_gate_policy)),
    )
    release.validate_frozen_policies(execution.definition_json, gate.definition_json)
    semantic = await asyncio.to_thread(load_semantic_assets)
    if (release.semantic_prompt, release.semantic_output_schema) != (
        semantic.prompt,
        semantic.output_schema,
    ):
        raise ManifestUnavailable("Semantic prompt or schema does not match frozen release")
    manifest = await resolve_generation_assets(release, profiles, prompts, routes, schemas)
    await resolve_semantic_route(release, routes)
    if suite.manifest is not None and suite.manifest != manifest:
        raise ManifestUnavailable("Registered suite manifest differs from release")
    definition = json.loads(suite.definition_json)
    fixture = definition["profile_fixture"]
    if (manifest.profile.identity, manifest.profile.version, manifest.profile.fingerprint) != (
        fixture["profile_id"],
        fixture["version"],
        fixture["fingerprint"],
    ) or (manifest.prompt.identity, manifest.prompt.version) != suite_prompt(suite):
        raise ManifestUnavailable("Suite fixture does not match generation assets")
    return manifest
