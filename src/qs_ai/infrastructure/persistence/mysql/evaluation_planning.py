"""Read immutable suites and assets from one snapshot; never reserve calls or create Runs."""

import json

from qs_ai.application.evaluation.planning import EvaluationPlan, EvaluationPlanQuery
from qs_ai.application.interpretation.manifest import build_generation_manifest
from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity, FrozenContractRef
from qs_ai.domain.governance.manifest import AssetReference
from qs_ai.domain.governance.profile import ProfileAsset
from qs_ai.domain.governance.prompt import PromptAsset
from qs_ai.domain.governance.route import RouteAsset
from qs_ai.domain.governance.schema import SchemaAsset
from qs_ai.infrastructure.persistence.mysql.asset_snapshot import AssetSnapshotReader
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_suites import load_registered_suite
from qs_ai.infrastructure.persistence.mysql.schema import (
    profile_assets,
    prompt_assets,
    route_assets,
    schema_assets,
)
from qs_ai.infrastructure.qs_server.evaluation_policies import (
    load_execution_policy,
    load_gate_policy,
)
from qs_ai.infrastructure.qs_server.evaluation_release import validate_release_assets
from qs_ai.infrastructure.qs_server.semantic_assets import load_semantic_assets


def evaluation_ref(asset: AssetReference, *, schema: bool = False) -> FrozenContractRef:
    version = f"{asset.identity}/{asset.version}" if schema else asset.version
    return FrozenContractRef(asset.identity, version, asset.fingerprint)


class MySQLEvaluationPlanner:
    def __init__(self, transactions: Transactions) -> None:
        self.transactions = transactions

    async def prepare(self, query: EvaluationPlanQuery) -> EvaluationPlan:
        # Assets, like the catalog, are shared; QS establishes the actor's audit permission.
        async with self.transactions.open() as db:
            await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            suite = await load_registered_suite(db, query.suite)
            fixture = json.loads(suite.definition_json)["profile_fixture"]
            profiles = AssetSnapshotReader(db, profile_assets, ProfileAsset)
            prompts = AssetSnapshotReader(db, prompt_assets, PromptAsset)
            routes = AssetSnapshotReader(db, route_assets, RouteAsset)
            schemas = AssetSnapshotReader(db, schema_assets, SchemaAsset)
            manifest = await build_generation_manifest(
                profiles,
                prompts,
                routes,
                schemas,
                profile_id=fixture["profile_id"],
                profile_version=fixture["version"],
                route_revision=query.generation_route.version,
            )
            if evaluation_ref(manifest.generation_route) != query.generation_route:
                raise ValueError("Selected generation route does not match suite Profile")
            execution, gate, semantic = (
                load_execution_policy(),
                load_gate_policy(),
                load_semantic_assets(),
            )
            release = EvidenceReleaseIdentity(
                suite=query.suite,
                prompt=evaluation_ref(manifest.prompt),
                profile=evaluation_ref(manifest.profile),
                input_schema=evaluation_ref(manifest.input_schema, schema=True),
                output_schema=evaluation_ref(manifest.output_schema, schema=True),
                generation_route=query.generation_route,
                semantic_prompt=semantic.prompt,
                semantic_output_schema=semantic.output_schema,
                semantic_route=query.semantic_route,
                execution_policy=FrozenContractRef(
                    execution.policy_id, execution.version, execution.fingerprint
                ),
                gate_policy=gate.reference,
            )
            await validate_release_assets(
                release, profiles, prompts, routes, schemas, frozen_suite=suite
            )
            if (len(suite.generation_case_ids), suite.repetitions, execution.preflight_cases) != (
                execution.generation_cases,
                execution.candidates_per_case,
                1,
            ):
                raise ValueError("Suite and execution policy disagree")
            return EvaluationPlan(
                release,
                release.fingerprint(),
                execution.generation_cases,
                execution.candidates_per_case,
                len(suite.slots()),
                execution.preflight_cases,
                execution.generation_per_run,
                execution.semantic_per_run,
                execution.definition_json,
                gate.definition_json,
            )
