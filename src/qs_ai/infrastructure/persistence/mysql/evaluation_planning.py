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
from qs_ai.infrastructure.persistence.mysql.evaluation_contracts import evaluation_contracts
from qs_ai.infrastructure.persistence.mysql.evaluation_suites import load_registered_suite
from qs_ai.infrastructure.persistence.mysql.schema import (
    profile_assets,
    prompt_assets,
    route_assets,
    schema_assets,
)
from qs_ai.infrastructure.persistence.mysql.suite_contracts import read as read_suite_contracts
from qs_ai.infrastructure.qs_server.evaluation_release import validate_release_assets


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
            suite = await load_registered_suite(
                db, query.suite, organization_id=query.scope.organization_id
            )
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
            refs = await read_suite_contracts(db, query.suite, query.scope.organization_id)
            release = EvidenceReleaseIdentity(
                suite=query.suite,
                prompt=evaluation_ref(manifest.prompt),
                profile=evaluation_ref(manifest.profile),
                input_schema=evaluation_ref(manifest.input_schema, schema=True),
                output_schema=evaluation_ref(manifest.output_schema, schema=True),
                generation_route=query.generation_route,
                semantic_prompt=refs.semantic_prompt,
                semantic_output_schema=refs.semantic_output_schema,
                semantic_route=query.semantic_route,
                execution_policy=refs.execution_policy,
                gate_policy=refs.gate_policy,
            )
            contracts = await evaluation_contracts(
                db,
                release,
                query.scope.organization_id,
                semantic_owner_organization_id=refs.semantic_owner_organization_id,
            )
            execution, gate, semantic = contracts.execution, contracts.gate, contracts.semantic
            await validate_release_assets(
                release,
                profiles,
                prompts,
                routes,
                schemas,
                frozen_suite=suite,
                execution_policy_json=execution.definition_json,
                gate_policy_json=gate.definition_json,
                semantic=semantic,
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
