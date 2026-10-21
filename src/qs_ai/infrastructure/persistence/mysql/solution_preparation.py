"""Read the original asset rules and freeze a complete future preparation without DML."""

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.governance.profile_registration import RegisterProfile
from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.governance.prompt_freeze import FreezePromptDraft
from qs_ai.application.governance.suite_registration import RegisterSuite
from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity
from qs_ai.domain.governance.profile import AssetConflict, ProfileAsset
from qs_ai.domain.governance.prompt import PromptAsset
from qs_ai.domain.governance.route import RouteAsset
from qs_ai.domain.governance.schema import SchemaAsset
from qs_ai.infrastructure.persistence.mysql.asset_records import (
    profile_assets,
    prompt_assets,
    route_assets,
    schema_assets,
)
from qs_ai.infrastructure.persistence.mysql.asset_snapshot import (
    AssetSnapshotReader,
    PlannedAssetReader,
    generation_snapshot,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_contracts import (
    EvaluationContracts,
    contracts_from_binding,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_runs import (
    RunCreationPlan,
    freeze_run_creation,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_suites import SuiteRegistrationPlan
from qs_ai.infrastructure.persistence.mysql.evaluation_suites import plan_registration as plan_suite
from qs_ai.infrastructure.persistence.mysql.profile_registrations import ProfileRegistrationPlan
from qs_ai.infrastructure.persistence.mysql.profile_registrations import (
    plan_registration as plan_profile,
)
from qs_ai.infrastructure.persistence.mysql.prompt_freezes import PromptFreezePlan, plan_freeze
from qs_ai.infrastructure.persistence.mysql.solution_assets import (
    PreparationIDs,
    freeze_route,
    prepared_release,
    prepared_result,
    route_for,
    route_reference,
    selected_release,
)
from qs_ai.infrastructure.qs_server.evaluation_release import validate_release_assets
from qs_ai.model_configuration import ModelConfiguration


@dataclass(frozen=True)
class PreparedAssetPlan:
    identities: PreparationIDs
    release: EvidenceReleaseIdentity
    first_at: datetime
    prompt: PromptFreezePlan = field(repr=False)
    generation: RouteAsset = field(repr=False)
    semantic: RouteAsset = field(repr=False)
    profile: ProfileRegistrationPlan = field(repr=False)
    suite: SuiteRegistrationPlan = field(repr=False)
    contracts: EvaluationContracts = field(repr=False)
    creation: RunCreationPlan = field(repr=False)
    prepared_json: str = field(repr=False)

    def prepared(self) -> dict[str, Any]:
        value = json.loads(self.prepared_json)
        if not isinstance(value, dict):
            raise ValueError("Original prepared result object required")
        return value

    @property
    def digest(self) -> bytes:
        from qs_ai.infrastructure.persistence.mysql.evaluation_suites import (
            RECEIPT as suite_receipt,
        )
        from qs_ai.infrastructure.persistence.mysql.profile_registrations import (
            RECEIPT as profile_receipt,
        )
        from qs_ai.infrastructure.persistence.mysql.prompt_freezes import RECEIPT as prompt_receipt
        from qs_ai.infrastructure.persistence.mysql.suite_contracts import encode

        if self.prompt.asset is None or self.suite.suite is None or self.suite.contracts is None:
            raise ValueError("New complete preparation plan required")
        values = (
            str(self.identities.solution_id).encode(),
            str(self.identities.command_id).encode(),
            self.first_at.isoformat().encode(),
            self.creation.execution_mode.encode(),
            self.prompt.asset.package_json.encode(),
            prompt_receipt.dump_json(self.prompt.receipt),
            self.generation.definition_json.encode(),
            self.semantic.definition_json.encode(),
            self.profile.profile.definition_json.encode(),
            profile_receipt.dump_json(self.profile.receipt),
            self.suite.suite.definition_json.encode(),
            suite_receipt.dump_json(self.suite.receipt, exclude_defaults=True),
            encode(self.suite.contracts)[0].encode(),
            self.creation.definition_json.encode(),
            self.prepared_json.encode(),
        )
        result = hashlib.sha256(b"qs-ai-solution-prepared-assets/v1\0")
        for raw in values:
            result.update(len(raw).to_bytes(8, "big"))
            result.update(raw)
        return result.digest()


async def plan_prepare_assets(
    db: AsyncSession,
    scope: DraftScope,
    state: dict[str, Any],
    command_id: UUID,
    at: datetime,
    models: tuple[str, ...],
    configuration: ModelConfiguration | None = None,
    execution_mode: str = "serial_v1",
    identities: PreparationIDs | None = None,
) -> PreparedAssetPlan:
    solution_id = UUID(state["solution_id"])
    ids = identities if identities is not None else PreparationIDs.freeze(solution_id, command_id)
    if (
        not isinstance(ids, PreparationIDs)
        or (ids.solution_id, ids.command_id) != (solution_id, command_id)
        or state["organization_id"] != scope.organization_id
        or execution_mode not in ("serial_v1", "candidate_v2")
        or not isinstance(at, datetime)
        or at.utcoffset() is None
    ):
        raise ValueError("Original scoped preparation and first mode/clock required")
    release = selected_release(state)
    source_profile, manifest = await generation_snapshot(db, release)
    prompt = await plan_freeze(
        db,
        scope,
        FreezePromptDraft(
            UUID(state["draft_id"]),
            ids.prompt_command_id,
            state["draft_revision"],
            state["reason"],
        ),
        at,
    )
    if prompt.asset is None:
        raise AssetConflict("Existing Prompt freeze cannot be adopted as new preparation")
    routes = []
    for purpose in ("generation", "semantic"):
        asset = freeze_route(
            await route_for(db, getattr(release, purpose + "_route")),
            solution_id,
            purpose,
            state[purpose],
            models,
            configuration,
        )
        if (
            await AssetSnapshotReader(db, route_assets, RouteAsset).get(asset.route, asset.revision)
            is not None
        ):
            raise AssetConflict("Prepared route version already exists")
        routes.append(asset)
    generation, semantic = routes
    definition = json.loads(source_profile.definition_json)
    definition["version"] = state["target_version"]
    generation_ref, semantic_ref = route_reference(generation), route_reference(semantic)
    definition["generation_policy"].update(
        prompt_template_id=prompt.receipt.asset.identity,
        prompt_version=prompt.receipt.asset.version,
        provider_route=generation_ref.identity,
    )
    profile = await plan_profile(
        db,
        scope,
        RegisterProfile(
            ids.profile_command_id,
            manifest.profile,
            json.dumps(definition, ensure_ascii=False),
            prompt.receipt.asset,
            generation_ref,
            state["reason"],
        ),
        at,
        planned_prompt=prompt.asset,
        planned_route=generation,
    )
    if profile.existing:
        raise AssetConflict("Existing Profile cannot be adopted as new preparation")
    suite = await plan_suite(
        db,
        scope,
        RegisterSuite(
            ids.suite_command_id,
            release.suite,
            release.suite.id,
            state["target_version"],
            profile.receipt.manifest.profile,
            prompt.receipt.asset,
            generation_ref,
            state["reason"],
            semantic_prompt=release.semantic_prompt,
            semantic_owner_organization_id=state.get("evaluation_assets", {}).get(
                "semantic_owner_organization_id", 0
            ),
        ),
        at,
        planned_profile=profile.profile,
        planned_prompt=prompt.asset,
        planned_route=generation,
    )
    if suite.suite is None or suite.contracts is None:
        raise AssetConflict("Existing Suite cannot be adopted as new preparation")
    target = prepared_release(release, suite.receipt, prompt.receipt, generation_ref, semantic_ref)
    contracts = await contracts_from_binding(db, target, scope.organization_id, suite.contracts)
    await validate_release_assets(
        target,
        PlannedAssetReader(
            AssetSnapshotReader(db, profile_assets, ProfileAsset), (profile.profile,)
        ),
        PlannedAssetReader(AssetSnapshotReader(db, prompt_assets, PromptAsset), (prompt.asset,)),
        PlannedAssetReader(
            AssetSnapshotReader(db, route_assets, RouteAsset), (generation, semantic)
        ),
        AssetSnapshotReader(db, schema_assets, SchemaAsset),
        frozen_suite=suite.suite,
        execution_policy_json=contracts.execution.definition_json,
        gate_policy_json=contracts.gate.definition_json,
        semantic=contracts.semantic,
    )
    creation = freeze_run_creation(
        ids.run_id,
        target,
        scope.organization_id,
        f"user:{scope.operator_user_id}",
        state["reason"],
        at,
        suite=suite.suite,
        contracts=contracts,
        generation_manifest=suite.receipt.manifest,
        execution_mode=execution_mode,
    )
    prepared = prepared_result(ids.run_id, target, suite.receipt.manifest, at, contracts.execution)
    return PreparedAssetPlan(
        ids,
        target,
        at,
        prompt,
        generation,
        semantic,
        profile,
        suite,
        contracts,
        creation,
        json.dumps(
            prepared, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
        ),
    )
