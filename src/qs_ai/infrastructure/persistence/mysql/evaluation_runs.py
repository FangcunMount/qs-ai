"""Create frozen Run records in the caller's transaction; no execution is scheduled."""

import json
import re
from dataclasses import asdict
from datetime import datetime
from uuid import UUID

from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.checkpoints import CheckpointState
from qs_ai.application.evaluation.execution_mode import SERIAL_EXECUTION_MODE, ExecutionMode
from qs_ai.application.evaluation.release import validate_generation_manifest
from qs_ai.application.interpretation.profile_assets import ProfileAssets
from qs_ai.application.interpretation.prompt_assets import PromptAssets
from qs_ai.application.interpretation.route_assets import RouteAssets
from qs_ai.application.interpretation.schema_assets import SchemaAssets
from qs_ai.domain.evaluation.acceptance import rule_document
from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity
from qs_ai.domain.governance.manifest import GenerationManifest
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_contracts import evaluation_contracts
from qs_ai.infrastructure.persistence.mysql.evaluation_dispatches import freeze_policy
from qs_ai.infrastructure.persistence.mysql.evaluation_suites import load_registered_suite
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_checkpoints, evaluation_runs


async def create_run(
    db: AsyncSession,
    run_id: UUID,
    release: EvidenceReleaseIdentity,
    organization_id: int,
    requested_by: str,
    request_reason: str,
    created_at: datetime,
    *,
    generation_manifest: GenerationManifest | None = None,
    execution_mode: str = "serial_v1",
) -> CheckpointState:
    """Caller must authorize actor and resolve every asset before entering this operation."""
    if execution_mode not in ("serial_v1", "candidate_v2"):
        raise ValueError("Invalid evaluation execution mode")
    if not isinstance(run_id, UUID) or run_id.int == 0:
        raise ValueError("Run id required")
    if type(organization_id) is not int or organization_id <= 0:
        raise ValueError("Organization required")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9:._/-]{0,127}", requested_by):
        raise ValueError("Request actor required")
    if (
        not request_reason.strip()
        or len(request_reason.encode()) > 1000
        or any(x in request_reason for x in "<>")
    ):
        raise ValueError("Invalid request reason")
    if created_at.tzinfo is None or created_at.utcoffset() is None:
        raise ValueError("Creation time must have a time zone")
    suite = await load_registered_suite(db, release.suite, organization_id=organization_id)
    contracts = await evaluation_contracts(db, release, organization_id)
    policy, gate, semantic = contracts.execution, contracts.gate, contracts.semantic
    if suite.manifest is not None and generation_manifest != suite.manifest:
        raise ValueError("Native suite requires its registered generation manifest")
    release.validate_frozen_policies(policy.definition_json, gate.definition_json)
    if (len(suite.generation_case_ids), suite.repetitions, policy.preflight_cases) != (
        policy.generation_cases,
        policy.candidates_per_case,
        1,
    ):
        raise ValueError("Suite and execution policy disagree")
    definition = {
        "schema_version": "qs-ai-evaluation-run-creation/v1",
        "run_id": str(run_id),
        "release": asdict(release),
        "release_fingerprint": release.fingerprint(),
        "execution_policy_json": policy.definition_json,
        "gate_policy_json": gate.definition_json,
        "semantic_prompt_markdown": semantic.prompt_markdown,
        "semantic_owner_organization_id": contracts.semantic_owner_organization_id,
        "semantic_output_schema_json": semantic.output_schema_json,
        "acceptance_rule": rule_document(),
        "suite_json": suite.definition_json,
        "status": "requested",
        "preflight": {"case_id": suite.preflight_case_id, "status": "pending"},
        "slots": [
            {"case_id": case, "ordinal": ordinal, "status": "pending"}
            for case, ordinal in suite.slots()
        ],
        "audit": {
            "organization_id": organization_id,
            "requested_by": requested_by,
            "request_reason": request_reason,
            "created_at": created_at.isoformat(),
        },
        "transitions": [
            {
                "to": "requested",
                "cause_code": "evaluation_requested",
                "actor": requested_by,
                "at": created_at.isoformat(),
            }
        ],
    }
    # Older retained Runs have no manifest. They remain readable, but cannot be
    # published until an evaluation with frozen executable asset bytes is accepted.
    if generation_manifest is not None:
        validate_generation_manifest(release, generation_manifest)
        definition["generation_manifest_json"] = generation_manifest.canonical_json()
        definition["generation_manifest_fingerprint"] = generation_manifest.fingerprint()
    await db.execute(
        insert(evaluation_runs).values(
            run_id=str(run_id),
            execution_mode=execution_mode,
            organization_id=organization_id,
            requested_by=requested_by,
            definition_json=json.dumps(definition, ensure_ascii=False, separators=(",", ":")),
            progress_json={"status": "requested", "transitions": definition["transitions"]},
        )
    )
    await freeze_policy(db, run_id, policy)
    await db.execute(insert(evaluation_checkpoints).values(run_id=str(run_id), version=1))
    return CheckpointState(run_id, 1, None)


class MySQLRunCreator:
    """Internal creation adapter; the caller must establish administrative authorization."""

    def __init__(
        self,
        transactions: Transactions,
        profiles: ProfileAssets,
        prompts: PromptAssets,
        routes: RouteAssets,
        schemas: SchemaAssets,
        execution_mode: ExecutionMode = SERIAL_EXECUTION_MODE,
    ) -> None:
        self.execution_mode = execution_mode
        self.transactions = transactions
        self.profiles, self.prompts, self.routes, self.schemas = profiles, prompts, routes, schemas

    async def create(
        self,
        run_id: UUID,
        release: EvidenceReleaseIdentity,
        organization_id: int,
        requested_by: str,
        request_reason: str,
        created_at: datetime,
    ) -> CheckpointState:
        from qs_ai.infrastructure.qs_server.evaluation_release import validate_release_assets

        async with self.transactions.open() as db:
            suite = await load_registered_suite(db, release.suite, organization_id=organization_id)
            contracts = await evaluation_contracts(db, release, organization_id)
            manifest = await validate_release_assets(
                release,
                self.profiles,
                self.prompts,
                self.routes,
                self.schemas,
                frozen_suite=suite,
                execution_policy_json=contracts.execution.definition_json,
                gate_policy_json=contracts.gate.definition_json,
                semantic=contracts.semantic,
            )
            state = await create_run(
                db,
                run_id,
                release,
                organization_id,
                requested_by,
                request_reason,
                created_at,
                generation_manifest=manifest,
                execution_mode=self.execution_mode.value,
            )
            await db.commit()
        return state
