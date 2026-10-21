"""Freeze original Solution state, then stage the original all-assets root exactly once."""

import asyncio
import hashlib
import json
import math
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID, uuid5

from reliable_messaging.durable import MessageConflict
from reliable_messaging.transaction import FrozenPlan
from reliable_messaging.transaction_mysql import MySQLTransactionStore
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.governance.solutions import ModelSelection
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.domain.governance.profile import ProfileAsset
from qs_ai.domain.governance.prompt import PromptAsset
from qs_ai.domain.governance.prompt_draft import DraftConflict, PromptDraft
from qs_ai.domain.governance.route import RouteAsset
from qs_ai.infrastructure.persistence.mysql.asset_records import (
    profile_assets,
    prompt_assets,
    route_assets,
)
from qs_ai.infrastructure.persistence.mysql.asset_snapshot import AssetSnapshotReader
from qs_ai.infrastructure.persistence.mysql.evaluation_suites import RECEIPT as suite_receipt
from qs_ai.infrastructure.persistence.mysql.profile_registrations import RECEIPT as profile_receipt
from qs_ai.infrastructure.persistence.mysql.prompt_drafts import SNAPSHOT, read_draft
from qs_ai.infrastructure.persistence.mysql.prompt_freezes import RECEIPT as prompt_receipt
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_runs,
    evaluation_suites,
    profile_registrations,
    prompt_draft_freezes,
    solution_commands,
    solution_revisions,
)
from qs_ai.infrastructure.persistence.mysql.solution_preparation import (
    PreparedAssetPlan,
    plan_prepare_assets,
)
from qs_ai.infrastructure.persistence.mysql.solutions import (
    MySQLSolutions,
    checksum,
    encode,
    hydrate,
    prior,
    read_state,
)
from qs_ai.infrastructure.persistence.mysql.suite_contracts import encode as encode_contracts
from qs_ai.infrastructure.persistence.mysql.transaction_archive import TransactionArchive
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import (
    EvaluationStateFacts,
    freeze_evaluation_state_plan,
)
from qs_ai.infrastructure.workflow_transport.evaluation_state_root import EvaluationStateRoot
from qs_ai.infrastructure.workflow_transport.messaging import prepare
from qs_ai.infrastructure.workflow_transport.solution_preparation_plan import (
    SolutionPreparationFacts,
    SolutionPreparationInput,
    preparation_target_digest,
    read_preparation_plan,
)
from qs_ai.infrastructure.workflow_transport.state_projection import evaluation_state


def source_digest(raw: str) -> bytes:
    return hashlib.sha256(b"qs-ai-solution-preparation-source/v1\0" + raw.encode()).digest()


def draft_digest(draft: PromptDraft) -> bytes:
    return hashlib.sha256(
        b"qs-ai-solution-preparation-draft/v1\0" + SNAPSHOT.dump_json(draft)
    ).digest()


def target_state(
    incoming: SolutionPreparationInput, source_json: str, assets: PreparedAssetPlan
) -> str:
    source = json.loads(source_json)
    if (
        source["solution_id"] != str(incoming.solution_id)
        or type(source["organization_id"]) is not int
        or source["organization_id"] != incoming.scope.organization_id
        or source["revision"] != incoming.command.expected_revision
        or source["prepared"]
    ):
        raise DraftConflict("Original Solution source changed or is already prepared")
    source.update(
        reason=incoming.command.reason,
        prepared=assets.prepared(),
        revision=source["revision"] + 1,
        updated_at=assets.first_at.isoformat(),
    )
    return encode(source)


@dataclass(frozen=True)
class SolutionPreparationCandidate:
    winner: FrozenPlan = field(repr=False)
    assets: PreparedAssetPlan = field(repr=False)
    source_json: str = field(repr=False)
    target_json: str = field(repr=False)


class SolutionPreparationPlanner:
    def __init__(
        self,
        original: MySQLSolutions,
        *,
        signing_key: Any,
        recipient_key: Any,
        database_seconds: float = 2,
    ) -> None:
        if not math.isfinite(database_seconds) or database_seconds <= 0:
            raise ValueError("Finite positive preparation database budget required")
        self._original, self._database_seconds = original, database_seconds
        self._signing_key, self._recipient_key = signing_key, recipient_key

    async def replay(
        self,
        incoming: SolutionPreparationInput,
        *,
        expected_target: bytes | None = None,
    ) -> dict[str, Any] | None:
        async with (
            asyncio.timeout(self._database_seconds),
            self._original.transactions.open() as db,
        ):
            saved = await prior(
                db, incoming.scope, incoming.command.command_id, incoming.request_text
            )
            if saved is None:
                return None
            if (
                expected_target is not None
                and preparation_target_digest(encode(saved)) != expected_target
            ):
                raise MessageConflict("Original preparation receipt contradicts frozen target")
            return await hydrate(db, incoming.scope, saved)

    async def candidate(
        self,
        incoming: SolutionPreparationInput,
        at: datetime,
    ) -> SolutionPreparationCandidate | None:
        EvaluationStateFacts(incoming.intent, at, 0, 0)
        mode = (
            "candidate_v2"
            if self._original.settings.evaluation.candidate_mode_enabled
            else "serial_v1"
        )
        async with (
            asyncio.timeout(self._database_seconds),
            self._original.transactions.open() as db,
        ):
            await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            if (
                await prior(db, incoming.scope, incoming.command.command_id, incoming.request_text)
                is not None
            ):
                return None
            state = await read_state(db, incoming.scope, incoming.solution_id)
            if state["revision"] != incoming.command.expected_revision or state["prepared"]:
                raise DraftConflict("Solution changed or is already prepared")
            current = await read_draft(db, incoming.scope, UUID(state["draft_id"]))
            if current.revision != state["draft_revision"]:
                raise DraftConflict("Linked Prompt was edited outside this workspace")
            await self._original._check_selections(
                db,
                state,
                ModelSelection(**state["generation"]),
                ModelSelection(**state["semantic"]),
            )
            source_json = encode(state)
            proposed = json.loads(source_json)
            proposed["reason"] = incoming.command.reason
            assets = await plan_prepare_assets(
                db,
                incoming.scope,
                proposed,
                incoming.command.command_id,
                at,
                self._original.settings.governance_models,
                self._original.settings.models,
                execution_mode=mode,
                identities=incoming.identities,
            )
            target_json = target_state(incoming, source_json, assets)
            preview = await hydrate(db, incoming.scope, json.loads(target_json))
            if len(encode(preview).encode()) > 1048576:
                raise ValueError("Solution response exceeds limit")
            original = SolutionPreparationFacts(
                incoming.solution_id,
                incoming.command.command_id,
                source_digest(source_json),
                draft_digest(current),
                assets.digest,
                preparation_target_digest(target_json),
                at,
                mode,
            )
            facts = EvaluationStateFacts(incoming.intent, at, 0, 0, original.encode())
            visible = evaluation_state(
                str(incoming.identities.run_id),
                dict(
                    definition_json=assets.creation.definition_json,
                    organization_id=incoming.scope.organization_id,
                    progress_json={"status": "requested"},
                    version=1,
                ),
                facts.sequence,
            )
        # No session, UUID generation, signing or encryption enters the business callback.
        first = prepare(
            pb.EVALUATION_STATE,
            str(uuid5(incoming.identities.run_id, "state:1")),
            str(incoming.identities.run_id),
            pb.MessagingBody(evaluation_state=visible),
            organization_id=str(incoming.scope.organization_id),
            signing_key=self._signing_key,
            recipient_key=self._recipient_key,
            original_occurred_at=facts.at.isoformat(timespec="microseconds"),
        )
        winner = freeze_evaluation_state_plan(facts, str(incoming.command.command_id), first)
        read_preparation_plan(incoming, winner)
        original.validate_assets(incoming, assets)
        return SolutionPreparationCandidate(winner, assets, source_json, target_json)


async def _expect_row(db: AsyncSession, statement: Any, expected: dict[str, Any]) -> None:
    row = (await db.execute(statement)).mappings().one_or_none()
    if row is None or any(row[key] != value for key, value in expected.items()):
        raise MessageConflict("Original frozen preparation facts differ")


class SolutionPreparationRoot(EvaluationStateRoot):
    def __init__(
        self,
        original: MySQLSolutions,
        archives: TransactionArchive,
        store: MySQLTransactionStore,
        incoming: SolutionPreparationInput,
        winner: FrozenPlan,
        candidate: SolutionPreparationCandidate,
        *,
        database_seconds: float = 2,
    ) -> None:
        facts, first, archived = read_preparation_plan(incoming, winner)
        assets = candidate.assets
        first.validate_assets(incoming, assets)
        if (
            candidate.winner.digest != winner.digest
            or source_digest(candidate.source_json) != first.source_digest
            or preparation_target_digest(candidate.target_json) != first.target_digest
            or target_state(incoming, candidate.source_json, assets) != candidate.target_json
            or pb.MessagingBody.FromString(archived.body).evaluation_state.release_fingerprint
            != assets.release.fingerprint()
            or assets.prompt.asset is None
            or assets.suite.suite is None
            or assets.suite.contracts is None
        ):
            raise MessageConflict("Exact original frozen Solution candidate required")
        self._value: dict[str, Any] | None = None
        self._business_error: Exception | None = None
        self._candidate, self._incoming = candidate, incoming
        # Freeze exact SQL/audit expectations before the root, never inside it.
        self._prompt_raw = prompt_receipt.dump_json(assets.prompt.receipt).decode()
        self._profile_raw = profile_receipt.dump_json(assets.profile.receipt).decode()
        self._suite_raw = suite_receipt.dump_json(
            assets.suite.receipt, exclude_defaults=True
        ).decode()
        self._contracts = encode_contracts(assets.suite.contracts)

        async def write(db: AsyncSession, bound: EvaluationStateFacts) -> None:
            try:
                if bound != facts:
                    raise MessageConflict("Original preparation execution facts changed")
                source = await read_state(db, incoming.scope, incoming.solution_id, lock=True)
                if source_digest(encode(source)) != first.source_digest:
                    raise DraftConflict("Original Solution source changed")
                draft = await read_draft(db, incoming.scope, UUID(source["draft_id"]), lock=True)
                if draft_digest(draft) != first.draft_digest:
                    raise DraftConflict("Original linked Prompt revision changed")
                result = await original.apply_in(
                    db,
                    incoming.scope,
                    incoming.solution_id,
                    incoming.command,
                    first.first_at,
                    preparation_ids=assets.identities,
                    execution_mode=first.execution_mode,
                )
                if not result.wrote:
                    raise DraftConflict("Existing receipt cannot be adopted by new execution")
                if encode(result.value["prepared"]) != assets.prepared_json:
                    raise MessageConflict("Original prepared result changed")
                await self._verify_business(db)
                self._value = result.value
            except Exception as error:
                self._business_error = error
                raise

        super().__init__(
            original.transactions,
            archives,
            store,
            incoming.intent,
            winner,
            write,
            database_seconds=database_seconds,
            isolation_level="READ COMMITTED",
        )

    async def _verify_business(self, db: AsyncSession) -> None:
        c, incoming = self._candidate, self._incoming
        assets = c.assets
        assert assets.prompt.asset is not None and assets.suite.suite is not None
        await _expect_row(
            db,
            select(solution_revisions).where(
                solution_revisions.c.solution_id == str(incoming.solution_id),
                solution_revisions.c.organization_id == incoming.scope.organization_id,
            ),
            dict(
                state_json=c.target_json,
                state_sha256=checksum(c.target_json),
                revision=incoming.command.expected_revision + 1,
            ),
        )
        await _expect_row(
            db,
            select(solution_commands).where(
                solution_commands.c.command_id == str(incoming.command.command_id),
                solution_commands.c.solution_id == str(incoming.solution_id),
                solution_commands.c.organization_id == incoming.scope.organization_id,
                solution_commands.c.operator_user_id == incoming.scope.operator_user_id,
            ),
            dict(
                request_json=incoming.request_text,
                receipt_json=c.target_json,
                receipt_sha256=checksum(c.target_json),
            ),
        )
        creation = assets.creation
        await _expect_row(
            db,
            select(evaluation_runs).where(evaluation_runs.c.run_id == str(creation.run_id)),
            dict(
                organization_id=creation.organization_id,
                requested_by=creation.requested_by,
                definition_json=creation.definition_json,
                execution_mode=creation.execution_mode,
                frozen_execution_policy_fingerprint=creation.policy.fingerprint,
                frozen_execution_policy_json=creation.policy.definition_json,
            ),
        )
        await _expect_row(
            db,
            select(prompt_draft_freezes).where(
                prompt_draft_freezes.c.command_id == str(assets.identities.prompt_command_id),
            ),
            dict(receipt_json=self._prompt_raw),
        )
        await _expect_row(
            db,
            select(profile_registrations).where(
                profile_registrations.c.command_id == str(assets.identities.profile_command_id),
            ),
            dict(receipt_json=self._profile_raw),
        )
        await _expect_row(
            db,
            select(evaluation_suites).where(
                evaluation_suites.c.command_id == str(assets.identities.suite_command_id),
            ),
            dict(
                receipt_json=self._suite_raw,
                definition_json=assets.suite.suite.definition_json,
                contracts_json=self._contracts[0],
                contracts_sha256=self._contracts[1],
            ),
        )
        prompt = await AssetSnapshotReader(db, prompt_assets, PromptAsset).get(
            assets.prompt.receipt.asset.identity, assets.prompt.receipt.asset.version
        )
        if prompt != assets.prompt.asset:
            raise MessageConflict("Original first Prompt asset changed")
        profile = await AssetSnapshotReader(db, profile_assets, ProfileAsset).get(
            assets.profile.profile.profile_id, assets.profile.profile.version
        )
        if profile != assets.profile.profile:
            raise MessageConflict("Original first Profile asset changed")
        for route in (assets.generation, assets.semantic):
            actual = await AssetSnapshotReader(db, route_assets, RouteAsset).get(
                route.route, route.revision
            )
            if actual != route:
                raise MessageConflict("Original first model route changed")

    @property
    def value(self) -> dict[str, Any] | None:
        return self._value

    @property
    def business_error(self) -> Exception | None:
        return self._business_error
