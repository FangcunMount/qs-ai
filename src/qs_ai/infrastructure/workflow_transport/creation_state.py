"""Standalone creation planning and one guarded original root; no default assembly."""

import asyncio
import json
import math
from datetime import datetime
from typing import Any
from uuid import uuid5

from reliable_messaging.durable import MessageConflict
from reliable_messaging.transaction import FrozenPlan
from reliable_messaging.transaction_mysql import MySQLTransactionStore
from reliable_messaging.transport import RouteResolver
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.management import EvaluationView
from qs_ai.contracts.workflow import messaging_pb2 as pb
from qs_ai.domain.governance.manifest import AssetReference, GenerationManifest
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_management import read_view
from qs_ai.infrastructure.persistence.mysql.evaluation_runs import (
    MySQLRunCreator,
    RunCreationPlan,
    apply_run_creation,
    plan_run_creation,
)
from qs_ai.infrastructure.persistence.mysql.transaction_archive import TransactionArchive
from qs_ai.infrastructure.workflow_transport.creation_state_plan import (
    CreationPlanFacts,
    CreationStateInput,
    read_creation_state_plan,
)
from qs_ai.infrastructure.workflow_transport.evaluation_state_plan import (
    EvaluationStateFacts,
    freeze_evaluation_state_plan,
)
from qs_ai.infrastructure.workflow_transport.evaluation_state_root import EvaluationStateRoot
from qs_ai.infrastructure.workflow_transport.messaging import prepare
from qs_ai.infrastructure.workflow_transport.state_projection import evaluation_state


class CreationStatePlanner:
    def __init__(
        self,
        creator: MySQLRunCreator,
        *,
        signing_key: Any,
        recipient_key: Any,
        database_seconds: float = 2,
        route_resolver: RouteResolver,
    ) -> None:
        self._route_resolver = route_resolver
        if not math.isfinite(database_seconds) or database_seconds <= 0:
            raise ValueError("Finite positive creation database budget required")
        self._creator, self._database_seconds = creator, database_seconds
        self._signing_key, self._recipient_key = signing_key, recipient_key

    async def candidate(
        self, incoming: CreationStateInput, at: datetime
    ) -> tuple[FrozenPlan, RunCreationPlan]:
        # Reject invalid clocks before any asset/database I/O.
        EvaluationStateFacts(incoming.intent, at, 0, 0)
        async with asyncio.timeout(self._database_seconds), self._creator.transactions.open() as db:
            await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            plan = await self._creator.plan_in(
                db,
                incoming.scope.run_id,
                incoming.release,
                incoming.scope.organization_id,
                incoming.scope.actor,
                incoming.reason,
                at,
            )
            original = CreationPlanFacts.from_creation(incoming, plan)
            facts = EvaluationStateFacts(incoming.intent, at, 0, 0, original.encode())
            visible = evaluation_state(
                str(plan.run_id),
                dict(
                    organization_id=plan.organization_id,
                    definition_json=plan.definition_json,
                    progress_json={"status": "requested"},
                    version=1,
                ),
                facts.sequence,
            )
        # The read session is closed before signing/encryption or any half message.
        first = prepare(
            pb.EVALUATION_STATE,
            str(uuid5(plan.run_id, "state:1")),
            str(plan.run_id),
            pb.MessagingBody(evaluation_state=visible),
            organization_id=str(plan.organization_id),
            signing_key=self._signing_key,
            recipient_key=self._recipient_key,
            original_occurred_at=facts.at.isoformat(timespec="microseconds"),
        )
        winner = freeze_evaluation_state_plan(
            facts, str(plan.run_id), first, route_resolver=self._route_resolver
        )
        read_creation_state_plan(incoming, winner, route_resolver=self._route_resolver)
        return winner, plan


class CreationStateRoot(EvaluationStateRoot):
    def __init__(
        self,
        transactions: Transactions,
        archives: TransactionArchive,
        store: MySQLTransactionStore,
        incoming: CreationStateInput,
        winner: FrozenPlan,
        creation: RunCreationPlan,
        *,
        database_seconds: float = 2,
        route_resolver: RouteResolver,
    ) -> None:
        self._route_resolver = route_resolver
        facts, original, _ = read_creation_state_plan(
            incoming, winner, route_resolver=route_resolver
        )
        original.validate(incoming, creation)
        definition = json.loads(creation.definition_json)
        # Decode immutable assets before the callback. No new identity, clock,
        # signature, encryption or external asset request belongs in that callback.
        raw = definition.get("generation_manifest_json")
        manifest = None
        if raw is not None:
            value = json.loads(raw)
            if value.pop("schema_version") != "qs-ai-generation-manifest/v1":
                raise ValueError("Original creation manifest required")
            manifest = GenerationManifest(**{k: AssetReference(**v) for k, v in value.items()})
            if manifest.canonical_json() != raw:
                raise ValueError("Canonical original creation manifest required")
        self._view: EvaluationView | None = None
        self._business_error: Exception | None = None

        async def write(db: AsyncSession, bound: EvaluationStateFacts) -> None:
            try:
                current = await plan_run_creation(
                    db,
                    incoming.scope.run_id,
                    incoming.release,
                    incoming.scope.organization_id,
                    incoming.scope.actor,
                    incoming.reason,
                    original.created_at,
                    generation_manifest=manifest,
                    execution_mode=original.execution_mode,
                )
                original.validate(incoming, current)
                if current != creation or bound != facts:
                    raise MessageConflict("Original creation source contracts changed")
                await apply_run_creation(db, creation)
                self._view = await read_view(db, incoming.scope)
            except Exception as error:
                self._business_error = error
                raise

        super().__init__(
            transactions,
            archives,
            store,
            incoming.intent,
            winner,
            write,
            database_seconds=database_seconds,
            isolation_level="REPEATABLE READ",
            route_resolver=route_resolver,
        )

    @property
    def view(self) -> EvaluationView | None:
        return self._view

    @property
    def business_error(self) -> Exception | None:
        return self._business_error
