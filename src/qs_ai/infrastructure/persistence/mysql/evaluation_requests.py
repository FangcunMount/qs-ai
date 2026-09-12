"""Create once; a lost receipt can be recovered without resetting a progressed Run."""

import json
from dataclasses import asdict
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from qs_ai.application.evaluation.checkpoints import CheckpointConflict
from qs_ai.application.evaluation.management import EvaluationView, ManagementScope
from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_management import read_view
from qs_ai.infrastructure.persistence.mysql.evaluation_runs import MySQLRunCreator
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_runs


class MySQLEvaluationRequests:
    def __init__(self, transactions: Transactions, creator: MySQLRunCreator) -> None:
        self.transactions, self.creator = transactions, creator

    async def create(
        self,
        scope: ManagementScope,
        release: EvidenceReleaseIdentity,
        reason: str,
        at: datetime,
        *,
        confirm: bool,
    ) -> EvaluationView:
        reason = reason.strip()
        if confirm is not True or not reason or len(reason.encode()) > 1000:
            raise ValueError("Explicit confirmation and bounded reason required")
        try:
            # Resolves all immutable assets before any creation writes; commit is atomic.
            await self.creator.create(
                scope.run_id, release, scope.organization_id, scope.actor, reason, at
            )
        except IntegrityError as error:
            if error.orig is None or not error.orig.args or error.orig.args[0] != 1062:
                raise
            # The failed transaction has ended. A fresh snapshot sees the winning commit.
            async with self.transactions.open() as db:
                raw = (
                    await db.execute(
                        select(evaluation_runs.c.definition_json).where(
                            evaluation_runs.c.run_id == str(scope.run_id),
                            evaluation_runs.c.organization_id == scope.organization_id,
                        )
                    )
                ).scalar_one_or_none()
                if raw is None:
                    raise CheckpointConflict("Creation request conflicts") from None
                saved = json.loads(raw)
                if (
                    saved["release"] != asdict(release)
                    or saved["audit"]["requested_by"] != scope.actor
                    or saved["audit"]["request_reason"] != reason
                ):
                    raise CheckpointConflict("Creation request conflicts") from None
                return await read_view(db, scope)
        async with self.transactions.open() as db:
            return await read_view(db, scope)
