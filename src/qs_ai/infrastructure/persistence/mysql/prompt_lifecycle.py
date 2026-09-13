"""One consistent snapshot of the draft head and validated freeze provenance."""

from uuid import UUID

from sqlalchemy import select

from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.governance.prompt_lifecycle import (
    FrozenPromptVersion,
    PromptDraftLifecycle,
)
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.prompt_drafts import read_draft
from qs_ai.infrastructure.persistence.mysql.prompt_freezes import load_receipt
from qs_ai.infrastructure.persistence.mysql.schema import prompt_draft_freezes as freezes


class MySQLPromptLifecycleReader:
    def __init__(self, transactions: Transactions) -> None:
        self.transactions = transactions

    async def get(self, scope: DraftScope, draft_id: UUID) -> PromptDraftLifecycle:
        async with self.transactions.open() as db:
            # A concurrent freeze must not be paired with an older draft head.
            await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            draft = await read_draft(db, scope, draft_id)
            row = (
                (await db.execute(select(freezes).where(freezes.c.draft_id == str(draft_id))))
                .mappings()
                .one_or_none()
            )
            if row is None:
                return PromptDraftLifecycle(draft, None)
            if row["organization_id"] != scope.organization_id:
                raise ValueError("Draft freeze organization differs from draft head")
            # Same-org administrators can read lifecycle. Original actor's command
            # receipt remains private: validate it internally, return only version state.
            receipt = await load_receipt(
                db,
                DraftScope(scope.organization_id, row["operator_user_id"]),
                UUID(row["command_id"]),
            )
            if receipt is None or receipt.command.expected_revision != draft.revision:
                raise ValueError("Frozen revision differs from current draft head")
            return PromptDraftLifecycle(
                draft,
                FrozenPromptVersion(receipt.asset, draft.revision, receipt.frozen_at),
            )
