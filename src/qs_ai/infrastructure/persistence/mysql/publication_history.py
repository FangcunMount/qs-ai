"""Read global configuration history without replaying an operator command.

QS authorizes history readers separately from the original-actor recovery API.
Every entry is reconstructed from retained records and verified before projection.
The cursor is an exclusive selector version, using the existing unique index.
"""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.governance.publication import (
    PublicationHistoryEntry,
    PublicationHistoryPage,
    PublicationHistoryQuery,
    PublicationReceipt,
)
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.domain.governance.publication import ReleaseSelector, valid_version
from qs_ai.infrastructure.persistence.mysql.publication_records import load_receipt
from qs_ai.infrastructure.persistence.mysql.schema import (
    configuration_publication_changes as changes,
)


def entry(receipt: PublicationReceipt) -> PublicationHistoryEntry:
    change = receipt.change
    active = change.current.active
    previous = change.previous.active
    return PublicationHistoryEntry(
        change.current.version,
        receipt.command_id,
        change.action,
        change.audit.actor,
        change.audit.reason,
        change.audit.at,
        previous.publication_id if previous else None,
        active.publication_id if active else None,
        active.evidence.run_id if active else None,
        active.evidence.run_version if active else None,
        active.evidence.profile.profile_id if active else None,
        active.evidence.profile.version if active else None,
    )


async def list_history(db: AsyncSession, query: PublicationHistoryQuery) -> PublicationHistoryPage:
    await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
    statement = select(changes).where(changes.c.selector_key == query.selector.key())
    if query.before_version:
        statement = statement.where(changes.c.version < query.before_version)
    rows = list(
        (
            await db.execute(statement.order_by(changes.c.version.desc()).limit(query.limit + 1))
        ).mappings()
    )
    values = []
    for row in rows[: query.limit]:
        receipt = await load_receipt(db, row)
        if receipt.change.current.selector != query.selector:
            raise ValueError("History differs from requested selector")
        values.append(entry(receipt))
    return PublicationHistoryPage(
        query.selector,
        tuple(values),
        values[-1].version if len(rows) > query.limit else 0,
    )


async def get_history(
    db: AsyncSession, selector: ReleaseSelector, version: int
) -> PublicationReceipt:
    if not valid_version(version):
        raise ValueError("Positive publication history version required")
    await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
    row = (
        (
            await db.execute(
                select(changes).where(
                    changes.c.selector_key == selector.key(), changes.c.version == version
                )
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        raise NotFound("Publication history version unavailable")
    receipt = await load_receipt(db, row)
    if receipt.change.current.selector != selector or receipt.change.current.version != version:
        raise ValueError("History differs from requested selector/version")
    return receipt
