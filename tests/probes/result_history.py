"""Read synthetic original event history without reviving a delivery adapter."""

from sqlalchemy import select

from qs_ai.application.integration.events import StateEvent
from qs_ai.domain.interpretation.model import Actor
from qs_ai.infrastructure.persistence.mysql.schema import result_outbox


async def read_events(transactions, session_id):
    async with transactions.open() as db:
        rows = (
            (
                await db.execute(
                    select(result_outbox.c.payload)
                    .where(result_outbox.c.session_id == session_id)
                    .order_by(result_outbox.c.version)
                )
            )
            .scalars()
            .all()
        )
    return [StateEvent(**{**row, "actor": Actor(**row["actor"])}) for row in rows]
