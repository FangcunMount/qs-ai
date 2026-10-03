"""One scan step for the host's existing supervisor; no independent scheduler."""

import asyncio
from collections.abc import Mapping

from reliable_messaging.delivery import Confirmation, Outcome
from reliable_messaging.durable import Identity
from reliable_messaging.nsq import NSQPublisher

from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.messaging import MessagingStore


class MQRelay:
    def __init__(
        self,
        transactions: Transactions,
        store: MessagingStore,
        publishers: Mapping[str, NSQPublisher],
        *,
        address: str,
    ) -> None:
        if address not in publishers:
            raise ValueError("configured original NSQD publisher required")
        self.transactions, self.store = transactions, store
        self.publisher = publishers[address]  # borrowed, host owns start/stop
        self._gate = asyncio.Lock()

    async def step(self) -> None:
        # The host invokes this from its only relay. Concurrent invocation fails
        # rather than starting another scheduler or issuing duplicate in-flight PUBs.
        if self._gate.locked():
            raise RuntimeError("MQ relay scan already active")
        async with self._gate:
            async with self.transactions.open() as db:
                await db.begin()
                pending = await self.store.outbox.pending(db, 20)
            for row in pending:
                identity = Identity(row["producer"], row["destination"], row["message_id"])
                if row["attempts"] >= 8:
                    async with self.transactions.open() as db:
                        await db.begin()
                        await self.store.outbox.hold(
                            db,
                            identity,
                            row["body_sha256"],
                            error_code="delivery_budget_exhausted",
                        )
                        await db.commit()
                    continue
                result = await self.publisher.publish(row["topic"], row["wire"])
                async with self.transactions.open() as db:
                    await db.begin()
                    if (
                        result.outcome == Outcome.CONFIRMED
                        and result.confirmation == Confirmation.BROKER
                    ):
                        await self.store.outbox.published(db, identity, row["body_sha256"])
                    elif result.outcome == Outcome.REJECTED:
                        await self.store.outbox.hold(
                            db, identity, row["body_sha256"], error_code="publish_rejected"
                        )
                    else:
                        await self.store.outbox.retry(
                            db,
                            identity,
                            row["body_sha256"],
                            delay_seconds=min(60, 2 ** min(row["attempts"] + 1, 6)),
                        )
                    await db.commit()
