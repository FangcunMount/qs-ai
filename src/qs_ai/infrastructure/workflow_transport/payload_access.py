"""Original read transaction and fixed technical observations behind a pure port."""

from typing import Any

from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.messaging import MessagingStore
from qs_ai.infrastructure.persistence.mysql.messaging_observations import record_payload_failure
from qs_ai.infrastructure.workflow_transport.messaging import MAX_BODY, valid_hash, valid_id


class MySQLPayloadAccess:
    def __init__(self, transactions: Transactions, store: MessagingStore) -> None:
        self.transactions, self.store = transactions, store

    def valid_reference(self, reference: Any) -> bool:
        return (
            reference.ByteSize() <= 8192
            and valid_id(reference.message_id)
            and valid_hash(reference.body_sha256)
            and 0 < reference.body_length <= MAX_BODY
        )

    async def payload(self, reference: Any, authenticated_workload: str) -> bytes:
        async with self.transactions.open() as db:
            await db.begin()
            return await self.store.payload(db, reference, authenticated_workload)

    async def record_failure(self, kind: str) -> bool:
        return await record_payload_failure(self.transactions, kind)
