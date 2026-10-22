"""Operational roots may commit; visible business state needs its own SDK transcription."""

from collections.abc import Mapping
from typing import Any

from reliable_messaging.transaction import BindingError
from sqlalchemy.ext.asyncio import AsyncSession


class TransactionOnlyEvents:
    """Stateless application boundary; never replaces the original witness guard.

    Business transaction roots install their exact frozen transcription on that
    one Session. The default boundary rejects legacy event staging and permits
    only clean operational roots such as lease/response/quarantine persistence.
    It owns no Session, connection, transport or lifecycle.
    """

    async def record_interpretation(self, db: AsyncSession, row: Mapping[Any, Any]) -> None:
        raise BindingError("Interpretation state requires its original transaction plan")

    async def flush(self, db: AsyncSession) -> None:
        root = db.get_transaction()
        if (
            root is None
            or not root.is_active
            or db.in_nested_transaction()
            or db.info.get("state_events") is not self
            or db.info.get("evaluation_events")
        ):
            raise BindingError("Business state requires its exact original transaction plan")
