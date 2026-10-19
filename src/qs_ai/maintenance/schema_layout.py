"""Fixed supported storage layouts for maintenance; no reflection or schema writes."""

import sqlalchemy as sa

from qs_ai.maintenance.schema_refactor.layouts import MERGES, NEW_HEAD, OLD_HEAD, RENAMES


def require_known_head(heads: list[str] | None) -> str:
    if heads is None or heads not in ([OLD_HEAD], [NEW_HEAD]):
        raise ValueError("Maintenance requires a recognized complete schema head")
    return heads[0]


def physical_table(head: str, logical_name: str) -> str:
    require_known_head([head])
    if head == OLD_HEAD:
        return logical_name
    return RENAMES.get(logical_name, MERGES.get(logical_name, logical_name))


def require_handoff_head(heads: list[str] | None) -> str:
    # 0041 adds protocol tables; retained result/Outbox tables keep 0040 names.
    # This does not extend the old full-layout conversion/backup whitelist.
    if heads == ["0041_transaction_messaging"]:
        return heads[0]
    return require_known_head(heads)


def handoff_tables(head: str) -> tuple[sa.Table, sa.Table]:
    require_handoff_head([head])
    if head == OLD_HEAD:
        from qs_ai.maintenance.schema_refactor.v0038 import metadata

        return metadata.tables["result_outbox"], metadata.tables["ai_messaging_outbox"]
    from qs_ai.infrastructure.persistence.mysql.messaging import outbox
    from qs_ai.infrastructure.persistence.mysql.schema import result_outbox

    return result_outbox, outbox


def retained_physical_table(head: str, logical_name: str) -> str:
    """Names for retained business/Outbox audit only, not whole-layout conversion."""
    require_handoff_head([head])
    return physical_table(NEW_HEAD if head == "0041_transaction_messaging" else head, logical_name)
