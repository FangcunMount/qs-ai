"""Identity validation beside immutable DDL, without changing backup contract hashes."""

from qs_ai.maintenance.schema_refactor.layouts import INTERMEDIATE_HEAD, NEW_HEAD, OLD_HEAD

_AUTO_INCREMENT_COLUMNS: dict[str, frozenset[tuple[str, str]]] = {
    OLD_HEAD: frozenset(
        {
            ("evaluation_admission_locks", "organization_id"),
            ("participant_admission_locks", "organization_id"),
        }
    ),
    INTERMEDIATE_HEAD: frozenset(
        {
            ("governance_asset_versions", "asset_row_id"),
            ("governance_draft_heads", "draft_row_id"),
            ("evaluation_admission_locks", "organization_id"),
            ("participant_admission_locks", "organization_id"),
        }
    ),
    NEW_HEAD: frozenset(
        {
            ("governance_asset_versions", "asset_row_id"),
            ("governance_draft_heads", "draft_row_id"),
            ("quota_evaluation_admission_locks", "organization_id"),
            ("quota_participant_admission_locks", "organization_id"),
        }
    ),
}


def auto_increment_columns(head: str) -> frozenset[tuple[str, str]]:
    """Return the complete allowed set, including the isolated 0039 intermediate layout."""
    try:
        return _AUTO_INCREMENT_COLUMNS[head]
    except KeyError:
        raise ValueError("Unsupported schema head for identity validation") from None
