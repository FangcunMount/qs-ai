"""Add fixed technical observations, without historical reconstruction or cleanup."""

from alembic import op

revision = "0038_messaging_observations"
down_revision = "0037_workflow_messaging"
branch_labels = None
depends_on = None

KINDS = (
    "duplicate_command",
    "duplicate_ack",
    "payload_fetch_unavailable",
    "payload_fetch_reference_mismatch",
    "payload_fetch_workload_denied",
    "payload_serve_reference_mismatch",
    "payload_serve_workload_denied",
    "payload_serve_storage_unavailable",
)


def upgrade() -> None:
    op.execute("""CREATE TABLE ai_messaging_observations (
        kind VARCHAR(64) COLLATE ascii_bin NOT NULL PRIMARY KEY,
        recorded_count BIGINT UNSIGNED NOT NULL,
        recording_since DATETIME(6) NOT NULL,
        last_observed_at DATETIME(6)
    ) ENGINE=InnoDB CHARSET=utf8mb4""")
    for kind in KINDS:
        op.execute(
            "INSERT INTO ai_messaging_observations "
            "(kind,recorded_count,recording_since) "
            f"VALUES ('{kind}',0,UTC_TIMESTAMP(6))"
        )


def downgrade() -> None:
    raise RuntimeError("Retain technical observations; use a compatible image")
