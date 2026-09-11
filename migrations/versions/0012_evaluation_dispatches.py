"""Frozen run policy and append-only dispatch reservations."""

from alembic import op

revision = "0012_evaluation_dispatches"
down_revision = "0011_evaluation_checkpoints"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""CREATE TABLE evaluation_run_policies (
        run_id CHAR(36) NOT NULL PRIMARY KEY,
        fingerprint VARCHAR(71) NOT NULL,
        definition_json LONGTEXT NOT NULL
    ) ENGINE=InnoDB CHARSET=utf8mb4""")
    op.execute("""CREATE TABLE evaluation_dispatches (
        run_id CHAR(36) NOT NULL,
        invocation_id VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,
        kind VARCHAR(16) NOT NULL,
        case_id VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,
        slot_ordinal INTEGER NOT NULL,
        candidate_id VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,
        checkpoint_json JSON NOT NULL,
        PRIMARY KEY (run_id, invocation_id)
    ) ENGINE=InnoDB CHARSET=utf8mb4""")


def downgrade() -> None:
    op.drop_table("evaluation_dispatches")
    op.drop_table("evaluation_run_policies")
