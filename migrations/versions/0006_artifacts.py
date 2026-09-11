"""Immutable accepted interpretation artifacts."""

from alembic import op

revision = "0006_artifacts"
down_revision = "0005_model_calls"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""CREATE TABLE interpretation_artifacts (
        id VARCHAR(36) COLLATE utf8mb4_bin NOT NULL PRIMARY KEY,
        session_id VARCHAR(36) COLLATE utf8mb4_bin NOT NULL UNIQUE,
        run_id VARCHAR(36) COLLATE utf8mb4_bin NOT NULL UNIQUE,
        payload JSON NOT NULL,
        created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
        FOREIGN KEY (session_id) REFERENCES interpretation_sessions(id),
        FOREIGN KEY (run_id) REFERENCES interpretation_runs(id)
    ) ENGINE=InnoDB CHARSET=utf8mb4""")


def downgrade() -> None:
    op.drop_table("interpretation_artifacts")
