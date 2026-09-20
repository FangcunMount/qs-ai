"""Transactional execution diagnostics; no business evidence is retired."""

from alembic import op

revision = "0032_runtime_milestones"
down_revision = "0031_evaluation_policy_assets"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""CREATE TABLE runtime_milestones (
        session_id VARCHAR(36) COLLATE utf8mb4_bin NOT NULL,
        run_id VARCHAR(36) COLLATE utf8mb4_bin NOT NULL,
        dedupe_key VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,
        kind VARCHAR(32) NOT NULL,
        invocation_id VARCHAR(36) COLLATE utf8mb4_bin NULL,
        attempt INT NULL,
        occurred_at DATETIME(6) NOT NULL,
        expires_at DATETIME(6) NOT NULL,
        PRIMARY KEY (session_id, dedupe_key),
        INDEX ix_runtime_milestones_expiry (expires_at),
        FOREIGN KEY (session_id) REFERENCES interpretation_sessions(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")


def downgrade() -> None:
    raise RuntimeError("Preserve diagnostic evidence; roll back code without downgrading schema")
