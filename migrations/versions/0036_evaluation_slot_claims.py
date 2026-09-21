"""Add candidate execution ownership without converting existing Runs."""

from alembic import op

revision = "0036_evaluation_slot_claims"
down_revision = "0035_runtime_utc_timestamps"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE evaluation_runs ADD execution_mode VARCHAR(32) NOT NULL DEFAULT 'serial_v1'"
    )
    op.execute("""CREATE TABLE evaluation_slot_claims (
        run_id CHAR(36) NOT NULL,
        case_id VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,
        slot_ordinal INT NOT NULL,
        version BIGINT NOT NULL,
        checkpoint_json JSON NOT NULL,
        PRIMARY KEY (run_id, case_id, slot_ordinal),
        FOREIGN KEY (run_id) REFERENCES evaluation_runs(run_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")
    op.execute("""CREATE TABLE evaluation_response_receipts (
        run_id CHAR(36) NOT NULL,
        invocation_id VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,
        execution_id VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,
        claim_version BIGINT NOT NULL,
        definition_json LONGTEXT NOT NULL,
        sha256 CHAR(64) NOT NULL,
        PRIMARY KEY (run_id, invocation_id),
        CONSTRAINT uq_evaluation_response_execution UNIQUE (run_id, execution_id),
        FOREIGN KEY (run_id) REFERENCES evaluation_runs(run_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")


def downgrade() -> None:
    raise RuntimeError(
        "Preserve candidate execution evidence; use a compatible application rollback"
    )
