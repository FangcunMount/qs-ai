"""One durable provider dispatch per interpretation run."""

from alembic import op

revision = "0005_model_calls"
down_revision = "0004_delivery"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""CREATE TABLE model_calls (
        run_id VARCHAR(36) COLLATE utf8mb4_bin NOT NULL PRIMARY KEY,
        invocation_id VARCHAR(36) COLLATE utf8mb4_bin NOT NULL UNIQUE,
        fence_token BIGINT UNSIGNED NOT NULL,
        status VARCHAR(32) NOT NULL,
        request_json LONGTEXT NOT NULL,
        response_json LONGTEXT,
        failure_code VARCHAR(64),
        created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
        FOREIGN KEY (run_id) REFERENCES interpretation_runs(id)
    ) ENGINE=InnoDB CHARSET=utf8mb4""")


def downgrade() -> None:
    op.drop_table("model_calls")
