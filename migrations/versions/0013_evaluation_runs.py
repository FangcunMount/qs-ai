"""Frozen Run creation records; checkpoint version is the aggregate version."""

from alembic import op

revision = "0013_evaluation_runs"
down_revision = "0012_evaluation_dispatches"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""CREATE TABLE evaluation_runs (
        run_id CHAR(36) NOT NULL PRIMARY KEY,
        organization_id BIGINT NOT NULL,
        requested_by VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,
        definition_json LONGTEXT NOT NULL
    ) ENGINE=InnoDB CHARSET=utf8mb4""")


def downgrade() -> None:
    op.drop_table("evaluation_runs")
