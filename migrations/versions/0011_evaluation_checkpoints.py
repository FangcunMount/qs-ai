"""Version-fenced evaluation in-flight checkpoints; not a full evaluation aggregate."""

from alembic import op

revision = "0011_evaluation_checkpoints"
down_revision = "0010_schema_assets"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""CREATE TABLE evaluation_checkpoints (
        run_id CHAR(36) NOT NULL PRIMARY KEY,
        version BIGINT NOT NULL,
        checkpoint_json JSON NULL
    ) ENGINE=InnoDB CHARSET=utf8mb4""")


def downgrade() -> None:
    op.drop_table("evaluation_checkpoints")
