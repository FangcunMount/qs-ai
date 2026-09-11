"""Mutable progress separate from immutable Run creation evidence."""

from alembic import op

revision = "0014_evaluation_progress"
down_revision = "0013_evaluation_runs"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE evaluation_runs ADD COLUMN progress_json JSON NULL")


def downgrade() -> None:
    op.drop_column("evaluation_runs", "progress_json")
