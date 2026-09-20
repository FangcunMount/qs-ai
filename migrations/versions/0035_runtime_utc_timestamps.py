"""Explicit UTC evidence; never infer or rewrite historical database-local times."""

from alembic import op

revision = "0035_runtime_utc_timestamps"
down_revision = "0034_evaluation_suite_sources"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""ALTER TABLE interpretation_sessions
        ADD created_at_utc DATETIME(6) NULL,
        ADD updated_at_utc DATETIME(6) NULL""")
    op.execute("ALTER TABLE model_calls ADD created_at_utc DATETIME(6) NULL")


def downgrade() -> None:
    raise RuntimeError("Preserve UTC evidence; roll back code without downgrading schema")
