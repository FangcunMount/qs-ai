"""Technical checkpoint leases; business task leases are introduced in P1."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

revision = "0002_checkpoint_leases"
down_revision = "0001_baseline"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "checkpoint_leases",
        sa.Column("thread_id", sa.String(191, collation="utf8mb4_bin"), primary_key=True),
        sa.Column("fence", mysql.BIGINT(unsigned=True), nullable=False),
        sa.Column("expires_at", mysql.DATETIME(fsp=6), nullable=False),
        mysql_engine="InnoDB",
        mysql_charset="utf8mb4",
    )


def downgrade() -> None:
    op.drop_table("checkpoint_leases")
