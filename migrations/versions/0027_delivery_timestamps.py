"""Record first event creation and acknowledged delivery without inventing old times."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

revision = "0027_delivery_timestamps"
down_revision = "0026_participant_retries"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Existing events retain NULL: migration time is not their creation or delivery time.
    op.add_column("result_outbox", sa.Column("created_at", mysql.DATETIME(fsp=6), nullable=True))
    op.add_column("result_outbox", sa.Column("delivered_at", mysql.DATETIME(fsp=6), nullable=True))


def downgrade() -> None:
    op.drop_column("result_outbox", "delivered_at")
    op.drop_column("result_outbox", "created_at")
