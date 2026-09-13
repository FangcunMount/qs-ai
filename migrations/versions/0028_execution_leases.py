"""Preserve production fencing while retiring the prototype checkpoint table name.

Stop execution workers before applying this migration; old workers use the old name.
"""

from alembic import op

revision = "0028_execution_leases"
down_revision = "0027_delivery_timestamps"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.rename_table("checkpoint_leases", "execution_leases")


def downgrade() -> None:
    op.rename_table("execution_leases", "checkpoint_leases")
