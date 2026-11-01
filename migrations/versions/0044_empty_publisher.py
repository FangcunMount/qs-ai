"""Retire only the empty legacy publisher; preserve native and business history."""

from alembic import context, op

from qs_ai.maintenance.empty_publisher_retirement import retire

revision = "0044_empty_publisher"
down_revision = "0043_native_transaction_limit"
branch_labels = None
depends_on = None


def upgrade() -> None:
    args = context.get_x_argument(as_dictionary=True)
    retire(
        op.get_bind(),
        fresh=context.config.attributes.get("empty_publisher_fresh_install") is True,
        review_path=args.get("empty_publisher_review"),
        reviewed_sha256=args.get("reviewed_sha256"),
    )


def downgrade() -> None:
    raise RuntimeError("Preserve new native facts and ledger; use reviewed forward repair")
