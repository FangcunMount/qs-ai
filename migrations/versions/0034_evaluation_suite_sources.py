"""Typed suite initialization and exact evaluation contract bindings; preserve existing bytes."""

from alembic import op

revision = "0034_evaluation_suite_sources"
down_revision = "0033_semantic_prompt_drafts"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""ALTER TABLE evaluation_suites
        MODIFY command_id CHAR(36) NULL,
        MODIFY operator_user_id BIGINT NULL,
        MODIFY receipt_json LONGTEXT NULL,
        MODIFY receipt_sha256 CHAR(64) NULL,
        ADD source_ref VARCHAR(255) NULL,
        ADD imported_by VARCHAR(128) NULL,
        ADD contracts_json LONGTEXT NULL,
        ADD contracts_sha256 CHAR(64) NULL""")


def downgrade() -> None:
    raise RuntimeError(
        "Preserve suite initialization and bindings; restore code without data deletion"
    )
