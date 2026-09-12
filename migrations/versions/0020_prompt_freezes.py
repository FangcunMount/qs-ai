"""Atomically bind a draft revision to its native immutable Prompt and command receipt."""

from alembic import op

revision = "0020_prompt_freezes"
down_revision = "0019_prompt_drafts"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""CREATE TABLE prompt_draft_freezes (
        command_id CHAR(36) NOT NULL PRIMARY KEY,
        draft_id CHAR(36) NOT NULL,
        organization_id BIGINT NOT NULL,
        operator_user_id BIGINT NOT NULL,
        receipt_json LONGTEXT NOT NULL,
        receipt_sha256 CHAR(64) NOT NULL,
        CONSTRAINT uq_prompt_draft_freeze UNIQUE (draft_id),
        FOREIGN KEY (draft_id) REFERENCES prompt_drafts(draft_id)
    ) ENGINE=InnoDB CHARSET=utf8mb4""")


def downgrade() -> None:
    op.drop_table("prompt_draft_freezes")
