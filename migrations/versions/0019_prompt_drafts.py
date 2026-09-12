"""Keep current draft revisions and append-only content/command audit separately."""

from alembic import op

revision = "0019_prompt_drafts"
down_revision = "0018_execution_configurations"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""CREATE TABLE prompt_drafts (
        draft_id CHAR(36) NOT NULL PRIMARY KEY,
        organization_id BIGINT NOT NULL,
        revision BIGINT NOT NULL
    ) ENGINE=InnoDB CHARSET=utf8mb4""")
    op.execute("""CREATE TABLE prompt_draft_revisions (
        command_id CHAR(36) NOT NULL PRIMARY KEY,
        draft_id CHAR(36) NOT NULL,
        revision BIGINT NOT NULL,
        organization_id BIGINT NOT NULL,
        operator_user_id BIGINT NOT NULL,
        request_json LONGTEXT NOT NULL,
        snapshot_json LONGTEXT NOT NULL,
        snapshot_sha256 CHAR(64) NOT NULL,
        CONSTRAINT uq_prompt_draft_revision UNIQUE (draft_id, revision),
        FOREIGN KEY (draft_id) REFERENCES prompt_drafts(draft_id)
    ) ENGINE=InnoDB CHARSET=utf8mb4""")


def downgrade() -> None:
    op.drop_table("prompt_draft_revisions")
    op.drop_table("prompt_drafts")
