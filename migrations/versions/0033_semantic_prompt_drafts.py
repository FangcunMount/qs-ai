"""Organization-owned semantic Prompt revision history and immutable command receipts."""

from alembic import op

revision = "0033_semantic_prompt_drafts"
down_revision = "0032_runtime_milestones"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""CREATE TABLE semantic_draft_heads (
        organization_id BIGINT UNSIGNED NOT NULL,
        draft_id CHAR(36) COLLATE utf8mb4_bin NOT NULL,
        revision BIGINT NOT NULL,
        PRIMARY KEY(organization_id, draft_id)
    ) ENGINE=InnoDB CHARSET=utf8mb4""")
    op.execute("""CREATE TABLE semantic_draft_versions (
        organization_id BIGINT UNSIGNED NOT NULL,
        draft_id CHAR(36) COLLATE utf8mb4_bin NOT NULL,
        revision BIGINT NOT NULL,
        snapshot_json LONGTEXT NOT NULL,
        snapshot_sha256 CHAR(64) NOT NULL,
        PRIMARY KEY(organization_id, draft_id, revision),
        FOREIGN KEY(organization_id, draft_id)
            REFERENCES semantic_draft_heads(organization_id, draft_id)
    ) ENGINE=InnoDB CHARSET=utf8mb4""")
    op.execute("""CREATE TABLE semantic_draft_commands (
        organization_id BIGINT UNSIGNED NOT NULL,
        command_id CHAR(36) COLLATE utf8mb4_bin NOT NULL,
        operator_user_id BIGINT UNSIGNED NOT NULL,
        draft_id CHAR(36) COLLATE utf8mb4_bin NOT NULL,
        request_json LONGTEXT NOT NULL,
        receipt_json LONGTEXT NOT NULL,
        receipt_sha256 CHAR(64) NOT NULL,
        PRIMARY KEY(organization_id, command_id),
        FOREIGN KEY(organization_id, draft_id)
            REFERENCES semantic_draft_heads(organization_id, draft_id)
    ) ENGINE=InnoDB CHARSET=utf8mb4""")


def downgrade() -> None:
    raise RuntimeError("Semantic draft history and receipts require preservation")
