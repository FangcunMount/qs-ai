"""Persistent solution editing and original-command receipts; no publication changes."""

from alembic import op

revision = "0029_solution_revisions"
down_revision = "0028_execution_leases"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""CREATE TABLE solution_revisions (
        solution_id VARCHAR(36) COLLATE utf8mb4_bin NOT NULL PRIMARY KEY,
        organization_id BIGINT UNSIGNED NOT NULL,
        revision BIGINT NOT NULL,
        draft_id CHAR(36) NOT NULL UNIQUE,
        state_json LONGTEXT NOT NULL,
        state_sha256 VARCHAR(64) NOT NULL,
        INDEX ix_solution_org (organization_id, solution_id),
        FOREIGN KEY (draft_id) REFERENCES prompt_drafts(draft_id)
    ) ENGINE=InnoDB CHARSET=utf8mb4""")
    op.execute("""CREATE TABLE solution_commands (
        command_id VARCHAR(36) COLLATE utf8mb4_bin NOT NULL PRIMARY KEY,
        solution_id VARCHAR(36) COLLATE utf8mb4_bin NOT NULL,
        organization_id BIGINT UNSIGNED NOT NULL,
        operator_user_id BIGINT UNSIGNED NOT NULL,
        request_json LONGTEXT NOT NULL,
        receipt_json LONGTEXT NOT NULL,
        receipt_sha256 VARCHAR(64) NOT NULL,
        INDEX ix_solution_commands (solution_id),
        FOREIGN KEY (solution_id) REFERENCES solution_revisions(solution_id)
    ) ENGINE=InnoDB CHARSET=utf8mb4""")


def downgrade() -> None:
    op.drop_table("solution_commands")
    op.drop_table("solution_revisions")
