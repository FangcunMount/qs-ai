"""Immutable imported Prompt definitions, without runtime activation."""

from alembic import op

revision = "0008_prompt_assets"
down_revision = "0007_profile_assets"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""CREATE TABLE prompt_assets (
        template_id VARCHAR(255) COLLATE utf8mb4_0900_bin NOT NULL,
        version VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,
        fingerprint VARCHAR(71) COLLATE utf8mb4_bin NOT NULL,
        package_sha256 VARCHAR(64) COLLATE utf8mb4_bin NOT NULL,
        package_json LONGTEXT COLLATE utf8mb4_bin NOT NULL,
        source_ref VARCHAR(255) COLLATE utf8mb4_bin NOT NULL,
        imported_by VARCHAR(255) COLLATE utf8mb4_bin NOT NULL,
        created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
        PRIMARY KEY (template_id, version)
    ) ENGINE=InnoDB CHARSET=utf8mb4""")


def downgrade() -> None:
    op.drop_table("prompt_assets")
