"""Add immutable evaluation policy and semantic prompt assets; no activation or data deletion."""

from alembic import op

revision = "0031_evaluation_policy_assets"
down_revision = "0030_organization_quotas"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""CREATE TABLE evaluation_policy_assets (
        kind VARCHAR(16) COLLATE utf8mb4_bin NOT NULL,
        asset_id VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,
        version VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,
        fingerprint VARCHAR(71) COLLATE utf8mb4_bin NOT NULL,
        definition_json LONGTEXT NOT NULL,
        source_ref VARCHAR(255) NOT NULL,
        imported_by VARCHAR(255) NOT NULL,
        created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        PRIMARY KEY(kind, asset_id, version),
        CONSTRAINT ck_evaluation_policy_kind CHECK(kind IN ('execution', 'gate'))
    ) ENGINE=InnoDB CHARSET=utf8mb4""")
    op.execute("""CREATE TABLE semantic_prompt_assets (
        organization_id BIGINT UNSIGNED NOT NULL,
        asset_id VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,
        version VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,
        fingerprint VARCHAR(71) COLLATE utf8mb4_bin NOT NULL,
        markdown LONGTEXT NOT NULL,
        source_ref VARCHAR(255) NOT NULL,
        imported_by VARCHAR(255) NOT NULL,
        created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        PRIMARY KEY(organization_id, asset_id, version)
    ) ENGINE=InnoDB CHARSET=utf8mb4""")


def downgrade() -> None:
    raise RuntimeError("Evaluation assets must be preserved for historical evaluation recovery")
