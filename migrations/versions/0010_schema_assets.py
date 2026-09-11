"""Immutable imported Schema definitions, without runtime activation."""

from alembic import op

revision = "0010_schema_assets"
down_revision = "0009_route_assets"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""CREATE TABLE schema_assets (
        schema_id VARCHAR(255) COLLATE utf8mb4_0900_bin NOT NULL,
        version VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,
        fingerprint VARCHAR(71) COLLATE utf8mb4_bin NOT NULL,
        definition_json LONGTEXT COLLATE utf8mb4_bin NOT NULL,
        source_ref VARCHAR(255) COLLATE utf8mb4_bin NOT NULL,
        imported_by VARCHAR(255) COLLATE utf8mb4_bin NOT NULL,
        created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
        PRIMARY KEY (schema_id, version)
    ) ENGINE=InnoDB CHARSET=utf8mb4""")


def downgrade() -> None:
    op.drop_table("schema_assets")
