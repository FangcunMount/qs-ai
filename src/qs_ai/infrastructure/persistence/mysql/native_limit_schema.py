"""Host governance metadata; importing does not install or alter any schema."""

import sqlalchemy as sa
from sqlalchemy.dialects import mysql

metadata = sa.MetaData()
notices = sa.Table(
    "messaging_native_limit_notices",
    metadata,
    sa.Column("notice_key", mysql.BINARY(32), primary_key=True),
    sa.Column("owner", mysql.VARBINARY(128), nullable=False),
    sa.Column("store_id", mysql.VARBINARY(128), nullable=False),
    sa.Column("writer_epoch", mysql.VARBINARY(128), nullable=False),
    sa.Column("route_revision", mysql.VARBINARY(128), nullable=False),
    sa.Column("native_index_sha256", mysql.BINARY(32), nullable=False),
    sa.Column("evidence_sha256", mysql.BINARY(32), nullable=False),
    sa.Column("status", mysql.VARBINARY(32), nullable=False),
    sa.Column("schema_version", sa.Integer, nullable=False, server_default="1"),
    sa.Column("recorded_at", mysql.DATETIME(fsp=6), nullable=False),
    sa.Column("resolved_at", mysql.DATETIME(fsp=6)),
    sa.Column("resolution_receipt_sha256", mysql.BINARY(32)),
    sa.Index("messaging_native_limit_owner", "owner", "status", "notice_key"),
    sa.CheckConstraint("schema_version=1", name="chk_native_limit_version"),
    sa.CheckConstraint("status IN ('blocked','resolved')", name="chk_native_limit_state"),
    sa.CheckConstraint(
        "(status='blocked' AND resolved_at IS NULL AND resolution_receipt_sha256 IS NULL) "
        "OR (status='resolved' AND resolved_at IS NOT NULL "
        "AND resolution_receipt_sha256 IS NOT NULL "
        "AND resolution_receipt_sha256<>UNHEX(REPEAT('00',32)))",
        name="chk_native_limit_resolution",
    ),
    mysql_engine="InnoDB",
)
