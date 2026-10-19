"""Host-owned SDK v1 schema metadata, separate from business Outbox metadata.

Import is inert; only an explicit Alembic migration installs these tables.
No binding is inferred from credentials, database names or the current process.
"""

import sqlalchemy as sa
from sqlalchemy.dialects import mysql

metadata = sa.MetaData()
BINARY = mysql.BINARY(32)
IDENTITY = mysql.VARBINARY(128)
STATE = mysql.VARBINARY(16)
UTC = mysql.DATETIME(fsp=6)

binding = sa.Table(
    "rm_tx_binding",
    metadata,
    sa.Column("owner", IDENTITY, primary_key=True),
    sa.Column("store_id", IDENTITY, nullable=False),
    sa.Column("writer_epoch", IDENTITY, nullable=False),
    sa.Column("schema_version", sa.Integer, nullable=False),
    mysql_engine="InnoDB",
)
request = sa.Table(
    "rm_tx_request",
    metadata,
    sa.Column("request_key", BINARY, primary_key=True),
    sa.Column("owner", IDENTITY, nullable=False),
    sa.Column("scope", mysql.BLOB, nullable=False),
    sa.Column("idempotency_key", mysql.BLOB, nullable=False),
    sa.Column("input_hash", BINARY, nullable=False),
    sa.Column("active_execution", BINARY, nullable=False),
    mysql_engine="InnoDB",
)
execution = sa.Table(
    "rm_tx_execution",
    metadata,
    sa.Column("execution_key", BINARY, primary_key=True),
    sa.Column("request_key", BINARY, nullable=False),
    sa.Column("owner", IDENTITY, nullable=False),
    sa.Column("operation_id", mysql.BLOB, nullable=False),
    sa.Column("writer_epoch", IDENTITY, nullable=False),
    sa.Column("fence", sa.BigInteger, nullable=False),
    sa.Column("token", BINARY, nullable=False),
    sa.Column("plan", mysql.LONGBLOB, nullable=False),
    sa.Column("state", STATE, nullable=False),
    sa.Column("lease_until", UTC, nullable=False),
    sa.Column("guard_version", sa.BigInteger, nullable=False, server_default="0"),
    sa.Column("prepare_started", sa.Boolean, nullable=False, server_default=sa.false()),
    sa.Index("rm_tx_expiry", "owner", "state", "lease_until"),
    mysql_engine="InnoDB",
)
witness = sa.Table(
    "rm_tx_witness",
    metadata,
    sa.Column("execution_key", BINARY, primary_key=True),
    sa.Column("branch_key", BINARY, primary_key=True),
    sa.Column("reference", mysql.BLOB, nullable=False),
    sa.Column("state", STATE, nullable=False),
    sa.Column("broker_message_id", mysql.VARBINARY(512)),
    sa.Column("broker_transaction_id", mysql.VARBINARY(4096)),
    sa.Column("half_outcome", STATE, nullable=False, server_default="not_attempted"),
    sa.Column("final_outcome", mysql.VARBINARY(32), nullable=False, server_default="not_attempted"),
    sa.Column("half_error_code", IDENTITY, nullable=False, server_default=""),
    sa.Column("final_error_code", IDENTITY, nullable=False, server_default=""),
    sa.Column("half_recorded_at", UTC),
    sa.Column("final_recorded_at", UTC),
    mysql_engine="InnoDB",
)

# Host business archive; protocol tables above remain byte-for-byte SDK schema.
archive = sa.Table(
    "messaging_archive",
    metadata,
    sa.Column("producer", IDENTITY, primary_key=True),
    sa.Column("destination", IDENTITY, primary_key=True),
    sa.Column("message_id", IDENTITY, primary_key=True),
    sa.Column("organization_id", mysql.BIGINT(unsigned=True), nullable=False),
    sa.Column("frozen_payload", mysql.LONGBLOB, nullable=False),
    sa.Column("first_wire", mysql.MEDIUMBLOB, nullable=False),
    sa.Column("execution_key", BINARY, nullable=False),
    sa.Column("branch_key", BINARY, nullable=False),
    sa.Column("reference", mysql.BLOB, nullable=False),
    sa.Index("messaging_archive_branch", "execution_key", "branch_key", unique=True),
    mysql_engine="InnoDB",
)
