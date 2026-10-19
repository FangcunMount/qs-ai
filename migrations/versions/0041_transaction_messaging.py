"""Install separate transaction evidence; never infer or migrate old Outbox proof.

Frozen SDK schema v1 from 0.4.0a1 / bfaae2b. Binding values are explicitly
installed by the host under a separately frozen owner/store/epoch manifest.
"""

from alembic import op

revision = "0041_transaction_messaging"
down_revision = "0040_module_table_names"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for statement in DDL:
        op.execute(statement)


def downgrade() -> None:
    raise RuntimeError("Preserve transaction evidence; use a compatible forward repair")


DDL = (
    """-- Explicit host migration template. No constructor executes this DDL.
CREATE TABLE rm_tx_binding (
 owner VARBINARY(128) NOT NULL PRIMARY KEY,
 store_id VARBINARY(128) NOT NULL,
 writer_epoch VARBINARY(128) NOT NULL,
 schema_version INT NOT NULL
) ENGINE=InnoDB""",
    """CREATE TABLE rm_tx_request (
 request_key BINARY(32) NOT NULL PRIMARY KEY,
 owner VARBINARY(128) NOT NULL,
 scope BLOB NOT NULL,
 idempotency_key BLOB NOT NULL,
 input_hash BINARY(32) NOT NULL,
 active_execution BINARY(32) NOT NULL
) ENGINE=InnoDB""",
    """CREATE TABLE rm_tx_execution (
 execution_key BINARY(32) NOT NULL PRIMARY KEY,
 request_key BINARY(32) NOT NULL,
 owner VARBINARY(128) NOT NULL,
 operation_id BLOB NOT NULL,
 writer_epoch VARBINARY(128) NOT NULL,
 fence BIGINT NOT NULL,
 token BINARY(32) NOT NULL,
 plan LONGBLOB NOT NULL,
 state VARBINARY(16) NOT NULL,
 lease_until DATETIME(6) NOT NULL,
 guard_version BIGINT NOT NULL DEFAULT 0,
 prepare_started BOOLEAN NOT NULL DEFAULT FALSE,
 INDEX rm_tx_expiry(owner,state,lease_until)
) ENGINE=InnoDB""",
    """CREATE TABLE rm_tx_witness (
 execution_key BINARY(32) NOT NULL,
 branch_key BINARY(32) NOT NULL,
 reference BLOB NOT NULL,
 state VARBINARY(16) NOT NULL,
 broker_message_id VARBINARY(512) NULL,
 broker_transaction_id VARBINARY(4096) NULL,
 half_outcome VARBINARY(16) NOT NULL DEFAULT 'not_attempted',
 final_outcome VARBINARY(32) NOT NULL DEFAULT 'not_attempted',
 half_error_code VARBINARY(128) NOT NULL DEFAULT '',
 final_error_code VARBINARY(128) NOT NULL DEFAULT '',
 half_recorded_at DATETIME(6) NULL,
 final_recorded_at DATETIME(6) NULL,
 PRIMARY KEY(execution_key,branch_key)
) ENGINE=InnoDB""",
    """CREATE TABLE messaging_archive (
 producer VARBINARY(128) NOT NULL,
 destination VARBINARY(128) NOT NULL,
 message_id VARBINARY(128) NOT NULL,
 organization_id BIGINT UNSIGNED NOT NULL,
 frozen_payload LONGBLOB NOT NULL,
 first_wire MEDIUMBLOB NOT NULL,
 execution_key BINARY(32) NOT NULL,
 branch_key BINARY(32) NOT NULL,
 reference BLOB NOT NULL,
 PRIMARY KEY(producer,destination,message_id),
 UNIQUE KEY messaging_archive_branch(execution_key,branch_key)
) ENGINE=InnoDB""",
)
