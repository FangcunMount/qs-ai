"""Current transaction state sequence metadata; importing never installs schema."""

import sqlalchemy as sa

from qs_ai.infrastructure.persistence.mysql.messaging import metadata

evaluation_sequences = sa.Table(
    "messaging_evaluation_sequences",
    metadata,
    sa.Column("run_id", sa.CHAR(36), primary_key=True),
    sa.Column("sequence", sa.BigInteger, nullable=False),
    sa.Column("version", sa.BigInteger, nullable=False),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
)
