"""Read an original frozen policy; absent legacy evidence remains absent."""

from typing import Any

from sqlalchemy import Select, select

from qs_ai.infrastructure.persistence.mysql.schema import evaluation_runs


def frozen_policy_query(run_id: str) -> Select[Any]:
    return select(
        evaluation_runs.c.run_id,
        evaluation_runs.c.frozen_execution_policy_fingerprint.label("fingerprint"),
        evaluation_runs.c.frozen_execution_policy_json.label("definition_json"),
    ).where(
        evaluation_runs.c.run_id == run_id,
        evaluation_runs.c.frozen_execution_policy_fingerprint.is_not(None),
        evaluation_runs.c.frozen_execution_policy_json.is_not(None),
    )
