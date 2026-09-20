import json
from dataclasses import replace

import pytest

from qs_ai.application.governance.asset_references import ReferenceQuery
from qs_ai.application.governance.quotas import default_baseline
from qs_ai.config import Settings
from qs_ai.domain.evaluation.identity import FrozenContractRef
from qs_ai.infrastructure.persistence.mysql.configuration_status import deployment_status


def test_secret_changes_never_change_public_projection_or_hashes():
    first = Settings(
        database_url="mysql+asyncmy://user:first-private@internal/db",
        model_api_key="secret-one",
        generation={"endpoint": "https://hidden-provider"},
        grpc={"ca_file": "/secret/ca", "cert_file": "/secret/cert", "key_file": "/secret/key"},
    )
    second = first.model_copy(update={"database_url": None, "model_api_key": None})
    value = deployment_status(first, default_baseline())
    assert value == deployment_status(second, default_baseline())
    raw = json.dumps(value)
    for secret in ("first-private", "internal", "secret-one", "hidden-provider", "/secret/"):
        assert secret not in raw
    changed = first.model_copy(update={"governance_models": ("new-verified-model",)})
    assert value != deployment_status(changed, default_baseline())


def test_reference_cursor_is_bound_to_org_and_full_query():
    query = ReferenceQuery(
        "execution_policy", FrozenContractRef("policy", "v1", "sha256:" + "a" * 64), "suite"
    )
    cursor = query.next_cursor(1, "suite", "v1")
    assert replace(query, cursor=cursor).after(1) == ("suite", "v1")
    for changed, org in (
        (query, 2),
        (replace(query, usage_kind="evaluation"), 1),
        (replace(query, reference=replace(query.reference, version="v2")), 1),
    ):
        with pytest.raises(ValueError):
            replace(changed, cursor=cursor).after(org)
