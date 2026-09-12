import base64
import json

import pytest

from qs_ai.application.governance.asset_catalog import CatalogQuery
from qs_ai.domain.governance.manifest import AssetReference


def ref(identity="中文/测评", version="v2"):
    return AssetReference(identity, version, "sha256:" + "a" * 64, "b" * 64)


def test_cursor_round_trip_and_binding_to_kind_and_exact_filter():
    query = CatalogQuery("profile", "中文/测评")
    cursor = query.next_cursor(ref())
    assert CatalogQuery("profile", "中文/测评", 1, cursor).after() == ("中文/测评", "v2")
    for changes in (
        {"kind": "prompt", "identity": "中文/测评"},
        {"kind": "profile", "identity": ""},
    ):
        with pytest.raises(ValueError):
            CatalogQuery(**changes, cursor=cursor)


@pytest.mark.parametrize(
    "changes",
    [
        {"kind": "receipts"},
        {"limit": 0},
        {"limit": -1},
        {"limit": 51},
        {"limit": True},
        {"identity": " "},
        {"identity": "a" * 256},
        {"identity": " a"},
        {"cursor": "a" * 4097},
        {"cursor": "%bad"},
    ],
)
def test_invalid_query_rejected_before_store(changes):
    with pytest.raises(ValueError):
        CatalogQuery(**{"kind": "profile", **changes})


@pytest.mark.parametrize(
    "value",
    [
        None,
        {},
        [1, "profile", "", "ok", 1],
        [1, "profile", "", "ok", " "],
        [2, "profile", "", "ok", "v1"],
        [1, "profile", "filter", "other", "v1"],
        [1, "profile", "", "", "v1"],
    ],
)
def test_malformed_decoded_cursor(value):
    cursor = base64.urlsafe_b64encode(json.dumps(value).encode()).decode()
    with pytest.raises(ValueError):
        CatalogQuery("profile", cursor=cursor)
