from dataclasses import replace
from unittest.mock import AsyncMock

from qs_ai.application.interpretation.asset_audit import audit_assets
from qs_ai.bootstrap.import_profiles import baseline_assets as profiles
from qs_ai.bootstrap.import_prompts import baseline_assets as prompts


async def test_reconciliation_detects_missing_content_and_unbundled_reference():
    expected_profiles = profiles()[1]
    expected_prompts = prompts()[1]
    profile_store, prompt_store = AsyncMock(), AsyncMock()
    profile_store.get.return_value = expected_profiles[0]
    by_ref = {(p.template_id, p.version): p for p in expected_prompts}
    prompt_store.get.side_effect = lambda identity, version: by_ref.get((identity, version))
    assert not await audit_assets(profile_store, prompt_store, expected_profiles, expected_prompts)
    # A valid but byte-different package must not count as migration equality.
    original = expected_prompts[0]
    import hashlib

    raw = original.package_json + " "
    by_ref[(original.template_id, original.version)] = replace(
        original, package_json=raw, package_sha256=hashlib.sha256(raw.encode()).hexdigest()
    )
    profile_store.get.return_value = None
    failures = await audit_assets(profile_store, prompt_store, expected_profiles, expected_prompts)
    assert [(f.kind, f.reason) for f in failures] == [
        ("prompt", "content_mismatch"),
        ("profile", "missing"),
    ]
    by_ref.clear()
    assert (await audit_assets(profile_store, prompt_store, expected_profiles, expected_prompts))[
        0
    ].reason == "missing"
    profile_store.get.return_value = expected_profiles[0]
    failures = await audit_assets(profile_store, prompt_store, expected_profiles, ())
    assert [f.reason for f in failures] == ["prompt_not_in_baseline"]
    profile_store.put.assert_not_called()
    prompt_store.put.assert_not_called()


async def test_route_reconciliation_requires_revision_and_profile_binding():
    from qs_ai.application.interpretation.asset_audit import audit_routes
    from qs_ai.bootstrap.import_routes import baseline_assets

    expected_routes = baseline_assets()[1]
    store = AsyncMock()
    store.get.return_value = expected_routes[0]
    assert not await audit_routes(store, expected_routes, profiles()[1])
    store.get.assert_awaited_once_with("balanced_text_v1", "v8")
    store.get.return_value = None
    assert [f.reason for f in await audit_routes(store, expected_routes, profiles()[1])] == [
        "missing"
    ]
    assert [f.reason for f in await audit_routes(store, (), profiles()[1])] == [
        "route_not_in_baseline"
    ]
    store.put.assert_not_called()


async def test_schema_reconciliation_requires_both_profile_contract_versions():
    from qs_ai.application.interpretation.asset_audit import audit_schemas
    from qs_ai.bootstrap.import_schemas import baseline_assets

    expected = baseline_assets()[1]
    store = AsyncMock()
    by_key = {(a.schema_id, a.version): a for a in expected}
    store.get.side_effect = lambda identity, version: by_key.get((identity, version))
    assert not await audit_schemas(store, expected, profiles()[1])
    by_key.clear()
    assert [f.reason for f in await audit_schemas(store, expected, profiles()[1])] == [
        "missing",
        "missing",
    ]
    assert [f.reason for f in await audit_schemas(store, (), profiles()[1])] == [
        "input_schema_version_not_in_baseline",
        "output_schema_version_not_in_baseline",
    ]
    store.put.assert_not_called()


async def test_referenced_audit_ignores_retired_prompts_but_still_blocks_missing_active_prompt(
    monkeypatch,
):
    from types import SimpleNamespace

    from pydantic import SecretStr

    from qs_ai.bootstrap import audit_assets as cli

    selected_profile = profiles()[1][0]
    selected_prompt = next(p for p in prompts()[1] if p.version == "v6")
    profile_store, prompt_store, route_store, schema_store = (
        AsyncMock(),
        AsyncMock(),
        AsyncMock(),
        AsyncMock(),
    )
    profile_store.get.return_value = selected_profile
    prompt_store.get.side_effect = lambda identity, version: (
        selected_prompt if (identity, version) == (selected_prompt.template_id, "v6") else None
    )
    route_store.get.return_value = cli.route_baseline()[1][0]
    schemas = {(s.schema_id, s.version): s for s in cli.schema_baseline()[1]}
    schema_store.get.side_effect = lambda identity, version: schemas.get((identity, version))
    db = AsyncMock()
    monkeypatch.setattr(
        cli, "Settings", lambda: SimpleNamespace(database_url=SecretStr("test-only"))
    )
    monkeypatch.setattr(cli, "Database", lambda _: db)
    monkeypatch.setattr(cli, "Transactions", lambda _: object())
    for name, store in (
        ("MySQLProfileAssets", profile_store),
        ("MySQLPromptAssets", prompt_store),
        ("MySQLRouteAssets", route_store),
        ("MySQLSchemaAssets", schema_store),
    ):
        monkeypatch.setattr(cli, name, lambda _, store=store: store)
    selected = await cli.run(referenced_only=True)
    assert selected["audit"] == "matched" and selected["prompts_checked"] == 1
    assert selected["scope"] == "fixed_published_profile_references"
    assert selected["current_production_inventory_verified"] is False
    assert selected["activated"] is False
    full = await cli.run()
    assert full["audit"] == "matched" and full["prompts_checked"] == 1
    assert full["mismatches"] == []
    prompt_store.get.side_effect = None
    prompt_store.get.return_value = None
    missing = await cli.run(referenced_only=True)
    assert missing["audit"] == "mismatch"
    assert missing["mismatches"] == [
        {
            "kind": "prompt",
            "identity": selected_prompt.template_id,
            "version": "v6",
            "reason": "missing",
        }
    ]
    for store in (profile_store, prompt_store, route_store, schema_store):
        store.put.assert_not_called()
