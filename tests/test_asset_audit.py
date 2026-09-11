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
    del by_ref[(expected_prompts[1].template_id, expected_prompts[1].version)]
    profile_store.get.return_value = None
    failures = await audit_assets(profile_store, prompt_store, expected_profiles, expected_prompts)
    assert [(f.kind, f.reason) for f in failures] == [
        ("prompt", "content_mismatch"),
        ("prompt", "missing"),
        ("profile", "missing"),
    ]
    profile_store.get.return_value = expected_profiles[0]
    failures = await audit_assets(profile_store, prompt_store, expected_profiles, ())
    assert [f.reason for f in failures] == ["prompt_not_in_baseline"]
    profile_store.put.assert_not_called()
    prompt_store.put.assert_not_called()
