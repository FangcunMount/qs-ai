"""Reconcile imported baseline bytes and Profile-to-Prompt references, without activation."""

import json
from dataclasses import dataclass

from qs_ai.application.interpretation.profile_assets import ProfileAssets
from qs_ai.application.interpretation.prompt_assets import PromptAssets
from qs_ai.domain.governance.profile import ProfileAsset
from qs_ai.domain.governance.prompt import PromptAsset


@dataclass(frozen=True)
class AssetMismatch:
    kind: str
    identity: str
    version: str
    reason: str


async def audit_assets(
    profiles: ProfileAssets,
    prompts: PromptAssets,
    expected_profiles: tuple[ProfileAsset, ...],
    expected_prompts: tuple[PromptAsset, ...],
) -> tuple[AssetMismatch, ...]:
    mismatches = []
    for expected in expected_prompts:
        actual_prompt = await prompts.get(expected.template_id, expected.version)
        if actual_prompt != expected:
            mismatches.append(
                AssetMismatch(
                    "prompt",
                    expected.template_id,
                    expected.version,
                    "missing" if actual_prompt is None else "content_mismatch",
                )
            )
    prompt_refs = {(a.template_id, a.version) for a in expected_prompts}
    for expected_profile in expected_profiles:
        actual_profile = await profiles.get(expected_profile.profile_id, expected_profile.version)
        if actual_profile != expected_profile:
            mismatches.append(
                AssetMismatch(
                    "profile",
                    expected_profile.profile_id,
                    expected_profile.version,
                    "missing" if actual_profile is None else "content_mismatch",
                )
            )
        policy = json.loads(expected_profile.definition_json)["generation_policy"]
        reference = (policy["prompt_template_id"], policy["prompt_version"])
        if reference not in prompt_refs:
            mismatches.append(
                AssetMismatch(
                    "profile",
                    expected_profile.profile_id,
                    expected_profile.version,
                    "prompt_not_in_baseline",
                )
            )
    return tuple(mismatches)
