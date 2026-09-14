"""Explicit baseline preparation for synthetic tests; never packaged with the service."""

from qs_ai.infrastructure.qs_server.evaluation_case import prepare_asset_evaluation_case
from qs_ai.infrastructure.qs_server.profiles import load_migrated_release
from qs_ai.infrastructure.qs_server.prompts import load_prompt
from qs_ai.infrastructure.qs_server.semantic_input import prepare_semantic_messages as messages


def prepare_evaluation_case(release, case_id):
    return prepare_asset_evaluation_case(
        release,
        case_id,
        load_migrated_release(release.profile.id, release.profile.version),
        load_prompt(release.prompt.id, release.prompt.version),
    )


def prepare_semantic_messages(release, generation, assertions):
    return messages(
        release,
        generation,
        assertions,
        prepared=prepare_evaluation_case(release, generation.case_id),
    )
