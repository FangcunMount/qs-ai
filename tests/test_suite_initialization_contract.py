"""Source identities are not inferred from a schema version prefix."""

from qs_ai.bootstrap.import_evaluation_suites import baseline
from qs_ai.infrastructure.qs_server.semantic_assets import load_semantic_assets


def test_initializer_preserves_original_semantic_schema_identity():
    _, _, refs = baseline()
    semantic = load_semantic_assets()
    assert refs.semantic_prompt == semantic.prompt
    assert refs.semantic_output_schema == semantic.output_schema
