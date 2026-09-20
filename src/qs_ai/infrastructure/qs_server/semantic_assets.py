"""Frozen v2 judge instructions; no route selection, payload construction or model call."""

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path

from jsonschema import Draft202012Validator

from qs_ai.domain.evaluation.identity import FrozenContractRef
from qs_ai.infrastructure.qs_server.evaluation_policies import evaluation_directory

PROMPT_V2 = FrozenContractRef(
    "ai-explanation-semantic-evaluator",
    "v2",
    "sha256:1789c53a44e1be065f5d2691faacaeb65a0a264f9fb0107138c8c8004a8ab572",
)


@dataclass(frozen=True)
class SemanticAssets:
    prompt: FrozenContractRef
    output_schema: FrozenContractRef
    prompt_markdown: str
    output_schema_json: str
    system_message: str
    task_message: str
    data_preamble: str


def load_semantic_assets(*, directory: Path | None = None) -> SemanticAssets:
    directory = directory if directory is not None else evaluation_directory()
    manifest = json.loads((directory / "manifest.json").read_bytes())

    def verified(name: str) -> bytes:
        raw = (directory / name).read_bytes()
        if hashlib.sha256(raw).hexdigest() != manifest["files"][name]:
            raise ValueError("Semantic resource checksum mismatch")
        return raw

    raw_prompt = verified("ai-explanation-semantic-evaluator-prompt-v2.md")
    if "sha256:" + hashlib.sha256(raw_prompt).hexdigest() != PROMPT_V2.fingerprint:
        raise ValueError("Semantic prompt fingerprint mismatch")
    raw_schema = verified("ai-explanation-semantic-evaluation-output-v1.schema.json")
    reference = FrozenContractRef(
        "ai-explanation-semantic-output",
        "ai-explanation-semantic-evaluation-output/v1",
        "sha256:" + hashlib.sha256(raw_schema).hexdigest(),
    )
    return semantic_assets(raw_prompt.decode(), raw_schema.decode(), PROMPT_V2, reference)


def semantic_assets(
    markdown: str,
    output_schema_json: str,
    prompt: FrozenContractRef,
    output_schema: FrozenContractRef,
) -> SemanticAssets:
    if (
        "sha256:" + hashlib.sha256(markdown.encode()).hexdigest() != prompt.fingerprint
        or "sha256:" + hashlib.sha256(output_schema_json.encode()).hexdigest()
        != output_schema.fingerprint
    ):
        raise ValueError("Semantic asset fingerprint mismatch")
    blocks = re.findall(r"```text\n(.*?)\n```", markdown.replace("\r\n", "\n"), re.DOTALL)
    suffix = "\n\n{{semantic_evaluation_payload_json}}"
    if len(blocks) != 3 or not blocks[2].endswith(suffix):
        raise ValueError("Frozen semantic prompt blocks invalid")
    schema = json.loads(output_schema_json)
    Draft202012Validator.check_schema(schema)
    version = schema["properties"]["schema_version"]["const"]
    if (
        version != "ai-explanation-semantic-evaluation-output/v1"
        or output_schema.version != version
    ):
        raise ValueError("Unsupported semantic output schema")
    return SemanticAssets(
        prompt,
        output_schema,
        markdown,
        output_schema_json,
        blocks[0],
        blocks[1],
        blocks[2][: -len(suffix)],
    )
