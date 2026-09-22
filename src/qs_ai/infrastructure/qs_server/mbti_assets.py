"""Controlled MBTI initializer input; never used as a runtime asset fallback."""

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path

from qs_ai.application.interpretation.prompt_assets import executable_prompt
from qs_ai.application.interpretation.prompts import render_prompt
from qs_ai.domain.evaluation.assets import SemanticPromptAsset
from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity, FrozenContractRef
from qs_ai.domain.evaluation.suite_contracts import SuiteContracts
from qs_ai.domain.governance.profile import ProfileAsset
from qs_ai.domain.governance.prompt import PromptAsset
from qs_ai.domain.governance.schema import SchemaAsset
from qs_ai.infrastructure.qs_server.evaluation_input import validate_suite_inputs
from qs_ai.infrastructure.qs_server.evaluation_policies import (
    evaluation_directory,
    load_execution_policy,
    load_gate_policy,
)
from qs_ai.infrastructure.qs_server.evaluation_suite import MBTI_ROOT, FrozenSuite, load_suite
from qs_ai.infrastructure.qs_server.output import schema_directory
from qs_ai.infrastructure.qs_server.profiles import decode_published_profile
from qs_ai.infrastructure.qs_server.semantic_assets import load_semantic_assets, semantic_assets


@dataclass(frozen=True)
class MBTIRootAssets:
    prompt: PromptAsset
    profile: ProfileAsset
    input_schema: SchemaAsset
    semantic: SemanticPromptAsset
    suite: FrozenSuite
    contracts: SuiteContracts
    release: EvidenceReleaseIdentity
    manifest_sha256: str


def digest(raw: str) -> str:
    return "sha256:" + hashlib.sha256(raw.encode()).hexdigest()


def load_mbti_root(directory: Path | None = None) -> MBTIRootAssets:
    directory = directory or evaluation_directory() / "mbti"
    manifest_bytes = (directory / "manifest.json").read_bytes()
    if hashlib.sha256(manifest_bytes).hexdigest() != (
        "5b131f68b9a673597cec9292f35c23b10c5f5a38fa6b91ec312bcc6d089b9cfa"
    ):
        raise ValueError("Unregistered MBTI initialization manifest")
    manifest = json.loads(manifest_bytes)
    expected = {
        "prompt-v1.json",
        "prompt-v1.md",
        "profile-v1.json",
        "semantic-v1.md",
        "suite-v1.json",
    }
    if (
        manifest.get("format") != "qs-ai-mbti-root/v1"
        or manifest.get("source_kind") != "qs-ai-authored-not-qs-export"
        or set(manifest.get("files", {})) != expected
    ):
        raise ValueError("Unsupported MBTI initialization manifest")
    values = {}
    for name in sorted(expected):
        raw = (directory / name).read_bytes()
        if hashlib.sha256(raw).hexdigest() != manifest["files"][name]:
            raise ValueError("MBTI initialization asset checksum mismatch")
        values[name] = raw.decode()
    package = json.loads(values["prompt-v1.json"])
    source = values["prompt-v1.md"].encode()
    blocks = re.findall(r"```text\n(.*?)\n```", values["prompt-v1.md"], re.DOTALL)
    if (
        len(blocks) != 3
        or blocks != [package[k] for k in ("SystemMessage", "TaskTemplate", "DataPreamble")]
        or package["Ref"]["GitBlobSHA"]
        != hashlib.sha1(b"blob " + str(len(source)).encode() + b"\0" + source).hexdigest()
        or package["Ref"]["Fingerprint"] != digest(values["prompt-v1.md"])
    ):
        raise ValueError("MBTI Prompt source proof differs")
    prompt = PromptAsset(
        "participant-mbti-single",
        "v1",
        digest(values["prompt-v1.md"]),
        hashlib.sha256(values["prompt-v1.json"].encode()).hexdigest(),
        values["prompt-v1.json"],
    )
    profile = ProfileAsset(
        "participant-mbti-single",
        "v1",
        digest(values["profile-v1.json"]),
        values["profile-v1.json"],
    )
    decoded = decode_published_profile(
        {
            "definition": json.loads(profile.definition_json),
            "fingerprint": profile.fingerprint,
            "status": "published",  # decoder envelope only; never creates a publication
        }
    )
    raw_input = (schema_directory() / "ai-explanation-input-v2.schema.json").read_text()
    input_schema = SchemaAsset("ai-explanation-input", "v2", digest(raw_input), raw_input)
    suite = load_suite(MBTI_ROOT, definition_json=values["suite-v1.json"])
    if suite.input_schema is None:
        raise ValueError("MBTI root input contract missing")
    validate_suite_inputs(suite, suite.input_schema)
    document = json.loads(suite.definition_json)
    fixture = document["profile_fixture"]
    if {k: v for k, v in fixture.items() if k not in {"status", "fingerprint"}} != json.loads(
        profile.definition_json
    ) or fixture["fingerprint"] != profile.fingerprint:
        raise ValueError("MBTI root Profile differs from suite")
    for case in document["cases"]:
        if case["stage"] == "generation":
            render_prompt(
                executable_prompt(prompt),
                decoded.render_policy,
                json.dumps(case["provider_payload"]),
            )
    semantic = SemanticPromptAsset(
        FrozenContractRef("mbti-single-semantic-evaluator", "v1", digest(values["semantic-v1.md"])),
        values["semantic-v1.md"],
    )
    shared = load_semantic_assets()
    semantic_assets(
        semantic.markdown, shared.output_schema_json, semantic.reference, shared.output_schema
    )
    execution, gate = load_execution_policy(), load_gate_policy()
    contracts = SuiteContracts(
        FrozenContractRef(execution.policy_id, execution.version, execution.fingerprint),
        gate.reference,
        semantic.reference,
        shared.output_schema,
    )
    output_raw = (schema_directory() / "ai-explanation-output-v1.schema.json").read_text()
    release = EvidenceReleaseIdentity(
        suite.reference,
        FrozenContractRef(prompt.template_id, prompt.version, prompt.fingerprint),
        FrozenContractRef(profile.profile_id, profile.version, profile.fingerprint),
        suite.input_schema,
        FrozenContractRef("ai-explanation-output", "ai-explanation-output/v1", digest(output_raw)),
        FrozenContractRef(**document["template"]["generation_route"]),
        semantic.reference,
        shared.output_schema,
        FrozenContractRef(**document["template"]["semantic_route"]),
        contracts.execution_policy,
        contracts.gate_policy,
    )
    return MBTIRootAssets(
        prompt,
        profile,
        input_schema,
        semantic,
        suite,
        contracts,
        release,
        hashlib.sha256(manifest_bytes).hexdigest(),
    )
