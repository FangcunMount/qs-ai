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
from qs_ai.infrastructure.qs_server.evaluation_suite import (
    MBTI_EXPLORATION_ROOT,
    MBTI_ROOT,
    MBTI_THEMES_ROOT,
    FrozenSuite,
    load_suite,
)
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
    output_schema: SchemaAsset | None = None


def digest(raw: str) -> str:
    return "sha256:" + hashlib.sha256(raw.encode()).hexdigest()


def load_mbti_root(directory: Path | None = None) -> MBTIRootAssets:
    return _load_root(
        directory or evaluation_directory() / "mbti",
        MBTI_ROOT,
        "8be5bead7f390b5974a7131874f0de6e44b685f2d12353589abb684c5a2c2aa9",
        "v2",
        "v1",
    )


def load_mbti_themes_root(directory: Path | None = None) -> MBTIRootAssets:
    return _load_root(
        directory or evaluation_directory() / "mbti-themes",
        MBTI_THEMES_ROOT,
        "4c3416120c88d9020b736da2c335f6bcb20fa3eaa55d2bea405c8824faf061c3",
        "v3",
        "v2",
    )


def load_mbti_exploration_root(directory: Path | None = None) -> MBTIRootAssets:
    return _load_root(
        directory or evaluation_directory() / "mbti-exploration",
        MBTI_EXPLORATION_ROOT,
        "5ad26a2dd9b51e9d328633a7d2910a6508ff67baaaf3470e06d3adf341b61483",
        "v4",
        "v2",
    )


def _load_root(
    directory: Path,
    reference: FrozenContractRef,
    manifest_sha: str,
    input_version: str,
    output_version: str,
) -> MBTIRootAssets:
    manifest_bytes = (directory / "manifest.json").read_bytes()
    if hashlib.sha256(manifest_bytes).hexdigest() != manifest_sha:
        raise ValueError("Unregistered MBTI initialization manifest")
    manifest = json.loads(manifest_bytes)
    expected = {
        "prompt-v1.json",
        "prompt-v1.md",
        "profile-v1.json",
        "semantic-v1.md",
        "suite-v1.json",
    }
    if reference in (MBTI_THEMES_ROOT, MBTI_EXPLORATION_ROOT):
        expected.add("reference-material-v1.json")
    if reference == MBTI_EXPLORATION_ROOT:
        expected.add("source-proof-v1.json")
    source_kind = (
        "qs-ai-derived-from-published-r17"
        if reference == MBTI_EXPLORATION_ROOT
        else "qs-ai-authored-not-qs-export"
    )
    if (
        manifest.get("format") != "qs-ai-mbti-root/v1"
        or manifest.get("source_kind") != source_kind
        or set(manifest.get("files", {})) != expected
    ):
        raise ValueError("Unsupported MBTI initialization manifest")
    values: dict[str, str] = {}
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
        reference.id,
        reference.version,
        digest(values["prompt-v1.md"]),
        hashlib.sha256(values["prompt-v1.json"].encode()).hexdigest(),
        values["prompt-v1.json"],
    )
    profile = ProfileAsset(
        reference.id,
        reference.version,
        digest(values["profile-v1.json"]),
        values["profile-v1.json"],
    )
    if reference in (MBTI_THEMES_ROOT, MBTI_EXPLORATION_ROOT) and json.loads(
        profile.definition_json
    )["reference_material"] != json.loads(values["reference-material-v1.json"]):
        raise ValueError("MBTI reference material differs from frozen Profile")
    decoded = decode_published_profile(
        {
            "definition": json.loads(profile.definition_json),
            "fingerprint": profile.fingerprint,
            "status": "published",  # decoder envelope only; never creates a publication
        }
    )
    raw_input = (
        schema_directory() / f"ai-explanation-input-{input_version}.schema.json"
    ).read_text()
    input_schema = SchemaAsset("ai-explanation-input", input_version, digest(raw_input), raw_input)
    suite = load_suite(reference, definition_json=values["suite-v1.json"])
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
                scene_contract_version=json.loads(profile.definition_json)[
                    "scene_contract_version"
                ],
            )
    semantic_version = (
        "three-topic-v2-exact-obligations"
        if reference == MBTI_EXPLORATION_ROOT
        else reference.version
    )
    if reference == MBTI_EXPLORATION_ROOT:
        proof = json.loads(values["source-proof-v1.json"])
        messages = {key: package[key] for key in ("SystemMessage", "TaskTemplate", "DataPreamble")}
        from qs_ai.infrastructure.qs_server.evaluation_suite import canonical

        if proof["message_sha256"] != digest(canonical(messages)):
            raise ValueError("Exploration messages differ from published r17")
        if proof["source_release"]["semantic_prompt"]["fingerprint"] != digest(
            values["semantic-v1.md"]
        ):
            raise ValueError("Exploration semantic rules differ from published r17")
    semantic = SemanticPromptAsset(
        FrozenContractRef(
            "mbti-single-semantic-evaluator", semantic_version, digest(values["semantic-v1.md"])
        ),
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
    output_raw = (
        schema_directory() / f"ai-explanation-output-{output_version}.schema.json"
    ).read_text()
    release = EvidenceReleaseIdentity(
        suite.reference,
        FrozenContractRef(prompt.template_id, prompt.version, prompt.fingerprint),
        FrozenContractRef(profile.profile_id, profile.version, profile.fingerprint),
        suite.input_schema,
        FrozenContractRef(
            "ai-explanation-output", "ai-explanation-output/" + output_version, digest(output_raw)
        ),
        FrozenContractRef(**document["template"]["generation_route"]),
        semantic.reference,
        shared.output_schema,
        FrozenContractRef(**document["template"]["semantic_route"]),
        contracts.execution_policy,
        contracts.gate_policy,
    )
    from dataclasses import asdict

    if document["template"]["release"] != {
        k: v for k, v in asdict(release).items() if k != "suite"
    }:
        raise ValueError("MBTI template differs from exact initialization assets")
    return MBTIRootAssets(
        prompt,
        profile,
        input_schema,
        semantic,
        suite,
        contracts,
        release,
        hashlib.sha256(manifest_bytes).hexdigest(),
        SchemaAsset("ai-explanation-output", output_version, digest(output_raw), output_raw)
        if reference in (MBTI_THEMES_ROOT, MBTI_EXPLORATION_ROOT)
        else None,
    )
