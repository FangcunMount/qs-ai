"""Read exact frozen suite identities; derived suites never replace old Run assets."""

import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from pydantic import TypeAdapter

from qs_ai.domain.evaluation.identity import FrozenContractRef
from qs_ai.domain.governance.manifest import GenerationManifest
from qs_ai.domain.governance.profile import ProfileAsset
from qs_ai.infrastructure.qs_server.evaluation_policies import evaluation_directory

V6 = FrozenContractRef(
    "cross-dimension-participant-scale-v6",
    "ai-explanation-prompt-evaluation-cases/v6",
    "sha256:fbb84e4f2ba17f59e734d609ce1132a2704af8778048a79f08454115c84e115b",
)
V6_PUBLISHED = FrozenContractRef(
    "cross-dimension-participant-scale-v6-published",
    "qs-ai-evaluation-cases/v1",
    "sha256:42afcc73db6fa272ab54aa17bcb9b30dc8797a382ee9329d9453aa9c9953e382",
)
PUBLISHED_INPUT_VERSION = "qs-published-snapshot-v1"
SUITE_FILES = {
    V6: "ai-explanation-prompt-evaluation-cases-v6.json",
    V6_PUBLISHED: "qs-ai-published-input-cases-v1.json",
}


@dataclass(frozen=True)
class FrozenSuite:
    reference: FrozenContractRef
    definition_json: str
    generation_case_ids: tuple[str, ...]
    preflight_case_id: str
    repetitions: int
    input_construction_version: str | None = None
    input_schema: FrozenContractRef | None = None
    manifest: GenerationManifest | None = None

    def slots(self) -> tuple[tuple[str, int], ...]:
        return tuple(
            (case, ordinal)
            for case in self.generation_case_ids
            for ordinal in range(1, self.repetitions + 1)
        )


def load_suite(
    reference: FrozenContractRef,
    *,
    directory: Path | None = None,
    definition_json: str | None = None,
) -> FrozenSuite:
    if definition_json is None:
        if reference not in SUITE_FILES:
            raise ValueError("Unsupported frozen suite identity")
        directory = directory if directory is not None else evaluation_directory()
        raw = (directory / SUITE_FILES[reference]).read_bytes()
    else:
        raw = definition_json.encode()
    if not 1 <= len(raw) <= 524288:
        raise ValueError("Frozen suite exceeds limit")
    if "sha256:" + hashlib.sha256(raw).hexdigest() != reference.fingerprint:
        raise ValueError("Frozen suite fingerprint mismatch")
    definition = json.loads(raw)
    if (definition["suite_id"], definition["suite_version"]) != (reference.id, reference.version):
        raise ValueError("Frozen suite identity mismatch")
    manifest = None
    if reference not in SUITE_FILES:
        manifest = validate_native(definition, raw.decode())
    generation = tuple(c["case_id"] for c in definition["cases"] if c["stage"] == "generation")
    preflight = tuple(c["case_id"] for c in definition["cases"] if c["stage"] == "preflight")
    repetitions = definition["execution_policy"]["generation_repetitions_per_case"]
    if len(generation) != 7 or len(preflight) != 1 or repetitions != 5:
        raise ValueError("Frozen suite execution plan mismatch")
    construction = None
    schema = None
    if reference == V6_PUBLISHED or manifest is not None:
        if manifest is None and FrozenContractRef(**definition["derived_from"]) != V6:
            raise ValueError("Derived suite source mismatch")
        contract = definition["input_contract"]
        construction = contract["construction_version"]
        if construction != PUBLISHED_INPUT_VERSION:
            raise ValueError("Unsupported input construction version")
        schema = FrozenContractRef(**contract["schema"])
    return FrozenSuite(
        reference,
        raw.decode(),
        generation,
        preflight[0],
        repetitions,
        construction,
        schema,
        manifest,
    )


def canonical(document: dict[str, Any]) -> str:
    return json.dumps(
        document, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    )


def validate_native(document: dict[str, Any], raw: str) -> GenerationManifest:
    """Rebind the full retained case set; changing quality obligations needs a new contract."""
    from qs_ai.infrastructure.qs_server.profiles import Definition, canonical_definition

    baseline = json.loads(load_suite(V6_PUBLISHED).definition_json)
    if raw != canonical(document) or document.get("registration_schema") != "qs-ai-suite/v1":
        raise ValueError("Canonical native suite required")
    if document.get("derived_from") != asdict(V6_PUBLISHED):
        raise ValueError("Native suite must retain the published-input case contract")
    mutable = {
        "suite_id",
        "suite_version",
        "prompt",
        "profile_fixture",
        "derived_from",
        "registration_schema",
        "manifest",
    }
    if canonical({k: v for k, v in document.items() if k not in mutable}) != canonical(
        {k: v for k, v in baseline.items() if k not in mutable}
    ):
        raise ValueError("Native suite changed inherited cases or quality obligations")
    manifest = TypeAdapter(GenerationManifest).validate_python(document["manifest"])
    fixture = document["profile_fixture"]
    definition = Definition.model_validate(
        {k: v for k, v in fixture.items() if k not in {"status", "fingerprint"}}
    )
    profile_raw = canonical_definition(definition.model_dump())
    checksum = hashlib.sha256(profile_raw.encode()).hexdigest()
    if fixture.get("status") != "registered" or fixture.get("fingerprint") != "sha256:" + checksum:
        raise ValueError("Native suite Profile fixture changed")
    if (definition.profile_id, definition.version, "sha256:" + checksum, checksum) != (
        manifest.profile.identity,
        manifest.profile.version,
        manifest.profile.fingerprint,
        manifest.profile.content_sha256,
    ):
        raise ValueError("Native suite Profile differs from manifest")
    policy = definition.generation_policy
    if document["prompt"] != asdict(manifest.prompt) or (
        policy.prompt_template_id,
        policy.prompt_version,
        policy.provider_route,
        policy.input_schema_version,
        policy.output_schema_version,
    ) != (
        manifest.prompt.identity,
        manifest.prompt.version,
        manifest.generation_route.identity,
        manifest.input_schema.identity + "/" + manifest.input_schema.version,
        manifest.output_schema.identity + "/" + manifest.output_schema.version,
    ):
        raise ValueError("Native suite executable references differ from Profile")
    # Cases exercise this exact input/eligibility projection. A changed projection
    # requires new cases, not relabeling the baseline as newly evaluated evidence.
    for field in ("eligibility", "input_policy"):
        if fixture[field] != baseline["profile_fixture"][field]:
            raise ValueError("New input policy requires a matching case contract")
    if document["input_contract"]["schema"] != {
        "id": manifest.input_schema.identity,
        "version": manifest.input_schema.identity + "/" + manifest.input_schema.version,
        "fingerprint": manifest.input_schema.fingerprint,
    }:
        raise ValueError("Native suite input schema mismatch")
    return manifest


def derive_suite(
    identity: str, version: str, profile: ProfileAsset, manifest: GenerationManifest
) -> FrozenSuite:
    document = json.loads(load_suite(V6_PUBLISHED).definition_json)
    document.update(
        suite_id=identity,
        suite_version=version,
        derived_from=asdict(V6_PUBLISHED),
        registration_schema="qs-ai-suite/v1",
        manifest=asdict(manifest),
        prompt=asdict(manifest.prompt),
        profile_fixture={
            **json.loads(profile.definition_json),
            "status": "registered",
            "fingerprint": profile.fingerprint,
        },
    )
    raw = canonical(document)
    reference = FrozenContractRef(
        identity, version, "sha256:" + hashlib.sha256(raw.encode()).hexdigest()
    )
    if (identity, version) in {(ref.id, ref.version) for ref in SUITE_FILES}:
        raise ValueError("Retained suite identity cannot be replaced")
    return load_suite(reference, definition_json=raw)


def suite_prompt(suite: FrozenSuite) -> tuple[str, str]:
    if suite.manifest is not None:
        return suite.manifest.prompt.identity, suite.manifest.prompt.version
    prompt = json.loads(suite.definition_json)["prompt"]
    return prompt["template_id"], prompt["version"]


def resolve_suite(reference: FrozenContractRef, frozen: FrozenSuite | None = None) -> FrozenSuite:
    if frozen is not None and frozen.reference != reference:
        raise ValueError("Supplied suite differs from release")
    return load_suite(reference, definition_json=frozen.definition_json if frozen else None)
