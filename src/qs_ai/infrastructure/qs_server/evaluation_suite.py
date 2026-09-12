"""Read exact frozen suite identities; derived suites never replace old Run assets."""

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

from qs_ai.domain.evaluation.identity import FrozenContractRef
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

    def slots(self) -> tuple[tuple[str, int], ...]:
        return tuple(
            (case, ordinal)
            for case in self.generation_case_ids
            for ordinal in range(1, self.repetitions + 1)
        )


def load_suite(reference: FrozenContractRef, *, directory: Path | None = None) -> FrozenSuite:
    if reference not in SUITE_FILES:
        raise ValueError("Unsupported frozen suite identity")
    directory = directory if directory is not None else evaluation_directory()
    raw = (directory / SUITE_FILES[reference]).read_bytes()
    if "sha256:" + hashlib.sha256(raw).hexdigest() != reference.fingerprint:
        raise ValueError("Frozen suite fingerprint mismatch")
    definition = json.loads(raw)
    if (definition["suite_id"], definition["suite_version"]) != (reference.id, reference.version):
        raise ValueError("Frozen suite identity mismatch")
    generation = tuple(c["case_id"] for c in definition["cases"] if c["stage"] == "generation")
    preflight = tuple(c["case_id"] for c in definition["cases"] if c["stage"] == "preflight")
    repetitions = definition["execution_policy"]["generation_repetitions_per_case"]
    if len(generation) != 7 or len(preflight) != 1 or repetitions != 5:
        raise ValueError("Frozen suite execution plan mismatch")
    construction = None
    schema = None
    if reference == V6_PUBLISHED:
        if FrozenContractRef(**definition["derived_from"]) != V6:
            raise ValueError("Derived suite source mismatch")
        contract = definition["input_contract"]
        construction = contract["construction_version"]
        if construction != PUBLISHED_INPUT_VERSION:
            raise ValueError("Unsupported input construction version")
        schema = FrozenContractRef(**contract["schema"])
    return FrozenSuite(
        reference, raw.decode(), generation, preflight[0], repetitions, construction, schema
    )
