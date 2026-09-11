"""Read the original frozen v6 suite for Run planning without selecting latest."""

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


@dataclass(frozen=True)
class FrozenSuite:
    reference: FrozenContractRef
    definition_json: str
    generation_case_ids: tuple[str, ...]
    preflight_case_id: str
    repetitions: int

    def slots(self) -> tuple[tuple[str, int], ...]:
        return tuple(
            (case, ordinal)
            for case in self.generation_case_ids
            for ordinal in range(1, self.repetitions + 1)
        )


def load_suite(reference: FrozenContractRef, *, directory: Path | None = None) -> FrozenSuite:
    if reference != V6:
        raise ValueError("Unsupported frozen suite identity")
    directory = directory if directory is not None else evaluation_directory()
    raw = (directory / "ai-explanation-prompt-evaluation-cases-v6.json").read_bytes()
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
    return FrozenSuite(reference, raw.decode(), generation, preflight[0], repetitions)
