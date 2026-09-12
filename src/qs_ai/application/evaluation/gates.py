"""A version-bound preview is evidence for a decision, never an approval itself."""

from dataclasses import dataclass

from qs_ai.domain.evaluation.quality_gates import QualityGateResult


@dataclass(frozen=True)
class GatePreview:
    run_id: str
    version: int
    release_fingerprint: str
    quality: QualityGateResult

    @property
    def gate_passes(self) -> tuple[tuple[str, bool], ...]:
        return (("G1", True), ("G2", True), *self.quality.gate_passes)
