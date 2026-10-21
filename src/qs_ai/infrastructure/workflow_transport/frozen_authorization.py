"""Target-bound authorization observed before transaction message preparation.

The original business service keeps its ownership checks. Its root adapter has
only a frozen decision, never the external authorization client. Unknown remote
outcomes cannot become a durable allow or deny decision.
"""

from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime

from qs_ai.application.interpretation.ports import AccessDenied, EvidenceSource
from qs_ai.application.interpretation.service import fingerprint
from qs_ai.domain.interpretation.model import Actor
from qs_ai.infrastructure.persistence.mysql.execution_configurations import (
    ConfigurationPlanConflict,
)


def _target(actor: Actor, testee_id: str, assessment_ids: tuple[str, ...]) -> bytes:
    return bytes.fromhex(fingerprint([asdict(actor), testee_id, assessment_ids]))


def _clock() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True)
class EvidenceAuthorization:
    target_hash: bytes = field(repr=False)
    allowed: bool
    observed_at: datetime

    def __post_init__(self) -> None:
        if type(self.target_hash) is not bytes or len(self.target_hash) != 32:
            raise ValueError("Exact authorization target digest required")
        if type(self.allowed) is not bool:
            raise ValueError("Explicit authorization result required")
        if not isinstance(self.observed_at, datetime) or self.observed_at.utcoffset() is None:
            raise ValueError("Aware authorization observation clock required")
        object.__setattr__(self, "observed_at", self.observed_at.astimezone(UTC))


class RecordingEvidenceSource:
    def __init__(
        self, source: EvidenceSource, *, clock: Callable[[], datetime] | None = None
    ) -> None:
        self._source = source
        self._clock = _clock if clock is None else clock
        self._proof: EvidenceAuthorization | None = None
        self._attempted = False

    @property
    def proof(self) -> EvidenceAuthorization | None:
        return self._proof

    async def authorize(
        self, actor: Actor, testee_id: str, assessment_ids: tuple[str, ...]
    ) -> None:
        if self._attempted:
            raise ConfigurationPlanConflict("One external authorization per command plan required")
        self._attempted = True
        target = _target(actor, testee_id, assessment_ids)
        try:
            await self._source.authorize(actor, testee_id, assessment_ids)
        except AccessDenied:
            self._proof = EvidenceAuthorization(target, False, self._clock())
            raise
        else:
            self._proof = EvidenceAuthorization(target, True, self._clock())


class FrozenEvidenceSource:
    def __init__(self, proof: EvidenceAuthorization | None) -> None:
        self._proof = proof

    async def authorize(
        self, actor: Actor, testee_id: str, assessment_ids: tuple[str, ...]
    ) -> None:
        proof = self._proof
        if proof is None or proof.target_hash != _target(actor, testee_id, assessment_ids):
            raise ConfigurationPlanConflict("Exact original authorization target required")
        if not proof.allowed:
            raise AccessDenied
