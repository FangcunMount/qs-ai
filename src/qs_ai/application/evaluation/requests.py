"""A caller-chosen Run UUID identifies one immutable, authorized creation request."""

from datetime import datetime
from typing import Protocol

from qs_ai.application.evaluation.management import EvaluationView, ManagementScope
from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity


class EvaluationRequests(Protocol):
    async def create(
        self,
        scope: ManagementScope,
        release: EvidenceReleaseIdentity,
        reason: str,
        at: datetime,
        *,
        confirm: bool,
    ) -> EvaluationView: ...
