"""Recheck newly edited routes at admission; never change accepted execution snapshots."""

from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.governance.solution_models import EditableModelPolicy
from qs_ai.application.interpretation.route_assets import executable_route
from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity
from qs_ai.infrastructure.persistence.mysql.solution_assets import route_for


async def check_editable_models(
    db: AsyncSession, release: EvidenceReleaseIdentity, policy: EditableModelPolicy
) -> None:
    for ref in (release.generation_route, release.semantic_route):
        # The existing fixed-route admission contract is unchanged. All routes
        # created by the workspace have this reserved immutable version namespace.
        if ref.version.startswith("solution-"):
            route = executable_route(await route_for(db, ref))
            if route.model not in policy.allowed:
                raise ValueError(
                    "Edited model is no longer enabled; create and evaluate a new version"
                )
