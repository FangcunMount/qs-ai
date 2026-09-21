"""Recheck newly edited routes at admission; never change accepted execution snapshots."""

from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.governance.solution_models import EditableModelPolicy, validate_v2_admission
from qs_ai.application.interpretation.model_route_v2 import ModelRouteV2
from qs_ai.application.interpretation.route_assets import executable_route
from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity
from qs_ai.infrastructure.persistence.mysql.solution_assets import route_for


async def check_editable_models(
    db: AsyncSession, release: EvidenceReleaseIdentity, policy: EditableModelPolicy
) -> None:
    for purpose, ref in (
        ("generation", release.generation_route),
        ("semantic", release.semantic_route),
    ):
        # The existing fixed-route admission contract is unchanged. All routes
        # created by the workspace have this reserved immutable version namespace.
        route = executable_route(await route_for(db, ref))
        if isinstance(route, ModelRouteV2):
            validate_v2_admission(route, policy.configuration, purpose)
        elif ref.version.startswith("solution-"):
            if route.model not in policy.allowed:
                raise ValueError(
                    "Edited model is no longer enabled; create and evaluate a new version"
                )
