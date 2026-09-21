import json
from typing import Protocol

from qs_ai.application.interpretation.model_route_v2 import ModelRouteV2
from qs_ai.application.interpretation.provider import ModelRoute
from qs_ai.domain.governance.route import RouteAsset


class RouteReader(Protocol):
    async def get(self, route: str, revision: str, /) -> RouteAsset | None: ...


class RouteAssets(RouteReader, Protocol):
    async def put(self, asset: RouteAsset, source_ref: str, imported_by: str) -> bool:
        """Insert immutable content; return False for an identical replay."""
        ...


def executable_route(asset: RouteAsset) -> ModelRoute:
    """Decode the supported structured route without accepting endpoints or credentials."""
    definition = json.loads(asset.definition_json)
    structured = definition.pop("structured_output")
    if "format_version" in definition:
        route_v2 = ModelRouteV2(**definition)
        if structured is not True or route_v2.fingerprint() != asset.fingerprint:
            raise ValueError("Invalid immutable model route")
        return route_v2
    definition.setdefault("protocol", "responses")
    definition.setdefault("structured_output_mode", "json_schema")
    definition.setdefault("reasoning_effort", "")
    route = ModelRoute(**definition)
    if (
        not structured
        or route.provider != "deepseek"
        or route.protocol != "responses"
        or route.structured_output_mode != "json_schema"
        or route.fingerprint() != asset.fingerprint
    ):
        raise ValueError("Unsupported immutable model route")
    return route
