"""Supported editor knobs map directly to frozen executable routes, never credentials."""

from dataclasses import asdict, dataclass, replace

from qs_ai.application.governance.solutions import ModelSelection
from qs_ai.application.interpretation.provider import ModelRoute


def selection(route: ModelRoute) -> ModelSelection:
    return ModelSelection(
        model=route.model,
        max_output_tokens=route.max_output_tokens,
        timeout_milliseconds=route.timeout_milliseconds,
        reasoning_effort=route.reasoning_effort,
    )


def edited_route(
    source: ModelRoute,
    values: ModelSelection,
    version: str,
    allowed_models: tuple[str, ...],
) -> ModelRoute:
    if values.model not in allowed_models:
        raise ValueError("Model is not enabled for this deployment")
    if source.provider != "deepseek" or source.protocol != "responses":
        raise ValueError("Unsupported model adapter")
    return replace(source, revision=version, **asdict(values))


@dataclass(frozen=True)
class EditableModelPolicy:
    allowed: tuple[str, ...] = ("deepseek-v4-pro",)


DEFAULT_EDITABLE_MODELS = EditableModelPolicy()
