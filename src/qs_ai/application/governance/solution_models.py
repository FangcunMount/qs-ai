"""Supported editor knobs map directly to frozen executable routes, never credentials."""

from dataclasses import dataclass, field, replace

from qs_ai.application.governance.solutions import ModelSelection
from qs_ai.application.interpretation.model_route_v2 import ModelRouteV2
from qs_ai.application.interpretation.provider import ModelRoute
from qs_ai.model_configuration import ModelConfiguration


def selection(route: ModelRoute) -> ModelSelection:
    return ModelSelection(
        model=route.model,
        max_output_tokens=route.max_output_tokens,
        timeout_milliseconds=route.timeout_milliseconds,
        reasoning_effort=route.reasoning_effort,
        **(
            {
                key: getattr(route, key)
                for key in ("model_key", "catalog_revision", "thinking", "temperature", "top_p")
            }
            if isinstance(route, ModelRouteV2)
            else {}
        ),
    )


def edited_route(
    source: ModelRoute,
    values: ModelSelection,
    version: str,
    allowed_models: tuple[str, ...],
    configuration: ModelConfiguration | None = None,
    purpose: str = "generation",
) -> ModelRoute:
    if values.model_key is not None:
        if configuration is None or not configuration.v2_writes_enabled:
            raise ValueError("model_v2_writes_disabled")
        entry, binding = configuration.resolve(values.model_key, values.catalog_revision, purpose)
        if values.model != entry.model_id:
            raise ValueError("model_identity_mismatch")
        entry.validate_parameters(
            **{
                name: getattr(values, name)
                for name in (
                    "max_output_tokens",
                    "timeout_milliseconds",
                    "reasoning_effort",
                    "thinking",
                    "temperature",
                    "top_p",
                )
            }
        )
        return ModelRouteV2(
            route=source.route,
            revision=version,
            provider=binding.provider,
            model=entry.model_id,
            protocol=binding.protocol,
            structured_output_mode="json_schema"
            if binding.provider == "deepseek"
            else "json_object",
            timeout_milliseconds=values.timeout_milliseconds,
            max_output_tokens=values.max_output_tokens,
            reasoning_effort=values.reasoning_effort,
            model_key=entry.model_key,
            catalog_revision=entry.catalog_revision,
            binding_id=binding.binding_id,
            binding_revision=binding.revision,
            adapter_contract=binding.adapter_contract,
            thinking=values.thinking,
            temperature=values.temperature,
            top_p=values.top_p,
        )
    if isinstance(source, ModelRouteV2) or any(
        getattr(values, name) is not None
        for name in ("catalog_revision", "thinking", "temperature", "top_p")
    ):
        raise ValueError("Explicit model identity required")
    if values.model not in allowed_models:
        raise ValueError("Model is not enabled for this deployment")
    if source.provider != "deepseek" or source.protocol != "responses":
        raise ValueError("Unsupported model adapter")
    return replace(
        source,
        revision=version,
        model=values.model,
        max_output_tokens=values.max_output_tokens,
        timeout_milliseconds=values.timeout_milliseconds,
        reasoning_effort=values.reasoning_effort,
    )


@dataclass(frozen=True)
class EditableModelPolicy:
    allowed: tuple[str, ...] = ("deepseek-v4-pro",)
    configuration: ModelConfiguration = field(default_factory=ModelConfiguration)


DEFAULT_EDITABLE_MODELS = EditableModelPolicy()


def validate_v2_admission(
    route: ModelRoute, configuration: ModelConfiguration, purpose: str
) -> None:
    if not isinstance(route, ModelRouteV2):
        return
    entry, binding = configuration.resolve(route.model_key, route.catalog_revision, purpose)
    if (route.model, route.binding_id, route.binding_revision, route.adapter_contract) != (
        entry.model_id,
        binding.binding_id,
        binding.revision,
        binding.adapter_contract,
    ):
        raise ValueError("Frozen model identity mismatch")
    entry.validate_parameters(
        **{
            name: getattr(route, name)
            for name in (
                "max_output_tokens",
                "timeout_milliseconds",
                "reasoning_effort",
                "thinking",
                "temperature",
                "top_p",
            )
        }
    )
