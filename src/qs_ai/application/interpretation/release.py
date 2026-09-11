from dataclasses import dataclass

from qs_ai.application.interpretation.input import InputPolicy
from qs_ai.application.interpretation.prompts import RenderPolicy


@dataclass(frozen=True)
class ExplanationRelease:
    """Validated immutable policy; no transport or persistence model escapes here."""

    input_policy: InputPolicy
    render_policy: RenderPolicy
    provider_route: str
    definition_json: str


class InvalidRelease(ValueError):
    pass
