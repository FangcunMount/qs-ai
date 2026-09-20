"""Post-commit diagnostics; never a business retry trigger."""

from qs_ai.infrastructure.observability.structured import emit as structured_emit


def emit(event: str, *, session_id: str, run_id: str, invocation_id: str = "") -> None:
    structured_emit(
        event,
        "generation",
        task_kind="participant_interpretation",
        session_id=session_id,
        run_id=run_id,
        invocation_id=invocation_id,
    )
