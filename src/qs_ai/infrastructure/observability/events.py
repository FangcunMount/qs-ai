"""Best-effort structured diagnostic logging, never a business retry trigger."""

import json
import logging

logger = logging.getLogger("qs_ai.execution")


def emit(event: str, *, session_id: str, run_id: str, invocation_id: str = "") -> None:
    try:
        logger.info(
            json.dumps(
                {
                    "event": event,
                    "task_kind": "participant_interpretation",
                    "session_id": session_id,
                    "run_id": run_id,
                    "invocation_id": invocation_id,
                }
            )
        )
    except Exception:
        # A failing log handler must not turn a committed provider receipt into a retry.
        pass
