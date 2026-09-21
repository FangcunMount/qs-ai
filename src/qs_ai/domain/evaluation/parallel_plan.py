"""Pure candidate scheduling; callers must claim returned work transactionally.

This planner never dispatches a model, acquires a lease, or changes Run status.
Active slots are supplied from durable claims, not inferred from local Tasks.
"""

from dataclasses import dataclass

from qs_ai.domain.evaluation.actions import NextAction, SlotProgress, next_action, slot_action
from qs_ai.domain.evaluation.policy import ExecutionPolicy


@dataclass(frozen=True)
class ParallelPlan:
    ready: tuple[NextAction, ...]
    blocked: tuple[NextAction, ...]
    control: NextAction | None
    complete: bool


def plan_parallel(
    status: str,
    preflight_status: str,
    preflight_case_id: str,
    slots: tuple[SlotProgress, ...],
    policy: ExecutionPolicy,
    *,
    active_slots: frozenset[tuple[str, int]],
    limit: int,
    unresolved_unknown: int = 0,
) -> ParallelPlan:
    if type(limit) is not int or not 1 <= limit <= 32:
        raise ValueError("Invalid parallel limit")
    # Reuse existing complete-plan validation, preflight and unknown-result guards.
    first = next_action(
        status,
        preflight_status,
        preflight_case_id,
        slots,
        policy,
        unresolved_unknown=unresolved_unknown,
    )
    identities = {(slot.case_id, slot.ordinal) for slot in slots}
    if not isinstance(active_slots, frozenset) or not active_slots <= identities:
        raise ValueError("Active claims must belong to frozen slots")
    if (
        status != "collecting"
        or preflight_status != "passed"
        or first.cause == "result_unknown_requires_review"
    ):
        return ParallelPlan((), (), first, False)

    available = max(0, limit - len(active_slots))
    ready: list[NextAction] = []
    blocked: list[NextAction] = []
    pending = False
    for slot in slots:
        if (slot.case_id, slot.ordinal) in active_slots:
            continue
        action = slot_action(slot, policy)
        if action is None:
            continue
        pending = True
        if action.kind == "block":
            blocked.append(action)
        elif len(ready) < available:
            ready.append(action)
    return ParallelPlan(tuple(ready), tuple(blocked), None, not pending and not active_slots)
