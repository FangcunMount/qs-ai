"""Parse the frozen judge contract and resolve exactly its requested obligations."""

import hashlib
import json
from dataclasses import dataclass

from jsonschema import Draft202012Validator

from qs_ai.application.evaluation.release import resolve_semantic_route
from qs_ai.application.interpretation.route_assets import RouteAssets
from qs_ai.domain.evaluation.completion import ProviderReceipt
from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity
from qs_ai.domain.evaluation.preflight import AssertionReceipt
from qs_ai.infrastructure.qs_server.semantic_assets import SemanticAssets


class SemanticDecisionInvalid(ValueError):
    """Provider decision evidence is invalid; not an asset, storage or caller error."""

    def __init__(self, message: str, *, expected: tuple = (), received: tuple = ()) -> None:
        super().__init__(message)
        self.expected = expected
        self.received = received


@dataclass(frozen=True)
class SemanticResult:
    evaluator_version: str
    # Ordered as in the frozen schema; no mutable model-owned mapping escapes parsing.
    scores: tuple[tuple[str, int], ...]
    rationale: str
    decisions: tuple[AssertionReceipt, ...]
    output_fingerprint: str


def _reject_constant(value: str) -> None:
    raise ValueError("Invalid semantic JSON")


async def parse_semantic_output(
    raw: bytes,
    release: EvidenceReleaseIdentity,
    routes: RouteAssets,
    receipt: ProviderReceipt,
    invocation_id: str,
    obligations: tuple[AssertionReceipt, ...],
    *,
    assets: SemanticAssets,
) -> SemanticResult:
    """Input is normalized output bytes; caller retains raw response and candidate binding."""
    if (
        release.semantic_prompt != assets.prompt
        or release.semantic_output_schema != assets.output_schema
    ):
        raise ValueError("Semantic release assets mismatch")
    route = await resolve_semantic_route(release, routes)
    definition = json.loads(route.definition_json)
    if (receipt.invocation_id, receipt.provider, receipt.model) != (
        invocation_id,
        definition["provider"],
        definition["model"],
    ):
        raise ValueError("Semantic receipt does not match frozen invocation and route")
    if not isinstance(raw, bytes) or not 0 < len(raw) <= 256 * 1024:
        raise ValueError("Semantic output missing or exceeds bound")
    try:
        output = json.loads(raw.decode("utf-8"), parse_constant=_reject_constant)
    except (UnicodeError, ValueError):
        raise ValueError("Invalid semantic JSON") from None
    if not Draft202012Validator(json.loads(assets.output_schema_json)).is_valid(output):
        raise ValueError("Semantic output violates frozen schema")
    if not isinstance(obligations, tuple) or not obligations:
        raise ValueError("Semantic obligations required")
    wanted = {}
    for obligation in obligations:
        if not isinstance(obligation, AssertionReceipt) or obligation.status != "pending_semantic":
            raise ValueError("Pending semantic obligation required")
        key = (obligation.type, obligation.scope, obligation.ordinal)
        if key in wanted:
            raise ValueError("Duplicate semantic obligation")
        wanted[key] = obligation
    if len(output["decisions"]) != len(wanted):
        raise SemanticDecisionInvalid(
            "Semantic decisions do not cover obligations",
            expected=tuple(sorted(wanted)),
            received=tuple(),
        )
    seen = set()
    decisions = []
    for decision in output["decisions"]:
        key = (decision["type"], decision["scope"], decision["ordinal"])
        # Some providers reproduce the assertion type and ordinal correctly but
        # flatten the scope to a generic label.  When that identity is
        # unambiguous, bind it back to the frozen obligation; never guess when
        # multiple obligations share the same type and ordinal.
        if key not in wanted:
            candidates = [
                candidate
                for candidate in wanted
                if (candidate[0], candidate[2]) == (key[0], key[2])
            ]
            if len(candidates) == 1:
                key = candidates[0]
        if key not in wanted or key in seen:
            raise SemanticDecisionInvalid(
                "Unknown or duplicate semantic decision",
                expected=tuple(sorted(wanted)),
                received=tuple(sorted(seen | {key})),
            )
        seen.add(key)
        detail = decision["detail"].strip()
        if not detail or len(detail.encode()) > 2000:
            raise SemanticDecisionInvalid("Invalid semantic decision rationale")
        decisions.append(
            AssertionReceipt(
                *key,
                wanted[key].hard,
                assets.prompt.version,
                decision["status"],
                detail,
            )
        )
    rationale = output["rationale"].strip()
    if not rationale or len(rationale.encode()) > 4000:
        raise SemanticDecisionInvalid("Invalid semantic rationale")
    return SemanticResult(
        assets.prompt.version,
        tuple(output["scores"].items()),
        rationale,
        tuple(decisions),
        "sha256:" + hashlib.sha256(raw).hexdigest(),
    )
