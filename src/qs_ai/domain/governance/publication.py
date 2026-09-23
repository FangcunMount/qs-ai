"""Release selection and pointer changes after scoped evidence has been revalidated.

These values do not establish database membership or approval by themselves. The
write adapter must lock the pointer and Run, recompute the final gate and resolve
all immutable assets before constructing a publication. Rollback targets must be
loaded from retained publication records, never supplied by a client.
"""

import hashlib
import json
import re
from dataclasses import asdict, dataclass, replace
from datetime import datetime
from typing import Literal
from uuid import UUID

from qs_ai.domain.evaluation.finalization import FinalReview
from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity, FrozenContractRef
from qs_ai.domain.governance.manifest import GenerationManifest
from qs_ai.domain.governance.profile import ProfileAsset
from qs_ai.domain.governance.scenes import is_mbti_selector


class PublicationConflict(ValueError):
    pass


def valid_time(at: datetime) -> bool:
    return isinstance(at, datetime) and at.tzinfo is not None and at.utcoffset() is not None


def valid_version(value: int, *, initial: bool = False) -> bool:
    return type(value) is int and (0 if initial else 1) <= value < 2**63


@dataclass(frozen=True)
class ReleaseSelector:
    audience: str
    model_kind: str
    decision_kind: str
    model_code: str | None = None
    model_version: str | None = None

    def __post_init__(self) -> None:
        if (self.audience, self.model_kind, self.decision_kind) != (
            "participant",
            "scale",
            "score_range",
        ) and not is_mbti_selector(
            self.audience, self.model_kind, self.decision_kind, self.model_code, self.model_version
        ):
            raise ValueError("Unsupported existing Profile selector")
        if self.model_code is not None and (
            not isinstance(self.model_code, str)
            or not self.model_code.strip()
            or len(self.model_code.encode()) > 255
        ):
            raise ValueError("Invalid selector model code")
        if self.model_version is not None and (
            self.model_code is None
            or not isinstance(self.model_version, str)
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,127}", self.model_version)
        ):
            raise ValueError("Selector version requires a model code")

    def admission_candidates(self) -> tuple["ReleaseSelector", ...]:
        if self.model_kind == "typology":
            return (self,)
        return tuple(
            dict.fromkeys(
                (
                    self,
                    replace(self, model_version=None),
                    replace(self, model_code=None, model_version=None),
                )
            )
        )

    @property
    def specificity(self) -> int:
        return 2 if self.model_version is not None else 1 if self.model_code is not None else 0

    def key(self) -> str:
        raw = json.dumps(asdict(self), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(raw.encode()).hexdigest()

    def matches(self, query: "ReleaseSelector") -> bool:
        return (
            (self.audience, self.model_kind, self.decision_kind)
            == (query.audience, query.model_kind, query.decision_kind)
            and (self.model_code is None or self.model_code == query.model_code)
            and (self.model_version is None or self.model_version == query.model_version)
        )


@dataclass(frozen=True)
class PublicationAudit:
    actor: str
    reason: str
    at: datetime

    def __post_init__(self) -> None:
        if (
            not isinstance(self.actor, str)
            or not re.fullmatch(r"user:[1-9][0-9]{0,18}", self.actor)
            or int(self.actor.removeprefix("user:")) >= 2**63
            or not isinstance(self.reason, str)
            or not self.reason.strip()
            or len(self.reason.encode()) > 1000
            or any(c in self.reason for c in "<>")
            or not valid_time(self.at)
        ):
            raise ValueError("Trusted publication audit required")


@dataclass(frozen=True)
class PublicationEvidence:
    profile: ProfileAsset
    manifest: GenerationManifest
    release: EvidenceReleaseIdentity
    run_id: UUID
    run_version: int
    final_review: FinalReview
    evaluated_manifest_fingerprint: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.run_id, UUID)
            or self.run_id.int == 0
            or not valid_version(self.run_version)
            or not self.final_review.passed
        ):
            raise ValueError("Final approved evaluation required")
        if self.evaluated_manifest_fingerprint != self.manifest.fingerprint():
            raise ValueError("Publication bytes differ from the evaluated manifest")
        for name in ("profile", "prompt", "generation_route", "input_schema", "output_schema"):
            asset = getattr(self.manifest, name)
            version = (
                f"{asset.identity}/{asset.version}" if name.endswith("schema") else asset.version
            )
            if getattr(self.release, name) != FrozenContractRef(
                asset.identity, version, asset.fingerprint
            ):
                raise ValueError("Published generation assets differ from evaluated release")
        if (
            self.manifest.profile.identity != self.profile.profile_id
            or self.manifest.profile.version != self.profile.version
            or self.manifest.profile.fingerprint != self.profile.fingerprint
            or self.manifest.profile.content_sha256
            != hashlib.sha256(self.profile.definition_json.encode()).hexdigest()
        ):
            raise ValueError("Publication Profile differs from immutable content")
        definition = json.loads(self.profile.definition_json)
        policy = definition["generation_policy"]
        expected = {
            "prompt_template_id": self.manifest.prompt.identity,
            "prompt_version": self.manifest.prompt.version,
            "provider_route": self.manifest.generation_route.identity,
            "input_schema_version": (
                f"{self.manifest.input_schema.identity}/{self.manifest.input_schema.version}"
            ),
            "output_schema_version": (
                f"{self.manifest.output_schema.identity}/{self.manifest.output_schema.version}"
            ),
        }
        if any(policy.get(name) != value for name, value in expected.items()):
            raise ValueError("Publication Profile policy differs from evaluated generation assets")
        ReleaseSelector(**definition["selector"])

    @property
    def selector(self) -> ReleaseSelector:
        return ReleaseSelector(**json.loads(self.profile.definition_json)["selector"])


@dataclass(frozen=True)
class PublishedConfiguration:
    publication_id: UUID
    evidence: PublicationEvidence
    audit: PublicationAudit

    def __post_init__(self) -> None:
        if not isinstance(self.publication_id, UUID) or self.publication_id.int == 0:
            raise ValueError("Publication identity required")
        if self.audit.at < self.evidence.final_review.finalized_at:
            raise ValueError("Publication predates final approval")


@dataclass(frozen=True)
class PublicationPointer:
    selector: ReleaseSelector
    version: int = 0
    active: PublishedConfiguration | None = None
    changed_at: datetime | None = None

    def __post_init__(self) -> None:
        if not valid_version(self.version, initial=True):
            raise ValueError("Invalid publication pointer version")
        if self.version == 0:
            if self.active is not None or self.changed_at is not None:
                raise ValueError("Initial publication pointer must be empty")
        elif self.changed_at is None or not valid_time(self.changed_at):
            raise ValueError("Pointer change time required")
        if self.active is not None and (
            self.active.evidence.selector != self.selector
            or self.changed_at is None
            or self.changed_at < self.active.audit.at
        ):
            raise ValueError("Active publication differs from selector or pointer time")


@dataclass(frozen=True)
class PublicationChange:
    previous: PublicationPointer
    current: PublicationPointer
    action: Literal["publish", "rollback", "disable"]
    audit: PublicationAudit


def change_publication(
    pointer: PublicationPointer,
    target: PublishedConfiguration | None,
    action: Literal["publish", "rollback", "disable"],
    audit: PublicationAudit,
    *,
    expected_version: int,
    expected_active_id: UUID | None,
) -> PublicationChange:
    current_id = pointer.active.publication_id if pointer.active is not None else None
    if (
        not valid_version(expected_version, initial=True)
        or pointer.version != expected_version
        or current_id != expected_active_id
        or pointer.version == 2**63 - 1
    ):
        raise PublicationConflict("Current publication changed; refresh before confirming")
    if pointer.changed_at is not None and audit.at < pointer.changed_at:
        raise ValueError("Publication change predates previous change")
    if action == "disable":
        if target is not None or pointer.active is None:
            raise PublicationConflict("Disable requires an active publication")
    elif action in ("publish", "rollback"):
        if target is None or target.evidence.selector != pointer.selector:
            raise PublicationConflict("Publication must occupy the same selector slot")
        if target.publication_id == current_id:
            raise PublicationConflict("Publication already active")
        if action == "publish" and target.audit != audit:
            raise ValueError("New publication and pointer require the same audit")
        if action == "rollback" and (pointer.version == 0 or target.audit.at > audit.at):
            raise ValueError("Rollback requires a previously published target")
    else:
        raise ValueError("Unknown publication operation")
    updated = PublicationPointer(pointer.selector, pointer.version + 1, target, audit.at)
    return PublicationChange(pointer, updated, action, audit)


def resolve_publication(
    pointers: tuple[PublicationPointer, ...], query: ReleaseSelector
) -> PublishedConfiguration | None:
    candidates = [p for p in pointers if p.active is not None and p.selector.matches(query)]
    if not candidates:
        return None
    highest = max(p.selector.specificity for p in candidates)
    selected = [p for p in candidates if p.selector.specificity == highest]
    if len(selected) != 1:
        raise PublicationConflict("Ambiguous published Profile selector")
    return selected[0].active
