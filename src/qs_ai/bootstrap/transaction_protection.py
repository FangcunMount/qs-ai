"""Host admission policy over current metadata; no message confirmation or retry."""

import math
from dataclasses import dataclass, field, replace

WARNING_AGE = 120.0
PROTECTION_AGE = 300.0
SATURATION_AGE = 120.0


class ProtocolProtectionError(RuntimeError):
    pass


@dataclass(frozen=True)
class ProtocolSample:
    native_blocked: bool = False
    reserved: int = 0
    preparing: int = 0
    gaps: int = 0
    unaged: int = 0
    untracked_aborted: int = 0
    unobserved_committed: int = 0
    conflicts: int = 0
    expiry_age: float = 0.0
    confirmation_age: float = 0.0

    def valid(self) -> bool:
        return all(
            v >= 0
            for v in (
                self.reserved,
                self.preparing,
                self.gaps,
                self.unaged,
                self.untracked_aborted,
                self.unobserved_committed,
                self.conflicts,
            )
        ) and all(math.isfinite(v) and v >= 0 for v in (self.expiry_age, self.confirmation_age))


@dataclass
class ProtocolProtection:
    sample: ProtocolSample = field(default_factory=ProtocolSample)
    available: bool = False
    last_success: float | None = None
    last_poll: float | None = None
    error_since: float | None = None
    saturated_since: float | None = None
    errors: int = 0
    reason: str = ""
    generation: int = 0

    def latch(self, reason: str) -> None:
        if not self.reason:
            self.reason = reason
            self.generation += 1

    def observe(
        self,
        now: float,
        sample: ProtocolSample,
        *,
        failed: bool,
        binding_conflict: bool,
        full: bool,
        freshness: float,
    ) -> None:
        if sample.native_blocked:
            self.sample = replace(self.sample, native_blocked=True)
            self.latch("native_transaction_limit")
        if self.last_poll is not None and now - self.last_poll > freshness:
            self.saturated_since = None
        self.last_poll = now
        if full:
            if self.saturated_since is None:
                self.saturated_since = now
            if now - self.saturated_since >= SATURATION_AGE:
                self.latch("checker_saturated")
        else:
            self.saturated_since = None
        if failed:
            self.available = False
            self.errors += 1
            if self.error_since is None:
                self.error_since = now
            if binding_conflict:
                self.latch("binding_conflict")
            elif now - self.error_since >= WARNING_AGE:
                self.latch("observation_unavailable")
            return
        if not sample.valid():
            self.available = False
            self.errors += 1
            self.latch("invalid_observation")
            return
        self.available = True
        self.last_success = now
        self.error_since = None
        self.sample = sample
        if sample.conflicts:
            self.latch("metadata_conflict")
        elif sample.unaged:
            self.latch("confirmation_age_unavailable")
        elif sample.unobserved_committed:
            self.latch("committed_half_unobserved")
        elif sample.gaps and sample.confirmation_age >= PROTECTION_AGE:
            self.latch("confirmation_gap")
        elif sample.preparing and sample.expiry_age >= PROTECTION_AGE:
            self.latch("preparation_expired")

    def fresh(self, now: float, freshness: float) -> bool:
        return (
            self.available
            and self.last_success is not None
            and 0 <= now - self.last_success <= freshness
        )

    def allowed(self, now: float, freshness: float) -> bool:
        return not self.reason and self.fresh(now, freshness)

    def review(
        self, now: float, freshness: float, generation: int, review: str, *, ready: bool, full: bool
    ) -> None:
        s = self.sample
        if (
            not review.strip()
            or len(review) > 128
            or generation != self.generation
            or not self.reason
            or not self.fresh(now, freshness)
            or not ready
            or full
            or s.native_blocked
            or s.conflicts
            or s.unaged
            or s.unobserved_committed
            or (s.gaps and s.confirmation_age >= WARNING_AGE)
            or (s.preparing and s.expiry_age >= WARNING_AGE)
        ):
            raise ProtocolProtectionError("Fresh evidence and matching named review required")
        self.reason = ""
