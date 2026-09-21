"""Single-event-loop capacity. Tokens are acquired before durable model dispatch."""

from dataclasses import dataclass


@dataclass(frozen=True)
class ProviderCapacity:
    total: int
    generation_reserved: int

    def __post_init__(self) -> None:
        if (
            type(self.total) is not int
            or not 1 <= self.total <= 32
            or type(self.generation_reserved) is not int
            or not 0 <= self.generation_reserved < self.total
        ):
            raise ValueError("Invalid provider model capacity")


class CapacityToken:
    def __init__(self, capacity: "ModelCapacity", provider: str, evaluation: bool) -> None:
        self._capacity, self._provider, self._evaluation = capacity, provider, evaluation
        self._released = False

    def release(self) -> None:
        if not self._released:
            self._released = True
            self._capacity._release(self._provider, self._evaluation)

    def __enter__(self) -> "CapacityToken":
        return self

    def __exit__(self, *args: object) -> None:
        self.release()


class ModelCapacity:
    """Nonblocking admission: no local waiter holds a persisted lease or database session.

    All operations are synchronous within one event loop, hence no await can interleave
    the counter checks and updates. This deliberately is not a distributed limiter.
    """

    def __init__(self, providers: dict[str, ProviderCapacity], evaluation_limit: int) -> None:
        if type(evaluation_limit) is not int or not 1 <= evaluation_limit <= 32:
            raise ValueError("Invalid global evaluation capacity")
        self._providers = dict(providers)
        self._limit = evaluation_limit
        self._active = {provider: 0 for provider in providers}
        self._evaluations = {provider: 0 for provider in providers}

    def try_acquire(self, provider: str, *, evaluation: bool) -> CapacityToken | None:
        if provider not in self._providers:
            raise ValueError("Provider capacity is not configured")
        limits = self._providers[provider]
        if self._active[provider] >= limits.total:
            return None
        if evaluation and (
            sum(self._evaluations.values()) >= self._limit
            or self._evaluations[provider] >= limits.total - limits.generation_reserved
        ):
            return None
        self._active[provider] += 1
        self._evaluations[provider] += int(evaluation)
        return CapacityToken(self, provider, evaluation)

    def _release(self, provider: str, evaluation: bool) -> None:
        self._active[provider] -= 1
        self._evaluations[provider] -= int(evaluation)
