from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class Readiness:
    database: str

    @property
    def ready(self) -> bool:
        return self.database == "connected"


class DatabaseProbe(Protocol):
    async def check(self) -> Readiness: ...


class CheckReadiness:
    def __init__(self, probe: DatabaseProbe) -> None:
        self.probe = probe

    async def execute(self) -> Readiness:
        return await self.probe.check()
