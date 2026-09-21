"""Deployment selection for new Runs only; deliberately outside release fingerprints."""

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class ExecutionMode:
    value: Literal["serial_v1", "candidate_v2"] = "serial_v1"


SERIAL_EXECUTION_MODE = ExecutionMode()


@dataclass(frozen=True)
class EvaluationRuntimeLimits:
    parallel_calls: int = 1


SERIAL_RUNTIME_LIMITS = EvaluationRuntimeLimits()
