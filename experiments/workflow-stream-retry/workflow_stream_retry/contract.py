from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class Scenario(StrEnum):
    UNFAULTED = "unfaulted"
    PRE_FLUSH_LOSS = "pre-flush-loss"
    POST_FLUSH_DUPLICATE = "post-flush-duplicate"


@dataclass(frozen=True)
class StreamEvent:
    logical_output_id: str
    kind: str
    attempt: int
    worker_id: str
    chunk_index: int | None = None
    text: str = ""


@dataclass(frozen=True)
class StreamObservation:
    offset: int
    event: StreamEvent


@dataclass(frozen=True)
class WorkflowInput:
    scenario: Scenario
    trial: int
    expected_output: str


@dataclass(frozen=True)
class ActivityInput:
    scenario: Scenario
    trial: int
    expected_output: str
    logical_output_id: str


@dataclass(frozen=True)
class ActivityResult:
    full_text: str
    attempt: int
    worker_id: str


@dataclass(frozen=True)
class WorkflowResult:
    full_text: str
    final_attempt: int
    final_worker_id: str
    acknowledged_offset: int


@dataclass(frozen=True)
class PublishedBatch:
    publisher_id: str
    sequence: int
    activity_attempt: int
    offsets: tuple[int, ...]


@dataclass(frozen=True)
class TrialRecord:
    scenario: Scenario
    trial: int
    workflow_id: str
    run_id: str
    expected_output: str
    final_attempt: int
    final_worker_id: str
    acknowledged_offset: int
    observations: tuple[StreamObservation, ...]
    batches: tuple[PublishedBatch, ...]


@dataclass(frozen=True)
class TrialVerdict:
    valid: bool
    naive_output: str
    retry_aware_output: str
    naive_duplicate_control_failed: bool
