from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class Arm(StrEnum):
    RAW = "raw"
    MANUAL = "manual"
    PRODUCT = "product"


class ProductScenario(StrEnum):
    UNFAULTED = "unfaulted"
    PRE_FLUSH_LOSS = "pre-flush-loss"
    POST_FLUSH_PREFIX = "post-flush-prefix"
    TERMINAL_BEFORE_ACK = "terminal-before-ack"


@dataclass(frozen=True)
class OutputEvent:
    logical_output_id: str
    generation: int
    publisher_id: str
    activity_attempt: int
    worker_id: str
    kind: str
    sequence: int
    chunk_index: int | None = None
    text: str = ""
    chunk_count: int | None = None
    terminal_sha256: str = ""


@dataclass(frozen=True)
class OutputObservation:
    offset: int
    event: OutputEvent


@dataclass(frozen=True)
class WorkerChunk:
    text: str
    worker_id: str


@dataclass(frozen=True)
class OutputTerminal:
    topic: str
    logical_output_id: str
    generation: int
    terminal_sequence: int
    chunk_count: int
    content_sha256: str
    publisher_id: str


@dataclass(frozen=True)
class OutputAcknowledgement:
    topic: str
    logical_output_id: str
    generation: int
    terminal_sequence: int
    terminal_offset: int
    chunk_count: int
    content_sha256: str
    publisher_id: str


@dataclass(frozen=True)
class ProductWorkflowInput:
    arm: Arm
    scenario: ProductScenario
    trial: int
    expected_output: str


@dataclass(frozen=True)
class ProductActivityInput:
    arm: Arm
    scenario: ProductScenario
    trial: int
    expected_output: str
    logical_output_id: str


@dataclass(frozen=True)
class ProductActivityResult:
    full_text: str
    attempt: int
    worker_id: str
    terminal: OutputTerminal


@dataclass(frozen=True)
class ProductWorkflowResult:
    full_text: str
    final_attempt: int
    final_worker_id: str
    terminal: OutputTerminal
    acknowledgement: OutputAcknowledgement


@dataclass(frozen=True)
class ProductTrialRecord:
    arm: Arm
    scenario: ProductScenario
    trial: int
    workflow_id: str
    run_id: str
    expected_output: str
    final_attempt: int
    final_worker_id: str
    final_terminal: OutputTerminal
    acknowledgement: OutputAcknowledgement
    stale_ack_rejections: int
    observations: tuple[OutputObservation, ...]
    stream_batch_count: int
    history_event_count: int
    history_json_bytes: int


@dataclass(frozen=True)
class ProductTrialVerdict:
    valid: bool
    raw_concatenation: str
    reconstructed_output: str
    duplicate_output: bool
    stale_ack_accepted: bool
    stale_ack_rejections: int
