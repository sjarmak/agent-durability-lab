"""Immutable values for the coding-agent durability protocol v1."""

from __future__ import annotations

import base64
import hashlib
import hmac
import re
import secrets
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Never, SupportsIndex

SCHEMA_VERSION = "1.0.0"
_STABLE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,159}$")
_DIGEST = re.compile(r"^sha256:[a-f0-9]{64}$")
_REASON_CODE = re.compile(r"^[a-z][a-z0-9_]{0,79}$")
_UTC_TIMESTAMP = re.compile(
    r"^[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])"
    r"T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](\.[0-9]+)?Z$"
)
_TERMINAL_LIFECYCLES = frozenset({"succeeded", "canceled", "unresolved"})


def validate_stable_id(value: str, field_name: str) -> None:
    if not _STABLE_ID.fullmatch(value):
        raise ValueError(f"{field_name} must be a protocol stable ID")


def validate_digest(value: str, field_name: str) -> None:
    if not _DIGEST.fullmatch(value):
        raise ValueError(f"{field_name} must be a sha256 digest")


def validate_reason_code(value: str) -> None:
    if not _REASON_CODE.fullmatch(value):
        raise ValueError("reason_code must be lower snake case and at most 80 characters")


def validate_utc_timestamp(value: str, field_name: str = "occurred_at") -> None:
    if not _UTC_TIMESTAMP.fullmatch(value):
        raise ValueError(f"{field_name} must use the protocol UTC timestamp format")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as error:
        raise ValueError(f"{field_name} must be a real UTC instant") from error
    offset = parsed.utcoffset()
    if offset is None or offset.total_seconds() != 0:
        raise ValueError(f"{field_name} must be a real UTC instant")


class OwnerCapability:
    """Opaque 256-bit bearer value with explicit export and no pickle support."""

    __slots__ = ("__secret",)

    def __init__(self, secret: bytes) -> None:
        if len(secret) != 32:
            raise ValueError("owner capability must contain exactly 256 bits")
        self.__secret = bytes(secret)

    @classmethod
    def mint(cls) -> OwnerCapability:
        return cls(secrets.token_bytes(32))

    @classmethod
    def parse(cls, encoded: str) -> OwnerCapability:
        try:
            padded = encoded + "=" * (-len(encoded) % 4)
            decoded = base64.b64decode(padded, altchars=b"-_", validate=True)
        except (ValueError, UnicodeEncodeError) as error:
            raise ValueError("owner capability must be canonical base64url") from error
        canonical = base64.urlsafe_b64encode(decoded).decode("ascii").rstrip("=")
        if len(decoded) != 32 or canonical != encoded:
            raise ValueError("owner capability must be canonical base64url-encoded 256 bits")
        return cls(decoded)

    def export_secret(self) -> str:
        return base64.urlsafe_b64encode(self.__secret).decode("ascii").rstrip("=")

    def digest(self) -> str:
        return f"sha256:{hashlib.sha256(self.__secret).hexdigest()}"

    def matches_digest(self, digest: str) -> bool:
        return hmac.compare_digest(self.digest(), digest)

    def __repr__(self) -> str:
        return "OwnerCapability([REDACTED])"

    __str__ = __repr__

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, OwnerCapability):
            return NotImplemented
        return hmac.compare_digest(self.__secret, other.__secret)

    __hash__ = None  # type: ignore[assignment]

    def __deepcopy__(self, memo: dict[int, object]) -> OwnerCapability:
        del memo
        return self

    def __reduce_ex__(self, protocol: SupportsIndex) -> Never:
        del protocol
        raise TypeError("owner capabilities cannot be pickled")


def mint_owner_capability() -> OwnerCapability:
    """Mint a cryptographically random 256-bit bearer capability."""

    return OwnerCapability.mint()


def capability_digest(capability: OwnerCapability) -> str:
    if not isinstance(capability, OwnerCapability):
        raise ValueError("owner capability must be an opaque OwnerCapability")
    return capability.digest()


class Lifecycle(StrEnum):
    CLAIMED = "claimed"
    STARTING = "starting"
    RUNNING = "running"
    COMPLETING = "completing"
    SUCCEEDED = "succeeded"
    CANCELED = "canceled"
    UNRESOLVED = "unresolved"


class AuthorityStatus(StrEnum):
    CURRENT = "current"
    REVOKED = "revoked"


class Operation(StrEnum):
    CLAIM = "claim"
    BEGIN_START = "begin_start"
    REGISTER = "register"
    ATTACH = "attach"
    REPLACE = "replace"
    OBSERVE_PROGRESS = "observe_progress"
    PUBLISH_EFFECT_RECEIPT = "publish_effect_receipt"
    PUBLISH_RESULT = "publish_result"
    COMPLETE = "complete"
    CANCEL = "cancel"
    MARK_UNRESOLVED = "mark_unresolved"
    RECORD_STOP_DELIVERY = "record_stop_delivery"
    ACKNOWLEDGE_STOP = "acknowledge_stop"


class Disposition(StrEnum):
    ACCEPTED = "accepted"
    REPLAYED = "replayed"


class RejectionType(StrEnum):
    CONFLICT = "conflict"
    STALE = "stale"
    REVOKED = "revoked"
    CANCELED = "canceled"


class DestinationCapability(StrEnum):
    ATOMIC_IDEMPOTENCY_KEY = "atomic_idempotency_key"
    TRANSACTIONAL_UNIQUE_EFFECT_IDENTITY = "transactional_unique_effect_identity"
    STABLE_MESSAGE_IDENTITY = "stable_message_identity"
    SERIALIZED_CORRELATION_LOOKUP = "serialized_correlation_lookup"
    CONDITIONAL_VERSIONED_GIT_MUTATION = "conditional_versioned_git_mutation"
    CONTENT_ADDRESSED_BLOB = "content_addressed_blob"
    MANUAL_RECONCILIATION = "manual_reconciliation"


class EffectOutcome(StrEnum):
    COMMITTED = "committed"
    RECONCILED = "reconciled"
    UNRESOLVED = "unresolved"


@dataclass(frozen=True, slots=True)
class LogicalIdentity:
    session_id: str
    turn_id: str

    def __post_init__(self) -> None:
        validate_stable_id(self.session_id, "session_id")
        validate_stable_id(self.turn_id, "turn_id")


@dataclass(frozen=True, slots=True)
class TurnState:
    lifecycle: Lifecycle
    generation: int
    owner_capability_digest: str
    authority_status: AuthorityStatus

    def __post_init__(self) -> None:
        if self.generation < 1:
            raise ValueError("generation must be at least 1")
        validate_digest(self.owner_capability_digest, "owner_capability_digest")
        if (
            self.lifecycle.value in _TERMINAL_LIFECYCLES
            and self.authority_status is AuthorityStatus.CURRENT
        ):
            raise ValueError("terminal lifecycle must have revoked authority")


@dataclass(frozen=True, slots=True)
class OperationRequest:
    operation_id: str
    request_hash: str
    transition_id: str
    receipt_id: str
    occurred_at: str

    def __post_init__(self) -> None:
        validate_stable_id(self.operation_id, "operation_id")
        validate_digest(self.request_hash, "request_hash")
        validate_stable_id(self.transition_id, "transition_id")
        validate_stable_id(self.receipt_id, "receipt_id")
        validate_utc_timestamp(self.occurred_at)


@dataclass(frozen=True, slots=True)
class ExecutorAuthorization:
    generation: int
    capability: OwnerCapability = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if self.generation < 1:
            raise ValueError("generation must be at least 1")
        if not isinstance(self.capability, OwnerCapability):
            raise ValueError("capability must be an opaque OwnerCapability")


@dataclass(frozen=True, slots=True)
class ProcessExecutorIdentity:
    process_id: str
    start_identity: str
    kind: str = field(default="process", init=False)

    def __post_init__(self) -> None:
        if not 1 <= len(self.process_id) <= 256:
            raise ValueError("process_id must contain 1 to 256 characters")
        if not 1 <= len(self.start_identity) <= 256:
            raise ValueError("start_identity must contain 1 to 256 characters")


@dataclass(frozen=True, slots=True)
class ProviderExecutorIdentity:
    provider: str
    session_id: str
    kind: str = field(default="provider", init=False)

    def __post_init__(self) -> None:
        if not 1 <= len(self.provider) <= 80:
            raise ValueError("provider must contain 1 to 80 characters")
        if not 1 <= len(self.session_id) <= 256:
            raise ValueError("provider session_id must contain 1 to 256 characters")


type ExecutorIdentity = ProcessExecutorIdentity | ProviderExecutorIdentity


@dataclass(frozen=True, slots=True)
class KindSubject:
    kind: str


@dataclass(frozen=True, slots=True)
class ExecutorSubject:
    kind: str
    executor_identity: ExecutorIdentity


@dataclass(frozen=True, slots=True)
class ReplacementSubject:
    previous_generation: int
    replacement_generation: int
    kind: str = field(default="replacement", init=False)


@dataclass(frozen=True, slots=True)
class ProgressSubject:
    progress_hash: str
    kind: str = field(default="progress", init=False)

    def __post_init__(self) -> None:
        validate_digest(self.progress_hash, "progress_hash")


@dataclass(frozen=True, slots=True)
class EffectSubject:
    effect_id: str
    destination_namespace: str
    destination_capability: DestinationCapability
    outcome: EffectOutcome
    destination_receipt_id: str | None = None
    kind: str = field(default="effect", init=False)

    def __post_init__(self) -> None:
        validate_stable_id(self.effect_id, "effect_id")
        validate_stable_id(self.destination_namespace, "destination_namespace")
        if self.destination_receipt_id is not None and not self.destination_receipt_id:
            raise ValueError("destination_receipt_id cannot be empty")


@dataclass(frozen=True, slots=True)
class ResultSubject:
    result_hash: str
    system_of_record_check: str
    kind: str = field(default="result", init=False)

    def __post_init__(self) -> None:
        validate_digest(self.result_hash, "result_hash")
        validate_stable_id(self.system_of_record_check, "system_of_record_check")


@dataclass(frozen=True, slots=True)
class CompletionSubject:
    result_receipt_id: str
    kind: str = field(default="completion", init=False)

    def __post_init__(self) -> None:
        validate_stable_id(self.result_receipt_id, "result_receipt_id")


@dataclass(frozen=True, slots=True)
class ReasonSubject:
    kind: str
    reason_code: str

    def __post_init__(self) -> None:
        validate_reason_code(self.reason_code)


@dataclass(frozen=True, slots=True)
class StopTarget:
    generation: int
    executor_identity: ExecutorIdentity

    def __post_init__(self) -> None:
        if self.generation < 1:
            raise ValueError("target generation must be at least 1")


@dataclass(frozen=True, slots=True)
class StopSubject:
    target: StopTarget
    delivery_status: str
    kind: str = field(default="stop", init=False)

    def __post_init__(self) -> None:
        if self.delivery_status not in {"delivered", "acknowledged"}:
            raise ValueError("invalid stop delivery status")


type ReceiptSubject = (
    KindSubject
    | ExecutorSubject
    | ReplacementSubject
    | ProgressSubject
    | EffectSubject
    | ResultSubject
    | CompletionSubject
    | ReasonSubject
    | StopSubject
)


@dataclass(frozen=True, slots=True)
class Receipt:
    receipt_id: str
    receipt_type: str
    request_hash: str
    recorded_at: str
    subject: ReceiptSubject

    def __post_init__(self) -> None:
        validate_stable_id(self.receipt_id, "receipt_id")
        validate_digest(self.request_hash, "request_hash")
        validate_utc_timestamp(self.recorded_at, "recorded_at")


@dataclass(frozen=True, slots=True)
class Rejection:
    result_type: RejectionType
    request_hash: str
    observed_at: str
    reason_code: str
    current_generation: int | None = None

    def __post_init__(self) -> None:
        validate_digest(self.request_hash, "request_hash")
        validate_utc_timestamp(self.observed_at, "observed_at")
        validate_reason_code(self.reason_code)
        if self.current_generation is not None and self.current_generation < 1:
            raise ValueError("current_generation must be at least 1")


@dataclass(frozen=True, slots=True)
class TransitionRecord:
    operation: Operation
    operation_id: str
    request_hash: str
    transition_id: str
    before: TurnState | None
    after: TurnState | None
    receipt: Receipt


@dataclass(frozen=True, slots=True)
class TransitionResult:
    disposition: Disposition
    transition_id: str
    before: TurnState | None
    after: TurnState | None
    receipt: Receipt
    original_transition_id: str | None = None
