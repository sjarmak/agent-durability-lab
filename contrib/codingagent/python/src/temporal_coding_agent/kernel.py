"""Pure guarded state transitions for the durability protocol."""

from __future__ import annotations

from dataclasses import dataclass

from .errors import (
    AuthorizationError,
    CanceledTurnError,
    InvalidTransitionError,
    OperationConflictError,
    RevokedAuthorityError,
    StaleAuthorityError,
)
from .models import (
    AuthorityStatus,
    CompletionSubject,
    DestinationCapability,
    Disposition,
    EffectOutcome,
    EffectSubject,
    ExecutorAuthorization,
    ExecutorIdentity,
    ExecutorSubject,
    KindSubject,
    Lifecycle,
    LogicalIdentity,
    Operation,
    OperationRequest,
    OwnerCapability,
    ProgressSubject,
    ReasonSubject,
    Receipt,
    ReceiptSubject,
    Rejection,
    RejectionType,
    ReplacementSubject,
    ResultSubject,
    StopSubject,
    StopTarget,
    TransitionRecord,
    TransitionResult,
    TurnState,
    capability_digest,
)

_NONTERMINAL = frozenset(
    {Lifecycle.CLAIMED, Lifecycle.STARTING, Lifecycle.RUNNING, Lifecycle.COMPLETING}
)


@dataclass(frozen=True, slots=True)
class ProtocolKernel:
    """An immutable state-machine snapshot scoped to one session and turn."""

    identity: LogicalIdentity
    state: TurnState | None = None
    records: tuple[TransitionRecord, ...] = ()

    def claim(
        self,
        request: OperationRequest,
        *,
        owner_capability: OwnerCapability,
        coordinator_authenticated: bool,
    ) -> tuple[ProtocolKernel, TransitionResult]:
        self._require_coordinator(coordinator_authenticated)
        replay = self._preflight(Operation.CLAIM, request)
        if replay is not None:
            if replay.after is None or not owner_capability.matches_digest(
                replay.after.owner_capability_digest
            ):
                self._raise_conflict(request, "operation_content_changed")
            return self, replay
        if self.state is not None:
            raise InvalidTransitionError("claim requires an absent turn")
        after = TurnState(
            Lifecycle.CLAIMED,
            1,
            capability_digest(owner_capability),
            AuthorityStatus.CURRENT,
        )
        return self._commit(Operation.CLAIM, request, after, "claim", KindSubject("claim"))

    def begin_start(
        self, request: OperationRequest, authorization: ExecutorAuthorization
    ) -> tuple[ProtocolKernel, TransitionResult]:
        replay = self._preflight(Operation.BEGIN_START, request, authorization)
        if replay is not None:
            return self, replay
        state = self._authorize_executor(request, authorization)
        self._require_lifecycle(state, Lifecycle.CLAIMED)
        after = self._state_with(state, lifecycle=Lifecycle.STARTING)
        return self._commit(Operation.BEGIN_START, request, after, "start", KindSubject("start"))

    def register(
        self,
        request: OperationRequest,
        authorization: ExecutorAuthorization,
        executor_identity: ExecutorIdentity,
    ) -> tuple[ProtocolKernel, TransitionResult]:
        replay = self._preflight(Operation.REGISTER, request, authorization)
        if replay is not None:
            self._require_subject(
                request, replay, ExecutorSubject("registration", executor_identity)
            )
            return self, replay
        state = self._authorize_executor(request, authorization)
        self._require_lifecycle(state, Lifecycle.STARTING)
        known_executor = self._known_executor(state.generation)
        if known_executor is not None and known_executor != executor_identity:
            self._raise_conflict(request, "executor_identity_conflict")
        after = self._state_with(state, lifecycle=Lifecycle.RUNNING)
        subject = ExecutorSubject("registration", executor_identity)
        return self._commit(Operation.REGISTER, request, after, "registration", subject)

    def attach(
        self,
        request: OperationRequest,
        authorization: ExecutorAuthorization,
        executor_identity: ExecutorIdentity,
        *,
        executor_discovered: bool = False,
    ) -> tuple[ProtocolKernel, TransitionResult]:
        replay = self._preflight(Operation.ATTACH, request, authorization)
        if replay is not None:
            self._require_subject(
                request, replay, ExecutorSubject("attachment", executor_identity)
            )
            return self, replay
        state = self._authorize_executor(request, authorization)
        self._require_lifecycle(
            state, Lifecycle.STARTING, Lifecycle.RUNNING, Lifecycle.COMPLETING
        )
        known_executor = self._known_executor(state.generation)
        if known_executor is not None and known_executor != executor_identity:
            self._raise_conflict(request, "executor_identity_conflict")
        if known_executor is None and executor_discovered is not True:
            raise InvalidTransitionError("attach requires authenticated executor discovery")
        subject = ExecutorSubject("attachment", executor_identity)
        return self._commit(Operation.ATTACH, request, state, "attachment", subject)

    def replace(
        self,
        request: OperationRequest,
        *,
        expected_generation: int,
        replacement_capability: OwnerCapability,
        coordinator_authenticated: bool,
    ) -> tuple[ProtocolKernel, TransitionResult]:
        self._require_coordinator(coordinator_authenticated)
        replay = self._preflight(Operation.REPLACE, request)
        if replay is not None:
            self._require_subject(
                request,
                replay,
                ReplacementSubject(expected_generation, expected_generation + 1),
            )
            if replay.after is None or not replacement_capability.matches_digest(
                replay.after.owner_capability_digest
            ):
                self._raise_conflict(request, "operation_content_changed")
            return self, replay
        state = self._require_state()
        self._require_nonterminal(state)
        self._require_expected_generation(request, state, expected_generation)
        after = TurnState(
            Lifecycle.STARTING,
            state.generation + 1,
            capability_digest(replacement_capability),
            AuthorityStatus.CURRENT,
        )
        subject = ReplacementSubject(state.generation, after.generation)
        return self._commit(Operation.REPLACE, request, after, "replacement", subject)

    def observe_progress(
        self,
        request: OperationRequest,
        authorization: ExecutorAuthorization,
        progress_hash: str,
    ) -> tuple[ProtocolKernel, TransitionResult]:
        replay = self._preflight(Operation.OBSERVE_PROGRESS, request, authorization)
        if replay is not None:
            self._require_subject(request, replay, ProgressSubject(progress_hash))
            return self, replay
        state = self._authorize_executor(request, authorization)
        self._require_lifecycle(state, Lifecycle.RUNNING)
        return self._commit(
            Operation.OBSERVE_PROGRESS,
            request,
            state,
            "progress",
            ProgressSubject(progress_hash),
        )

    def publish_effect_receipt(
        self,
        request: OperationRequest,
        authorization: ExecutorAuthorization,
        *,
        effect_id: str,
        destination_namespace: str,
        destination_capability: DestinationCapability,
        outcome: EffectOutcome,
        destination_receipt_id: str | None = None,
    ) -> tuple[ProtocolKernel, TransitionResult]:
        subject = EffectSubject(
            effect_id,
            destination_namespace,
            destination_capability,
            outcome,
            destination_receipt_id,
        )
        replay = self._preflight(Operation.PUBLISH_EFFECT_RECEIPT, request, authorization)
        if replay is not None:
            self._require_subject(request, replay, subject)
            return self, replay
        state = self._authorize_executor(request, authorization)
        self._require_lifecycle(state, Lifecycle.RUNNING)
        self._require_unique_effect(request, subject)
        return self._commit(
            Operation.PUBLISH_EFFECT_RECEIPT, request, state, "effect", subject
        )

    def publish_result(
        self,
        request: OperationRequest,
        authorization: ExecutorAuthorization,
        *,
        result_hash: str,
        system_of_record_check: str,
    ) -> tuple[ProtocolKernel, TransitionResult]:
        subject = ResultSubject(result_hash, system_of_record_check)
        replay = self._preflight(Operation.PUBLISH_RESULT, request, authorization)
        if replay is not None:
            self._require_subject(request, replay, subject)
            return self, replay
        state = self._authorize_executor(request, authorization)
        self._require_lifecycle(state, Lifecycle.RUNNING)
        after = self._state_with(state, lifecycle=Lifecycle.COMPLETING)
        return self._commit(Operation.PUBLISH_RESULT, request, after, "result", subject)

    def complete(
        self,
        request: OperationRequest,
        authorization: ExecutorAuthorization,
        *,
        result_receipt_id: str,
    ) -> tuple[ProtocolKernel, TransitionResult]:
        subject = CompletionSubject(result_receipt_id)
        replay = self._preflight(Operation.COMPLETE, request, authorization)
        if replay is not None:
            self._require_subject(request, replay, subject)
            return self, replay
        state = self._authorize_executor(request, authorization)
        self._require_lifecycle(state, Lifecycle.COMPLETING)
        if self._candidate_result_receipt_id() != result_receipt_id:
            self._raise_conflict(request, "result_receipt_mismatch")
        after = self._state_with(
            state, lifecycle=Lifecycle.SUCCEEDED, authority=AuthorityStatus.REVOKED
        )
        return self._commit(Operation.COMPLETE, request, after, "completion", subject)

    def cancel(
        self,
        request: OperationRequest,
        *,
        expected_generation: int,
        reason_code: str,
        coordinator_authenticated: bool,
    ) -> tuple[ProtocolKernel, TransitionResult]:
        subject = ReasonSubject("cancellation", reason_code)
        self._require_coordinator(coordinator_authenticated)
        replay = self._preflight(Operation.CANCEL, request)
        if replay is not None:
            self._require_replay_generation(request, replay, expected_generation)
            self._require_subject(request, replay, subject)
            return self, replay
        state = self._authorize_coordinator(request, expected_generation)
        after = self._state_with(
            state, lifecycle=Lifecycle.CANCELED, authority=AuthorityStatus.REVOKED
        )
        return self._commit(Operation.CANCEL, request, after, "cancellation", subject)

    def mark_unresolved(
        self,
        request: OperationRequest,
        *,
        expected_generation: int,
        reason_code: str,
        coordinator_authenticated: bool,
    ) -> tuple[ProtocolKernel, TransitionResult]:
        subject = ReasonSubject("unresolved", reason_code)
        self._require_coordinator(coordinator_authenticated)
        replay = self._preflight(Operation.MARK_UNRESOLVED, request)
        if replay is not None:
            self._require_replay_generation(request, replay, expected_generation)
            self._require_subject(request, replay, subject)
            return self, replay
        state = self._authorize_coordinator(request, expected_generation)
        after = self._state_with(
            state, lifecycle=Lifecycle.UNRESOLVED, authority=AuthorityStatus.REVOKED
        )
        return self._commit(Operation.MARK_UNRESOLVED, request, after, "unresolved", subject)

    def record_stop_delivery(
        self,
        request: OperationRequest,
        target: StopTarget,
        *,
        coordinator_authenticated: bool,
    ) -> tuple[ProtocolKernel, TransitionResult]:
        return self._record_stop(
            Operation.RECORD_STOP_DELIVERY,
            request,
            target,
            "delivered",
            "stop_delivery",
            coordinator_authenticated,
        )

    def acknowledge_stop(
        self,
        request: OperationRequest,
        target: StopTarget,
        *,
        coordinator_authenticated: bool,
    ) -> tuple[ProtocolKernel, TransitionResult]:
        return self._record_stop(
            Operation.ACKNOWLEDGE_STOP,
            request,
            target,
            "acknowledged",
            "stop_ack",
            coordinator_authenticated,
        )

    def _record_stop(
        self,
        operation: Operation,
        request: OperationRequest,
        target: StopTarget,
        delivery_status: str,
        receipt_type: str,
        coordinator_authenticated: bool,
    ) -> tuple[ProtocolKernel, TransitionResult]:
        subject = StopSubject(target, delivery_status)
        self._require_coordinator(coordinator_authenticated)
        replay = self._preflight(operation, request)
        if replay is not None:
            self._require_subject(request, replay, subject)
            return self, replay
        state = self._require_state()
        if target.generation > state.generation:
            raise InvalidTransitionError("stop target generation cannot be in the future")
        target_is_revoked = target.generation < state.generation
        turn_is_revoked = state.authority_status is AuthorityStatus.REVOKED
        if not (target_is_revoked or turn_is_revoked):
            raise InvalidTransitionError("stop target must be a revoked generation or turn")
        known_executor = self._known_executor(target.generation)
        if known_executor is None:
            raise InvalidTransitionError("stop target has no durable executor identity")
        if known_executor != target.executor_identity:
            self._raise_conflict(request, "stop_target_mismatch")
        if operation is Operation.ACKNOWLEDGE_STOP and not self._has_stop_delivery(target):
            raise InvalidTransitionError("stop acknowledgement requires matching delivery")
        return self._commit(operation, request, state, receipt_type, subject)

    def _preflight(
        self,
        operation: Operation,
        request: OperationRequest,
        authorization: ExecutorAuthorization | None = None,
    ) -> TransitionResult | None:
        record = next(
            (item for item in self.records if item.operation_id == request.operation_id), None
        )
        if record is None:
            return None
        if authorization is not None:
            self._authorize_record_executor(request, record, authorization)
        if record.request_hash != request.request_hash or record.operation is not operation:
            self._raise_conflict(request, "operation_content_changed")
        return TransitionResult(
            Disposition.REPLAYED,
            request.transition_id,
            record.before,
            record.after,
            record.receipt,
            record.transition_id,
        )

    def _commit(
        self,
        operation: Operation,
        request: OperationRequest,
        after: TurnState,
        receipt_type: str,
        subject: ReceiptSubject,
    ) -> tuple[ProtocolKernel, TransitionResult]:
        receipt = Receipt(
            request.receipt_id,
            receipt_type,
            request.request_hash,
            request.occurred_at,
            subject,
        )
        record = TransitionRecord(
            operation,
            request.operation_id,
            request.request_hash,
            request.transition_id,
            self.state,
            after,
            receipt,
        )
        kernel = ProtocolKernel(self.identity, after, (*self.records, record))
        result = TransitionResult(
            Disposition.ACCEPTED, request.transition_id, self.state, after, receipt
        )
        return kernel, result

    def _authorize_executor(
        self, request: OperationRequest, authorization: ExecutorAuthorization
    ) -> TurnState:
        state = self._require_state()
        if state.lifecycle is Lifecycle.CANCELED:
            self._raise_rejected(
                request,
                CanceledTurnError,
                RejectionType.CANCELED,
                "turn_canceled",
                state.generation,
            )
        if authorization.generation != state.generation:
            self._raise_rejected(
                request,
                StaleAuthorityError,
                RejectionType.STALE,
                "generation_superseded",
                state.generation,
            )
        if state.authority_status is AuthorityStatus.REVOKED:
            self._raise_rejected(
                request,
                RevokedAuthorityError,
                RejectionType.REVOKED,
                "owner_revoked",
                state.generation,
            )
        if not authorization.capability.matches_digest(state.owner_capability_digest):
            self._raise_rejected(
                request,
                RevokedAuthorityError,
                RejectionType.REVOKED,
                "capability_mismatch",
                state.generation,
            )
        return state

    def _authorize_record_executor(
        self,
        request: OperationRequest,
        record: TransitionRecord,
        authorization: ExecutorAuthorization,
    ) -> None:
        authority_state = record.before or record.after
        if authority_state is None:
            raise InvalidTransitionError("executor replay has no authority state")
        if authorization.generation != authority_state.generation:
            current_generation = self.state.generation if self.state else authority_state.generation
            self._raise_rejected(
                request,
                StaleAuthorityError,
                RejectionType.STALE,
                "generation_superseded",
                current_generation,
            )
        if not authorization.capability.matches_digest(authority_state.owner_capability_digest):
            self._raise_rejected(
                request,
                RevokedAuthorityError,
                RejectionType.REVOKED,
                "capability_mismatch",
                authority_state.generation,
            )

    def _authorize_coordinator(
        self,
        request: OperationRequest,
        expected_generation: int,
    ) -> TurnState:
        state = self._require_state()
        self._require_nonterminal(state)
        self._require_expected_generation(request, state, expected_generation)
        return state

    def _require_expected_generation(
        self, request: OperationRequest, state: TurnState, expected_generation: int
    ) -> None:
        if expected_generation != state.generation:
            rejection = Rejection(
                RejectionType.STALE,
                request.request_hash,
                request.occurred_at,
                "generation_superseded",
                state.generation,
            )
            raise StaleAuthorityError("expected generation is stale", rejection)

    def _raise_rejected(
        self,
        request: OperationRequest,
        error_type: type[CanceledTurnError | StaleAuthorityError | RevokedAuthorityError],
        result_type: RejectionType,
        reason_code: str,
        current_generation: int,
    ) -> None:
        rejection = Rejection(
            result_type,
            request.request_hash,
            request.occurred_at,
            reason_code,
            current_generation,
        )
        raise error_type(reason_code, rejection)

    def _raise_conflict(self, request: OperationRequest, reason_code: str) -> None:
        generation = self.state.generation if self.state is not None else None
        rejection = Rejection(
            RejectionType.CONFLICT,
            request.request_hash,
            request.occurred_at,
            reason_code,
            generation,
        )
        raise OperationConflictError(reason_code, rejection)

    def _require_subject(
        self,
        request: OperationRequest,
        result: TransitionResult,
        expected: ReceiptSubject,
    ) -> None:
        if result.receipt.subject != expected:
            self._raise_conflict(request, "operation_content_changed")

    def _require_replay_generation(
        self,
        request: OperationRequest,
        result: TransitionResult,
        expected_generation: int,
    ) -> None:
        authority_state = result.before or result.after
        if authority_state is None or authority_state.generation != expected_generation:
            self._raise_conflict(request, "operation_content_changed")

    @staticmethod
    def _require_coordinator(authenticated: bool) -> None:
        if authenticated is not True:
            raise AuthorizationError("authenticated coordinator is required")

    def _require_state(self) -> TurnState:
        if self.state is None:
            raise InvalidTransitionError("operation requires an existing turn")
        return self.state

    @staticmethod
    def _require_lifecycle(state: TurnState, *allowed: Lifecycle) -> None:
        if state.lifecycle not in allowed:
            expected = ", ".join(item.value for item in allowed)
            raise InvalidTransitionError(
                f"lifecycle {state.lifecycle.value} does not admit operation; expected {expected}"
            )

    @staticmethod
    def _require_nonterminal(state: TurnState) -> None:
        if state.lifecycle not in _NONTERMINAL:
            raise InvalidTransitionError("operation requires a nonterminal turn")

    @staticmethod
    def _state_with(
        state: TurnState,
        *,
        lifecycle: Lifecycle,
        authority: AuthorityStatus = AuthorityStatus.CURRENT,
    ) -> TurnState:
        return TurnState(
            lifecycle,
            state.generation,
            state.owner_capability_digest,
            authority,
        )

    def _known_executor(self, generation: int) -> ExecutorIdentity | None:
        for record in reversed(self.records):
            subject = record.receipt.subject
            if (
                record.after is not None
                and record.after.generation == generation
                and isinstance(subject, ExecutorSubject)
                and subject.kind in {"registration", "attachment"}
            ):
                return subject.executor_identity
        return None

    def _has_stop_delivery(self, target: StopTarget) -> bool:
        return any(
            record.operation is Operation.RECORD_STOP_DELIVERY
            and isinstance(record.receipt.subject, StopSubject)
            and record.receipt.subject.target == target
            for record in self.records
        )

    def _candidate_result_receipt_id(self) -> str | None:
        for record in reversed(self.records):
            if isinstance(record.receipt.subject, ResultSubject):
                return record.receipt.receipt_id
        return None

    def _require_unique_effect(self, request: OperationRequest, subject: EffectSubject) -> None:
        for record in self.records:
            prior = record.receipt.subject
            if isinstance(prior, EffectSubject) and prior.effect_id == subject.effect_id:
                self._raise_conflict(request, "effect_identity_conflict")
