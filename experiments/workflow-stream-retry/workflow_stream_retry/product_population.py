from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from .product_contract import (
    Arm,
    ProductScenario,
    ProductTrialRecord,
    ProductTrialVerdict,
)
from .product_oracle import audit_product_trial


@dataclass(frozen=True)
class ArmHistorySummary:
    arm: Arm
    trials: int
    stream_batches_total: int
    stream_batches_min: int
    stream_batches_max: int
    history_events_total: int
    history_json_bytes_total: int


@dataclass(frozen=True)
class ProductPopulationSummary:
    trials: int
    raw_duplicate_trials: int
    raw_stale_ack_trials: int
    protected_stale_ack_rejections: int
    maximum_product_batch_excess_over_manual: int
    history_by_arm: tuple[ArmHistorySummary, ...]


def audit_product_population(
    trials: Sequence[tuple[ProductTrialRecord, ProductTrialVerdict]],
) -> ProductPopulationSummary:
    expected_schedule = tuple(
        (arm, scenario, trial)
        for arm in Arm
        for scenario in ProductScenario
        for trial in range(1, 4)
    )
    actual_schedule = tuple((record.arm, record.scenario, record.trial) for record, _ in trials)
    if actual_schedule != expected_schedule:
        raise ValueError("product population schedule differs")
    for record, verdict in trials:
        if audit_product_trial(record) != verdict:
            raise ValueError("stored product trial verdict differs")

    indexed = {(record.arm, record.scenario, record.trial): record for record, _ in trials}
    batch_excesses = []
    for scenario in ProductScenario:
        for trial in range(1, 4):
            manual = indexed[(Arm.MANUAL, scenario, trial)]
            product = indexed[(Arm.PRODUCT, scenario, trial)]
            excess = product.stream_batch_count - manual.stream_batch_count
            if excess > 1:
                raise ValueError("product stream batch overhead exceeded its bound")
            batch_excesses.append(excess)

    summaries = []
    for arm in Arm:
        records = [record for record, _ in trials if record.arm is arm]
        batches = [record.stream_batch_count for record in records]
        summaries.append(
            ArmHistorySummary(
                arm=arm,
                trials=len(records),
                stream_batches_total=sum(batches),
                stream_batches_min=min(batches),
                stream_batches_max=max(batches),
                history_events_total=sum(record.history_event_count for record in records),
                history_json_bytes_total=sum(record.history_json_bytes for record in records),
            )
        )
    return ProductPopulationSummary(
        trials=len(trials),
        raw_duplicate_trials=sum(
            verdict.duplicate_output for (record, verdict) in trials if record.arm is Arm.RAW
        ),
        raw_stale_ack_trials=sum(
            verdict.stale_ack_accepted for (record, verdict) in trials if record.arm is Arm.RAW
        ),
        protected_stale_ack_rejections=sum(
            verdict.stale_ack_rejections
            for (record, verdict) in trials
            if record.arm is not Arm.RAW
        ),
        maximum_product_batch_excess_over_manual=max(batch_excesses),
        history_by_arm=tuple(summaries),
    )
