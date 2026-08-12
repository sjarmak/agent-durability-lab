from __future__ import annotations

from pathlib import Path

from workflow_stream_retry.product_metrics import measure_recovery_surface


def test_sdk_product_removes_application_owned_protocol_state_and_branches() -> None:
    project_root = Path(__file__).parents[1]

    metrics = measure_recovery_surface(project_root)

    assert metrics.manual.protocol_lines > 100
    assert metrics.manual.state_fields >= 10
    assert metrics.manual.branches >= 10
    assert metrics.product.protocol_lines == 0
    assert metrics.product.state_fields == 0
    assert metrics.product.branches == 0
    assert metrics.product.sdk_operations == (
        "logical_output_publisher",
        "logical_output_reconstructor",
        "validate_logical_output_acknowledgement",
    )
