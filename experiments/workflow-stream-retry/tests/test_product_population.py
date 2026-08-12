from __future__ import annotations

import pytest

from workflow_stream_retry.product_population import audit_product_population


def test_product_population_rejects_an_incomplete_schedule() -> None:
    with pytest.raises(ValueError, match="schedule"):
        audit_product_population(())
