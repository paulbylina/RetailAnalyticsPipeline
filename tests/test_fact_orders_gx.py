import pytest

from src.quality.validate_fact_orders_with_gx import main


pytestmark = pytest.mark.warehouse


def test_fact_orders_gx_validation():
    result = main()
    assert result == 0, "Great Expectations validation failed"