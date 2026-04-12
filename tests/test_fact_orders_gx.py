from pathlib import Path

import pytest

from src.quality.validate_fact_orders_with_gx import main


pytestmark = pytest.mark.warehouse

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DUCKDB_PATH = PROJECT_ROOT / "data" / "warehouse" / "retail.duckdb"

if not DUCKDB_PATH.exists():
    pytest.skip(
        f"Missing DuckDB database: {DUCKDB_PATH}. Run the warehouse pipeline first.",
        allow_module_level=True,
    )


def test_fact_orders_gx_validation():
    result = main()
    assert result == 0, "Great Expectations validation failed"