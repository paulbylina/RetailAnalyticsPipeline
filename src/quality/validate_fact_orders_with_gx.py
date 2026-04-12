from __future__ import annotations

import sys
import uuid
from pathlib import Path

import duckdb
import great_expectations as gx
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DUCKDB_PATH = PROJECT_ROOT / "data" / "warehouse" / "retail.duckdb"
TABLE_NAME = "fact_orders"


def load_fact_orders() -> pd.DataFrame:
    if not DUCKDB_PATH.exists():
        raise FileNotFoundError(f"DuckDB database not found: {DUCKDB_PATH}")

    with duckdb.connect(str(DUCKDB_PATH)) as conn:
        df = conn.execute(f"SELECT * FROM {TABLE_NAME}").df()

    if df.empty:
        raise ValueError(f"Table '{TABLE_NAME}' is empty.")

    return df


def result_success(result) -> bool:
    if hasattr(result, "success"):
        return bool(result.success)
    if isinstance(result, dict):
        return bool(result.get("success", False))
    try:
        return bool(result["success"])
    except Exception:
        return False


def expectation_name(result) -> str:
    if isinstance(result, dict):
        config = result.get("expectation_config", {})
        return str(config.get("type", "unknown_expectation"))
    if hasattr(result, "expectation_config"):
        config = result.expectation_config
        if hasattr(config, "type"):
            return str(config.type)
    return "unknown_expectation"


def main() -> int:
    try:
        df = load_fact_orders()

        context = gx.get_context()
        suffix = uuid.uuid4().hex[:8]

        data_source = context.data_sources.add_pandas(f"fact_orders_pandas_{suffix}")
        data_asset = data_source.add_dataframe_asset(f"fact_orders_asset_{suffix}")
        batch_definition = data_asset.add_batch_definition_whole_dataframe(
            f"fact_orders_batch_{suffix}"
        )
        batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

        expectations = [
            gx.expectations.ExpectTableRowCountToBeBetween(min_value=1),
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="order_id", severity="critical"
            ),
            gx.expectations.ExpectColumnValuesToBeUnique(
                column="order_id", severity="critical"
            ),
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="total_amount", severity="critical"
            ),
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="total_amount", min_value=0, severity="critical"
            ),
        ]

        all_passed = True

        print(f"Validating DuckDB table '{TABLE_NAME}' with {len(df)} rows")
        for expectation in expectations:
            result = batch.validate(expectation)
            success = result_success(result)
            name = expectation_name(result)
            status = "PASSED" if success else "FAILED"
            print(f"{status}: {name}")
            if not success:
                all_passed = False
                print(result)

        if not all_passed:
            raise ValueError("Great Expectations validation failed.")

        print("Great Expectations validation completed successfully.")
        return 0

    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())