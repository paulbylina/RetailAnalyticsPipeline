from __future__ import annotations

import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
INPUT_PATH = PROJECT_ROOT / "data" / "processed" / "retail_orders_clean.jsonl"
OUTPUT_PATH = PROJECT_ROOT / "data" / "curated" / "fact_orders_spark.parquet"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("retail-fact-orders-spark")
        .master("local[*]")
        .getOrCreate()
    )


def main() -> int:
    spark = None
    try:
        if not INPUT_PATH.exists():
            raise FileNotFoundError(f"Input file not found: {INPUT_PATH}")

        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

        spark = build_spark()
        spark.sparkContext.setLogLevel("WARN")

        print(f"Reading cleaned retail orders from: {INPUT_PATH}")
        df = spark.read.json(str(INPUT_PATH))

        fact_orders_df = (
            df.select(
                col("order_id").cast(LongType()).alias("order_id"),
                col("customer_id").cast(LongType()).alias("customer_id"),
                col("product_id").cast(LongType()).alias("product_id"),
                col("category").cast(StringType()).alias("category"),
                col("quantity").cast(LongType()).alias("quantity"),
                col("unit_price").cast(DoubleType()).alias("unit_price"),
                col("total_amount").cast(DoubleType()).alias("total_amount"),
                col("payment_method").cast(StringType()).alias("payment_method"),
                col("order_status").cast(StringType()).alias("order_status"),
                col("region").cast(StringType()).alias("region"),
                col("timestamp").cast(StringType()).alias("timestamp"),
                to_date(col("timestamp")).alias("order_date"),
                col("amount_bucket").cast(StringType()).alias("amount_bucket"),
            )
            .dropna(subset=["order_id", "customer_id", "product_id", "timestamp"])
            .dropDuplicates(["order_id"])
        )

        row_count = fact_orders_df.count()
        if row_count == 0:
            raise ValueError("Spark transformation produced 0 rows.")

        fact_orders_df.write.mode("overwrite").parquet(str(OUTPUT_PATH))

        print(f"Wrote {row_count} rows to: {OUTPUT_PATH}")
        return 0

    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())