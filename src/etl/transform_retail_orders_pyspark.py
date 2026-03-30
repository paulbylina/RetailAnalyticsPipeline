from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lower,
    trim,
    when,
    to_timestamp,
)

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_FILE = str(PROJECT_ROOT / "data" / "raw" / "retail_orders.jsonl")
OUTPUT_DIR = str(PROJECT_ROOT / "data" / "processed" / "retail_orders_clean_parquet")


def main():
    spark = (
        SparkSession.builder
        .appName("transform_retail_orders_pyspark")
        .getOrCreate()
    )

    df = spark.read.json(RAW_FILE)

    cleaned_df = (
        df.select(
            col("order_id").cast("int").alias("order_id"),
            col("customer_id").cast("int").alias("customer_id"),
            col("product_id").cast("int").alias("product_id"),
            lower(trim(col("category"))).alias("category"),
            col("quantity").cast("int").alias("quantity"),
            col("unit_price").cast("double").alias("unit_price"),
            col("total_amount").cast("double").alias("total_amount"),
            lower(trim(col("payment_method"))).alias("payment_method"),
            lower(trim(col("order_status"))).alias("order_status"),
            lower(trim(col("region"))).alias("region"),
            to_timestamp(col("timestamp")).alias("order_timestamp"),
        )
        .withColumn(
            "amount_bucket",
            when(col("total_amount") < 50, "low")
            .when(col("total_amount") < 200, "medium")
            .otherwise("high")
        )
    )

    cleaned_df.write.mode("overwrite").parquet(OUTPUT_DIR)

    print(f"PySpark cleaned data written to {OUTPUT_DIR}")

    spark.stop()


if __name__ == "__main__":
    main()