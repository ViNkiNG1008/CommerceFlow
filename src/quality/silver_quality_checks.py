
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# Project directories
PROJECT_ROOT = Path(__file__).resolve().parents[2]
SILVER_DIR = PROJECT_ROOT / "data" / "silver_spark"


# Business keys for each dataset
DATASET_KEYS = {
    "olist_customers_dataset": ["customer_id"],
    "olist_orders_dataset": ["order_id"],
    "olist_products_dataset": ["product_id"],
    "olist_sellers_dataset": ["seller_id"],
    "olist_order_items_dataset": ["order_id", "order_item_id"],
    "olist_order_payments_dataset": ["order_id", "payment_sequential"],
    "olist_order_reviews_dataset": ["review_id", "order_id"],
    "product_category_name_translation": ["product_category_name"],
}


def run_silver_quality_checks():

    spark = (
        SparkSession.builder
        .appName("CommerceFlow-Silver-Quality")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        total_checks = 0
        failed_checks = 0

        print("\nCOMMERCEFLOW - SILVER DATA QUALITY")
        print("=" * 65)
        print(f"Silver directory: {SILVER_DIR}")

        for dataset_name, key_columns in DATASET_KEYS.items():

            dataset_path = SILVER_DIR / dataset_name

            if not dataset_path.exists():
                raise FileNotFoundError(
                    f"Silver dataset not found: {dataset_path}"
                )

            df = spark.read.parquet(str(dataset_path))

            print(f"\nDataset: {dataset_name}")
            print("-" * 65)

            # Check 1: Null values in key columns
            for column in key_columns:

                null_count = df.filter(
                    col(column).isNull()
                ).count()

                total_checks += 1

                if null_count == 0:
                    print(f"PASS | {column}: No NULL values")
                else:
                    print(f"FAIL | {column}: {null_count} NULL values")
                    failed_checks += 1

            # Check 2: Uniqueness of business key
            duplicate_count = (
                df.groupBy(*key_columns)
                .count()
                .filter(col("count") > 1)
                .count()
            )

            total_checks += 1

            if duplicate_count == 0:
                print(f"PASS | Unique key: {key_columns}")
            else:
                print(
                    f"FAIL | {duplicate_count} duplicate key groups: "
                    f"{key_columns}"
                )
                failed_checks += 1

        print("\n" + "=" * 65)
        print(f"Total checks: {total_checks}")
        print(f"Passed: {total_checks - failed_checks}")
        print(f"Failed: {failed_checks}")

        if failed_checks > 0:
            raise RuntimeError(
                f"Silver quality validation failed: "
                f"{failed_checks} checks failed."
            )

        print("\nRESULT: ALL QUALITY CHECKS PASSED")

    finally:
        spark.stop()


if __name__ == "__main__":
    run_silver_quality_checks()
