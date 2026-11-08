
import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, broadcast
from pyspark.sql.types import DecimalType


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SILVER_DIR = PROJECT_ROOT / "data" / "silver_spark"

load_dotenv(PROJECT_ROOT / ".env")

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

POSTGRES_HOST = os.getenv("COMMERCEFLOW_POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("COMMERCEFLOW_POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv(
    "COMMERCEFLOW_POSTGRES_DB",
    "commerceflow_spark_test"
)

POSTGRES_URL = (
    f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)
JDBC_JAR = "/usr/share/java/postgresql-42.7.10.jar"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("CommerceFlow-Fact-Order-Items")
        .master("local[*]")
        .config("spark.jars", JDBC_JAR)
        .getOrCreate()
    )


def read_gold_table(spark, table_name):
    return (
        spark.read
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", table_name)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def load_fact_order_items(spark):

    # Read Silver order items
    items = spark.read.parquet(
        str(SILVER_DIR / "olist_order_items_dataset")
    )

    # Read customer keys through the already-loaded order fact
    orders = read_gold_table(
        spark, "gold.fact_orders"
    ).select("order_id", "customer_key")

    # Read product dimension keys
    products = read_gold_table(
        spark, "gold.dim_products"
    ).select("product_id", "product_key")

    # Read seller dimension keys
    sellers = read_gold_table(
        spark, "gold.dim_sellers"
    ).select("seller_id", "seller_key")

    # Join items with order/customer keys
    items = items.join(
        broadcast(orders),
        on="order_id",
        how="left"
    )

    # Join product keys
    items = items.join(
        broadcast(products),
        on="product_id",
        how="left"
    )

    # Join seller keys
    items = items.join(
        broadcast(sellers),
        on="seller_id",
        how="left"
    )

    # Transform to target fact schema
    fact_items = items.select(
        "order_id",
        col("order_item_id").cast("int").alias("order_item_id"),
        "customer_key",
        "product_key",
        "seller_key",
        to_timestamp(col("shipping_limit_date")).alias(
            "shipping_limit_date"
        ),
        col("price").cast(DecimalType(12, 2)).alias("price"),
        col("freight_value").cast(DecimalType(12, 2)).alias(
            "freight_value"
        )
    )

    # Validate row count and dimension lookups
    total_items = fact_items.count()

    missing_customers = fact_items.filter(
        col("customer_key").isNull()
    ).count()

    missing_products = fact_items.filter(
        col("product_key").isNull()
    ).count()

    missing_sellers = fact_items.filter(
        col("seller_key").isNull()
    ).count()

    duplicate_items = (
        fact_items.groupBy("order_id", "order_item_id")
        .count()
        .filter(col("count") > 1)
        .count()
    )

    print(f"Total order items: {total_items}")
    print(f"Missing customer keys: {missing_customers}")
    print(f"Missing product keys: {missing_products}")
    print(f"Missing seller keys: {missing_sellers}")
    print(f"Duplicate order items: {duplicate_items}")

    if (
        missing_customers > 0
        or missing_products > 0
        or missing_sellers > 0
        or duplicate_items > 0
    ):
        raise ValueError(
            "Validation failed. Fact table was not loaded."
        )

    # Write to PostgreSQL
    (
        fact_items.write
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", "gold.fact_order_items")
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", "1000")
        .mode("append")
        .save()
    )

    print("SUCCESS: Loaded gold.fact_order_items")


if __name__ == "__main__":

    if not all([POSTGRES_USER, POSTGRES_PASSWORD]):
        raise ValueError("Missing PostgreSQL credentials in .env")

    spark = create_spark_session()

    try:
        load_fact_order_items(spark)
    finally:
        spark.stop()
