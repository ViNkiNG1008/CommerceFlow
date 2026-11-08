
import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, to_timestamp, broadcast


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
        .appName("CommerceFlow-Fact-Orders")
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


def load_fact_orders(spark):

    # Read Silver orders
    orders = spark.read.parquet(
        str(SILVER_DIR / "olist_orders_dataset")
    )

    # Read dimension lookup tables from PostgreSQL
    customers = read_gold_table(
        spark, "gold.dim_customers"
    ).select("customer_id", "customer_key")

    dates = read_gold_table(
        spark, "gold.dim_date"
    ).select("full_date", "date_key")

    # Join orders with customer dimension
    orders = orders.join(
        broadcast(customers),
        on="customer_id",
        how="left"
    )

    # Convert purchase timestamp and derive purchase date
    orders = orders.withColumn(
        "order_purchase_timestamp",
        to_timestamp(col("order_purchase_timestamp"))
    )

    orders = orders.withColumn(
        "purchase_date",
        to_date(col("order_purchase_timestamp"))
    )

    # Join with date dimension
    orders = orders.join(
        broadcast(dates),
        orders.purchase_date == dates.full_date,
        how="left"
    )

    # Select target fact table columns
    fact_orders = orders.select(
        "order_id",
        "customer_key",
        col("date_key").alias("purchase_date_key"),
        "order_status",
        "order_purchase_timestamp",
        to_timestamp(col("order_approved_at")).alias("order_approved_at"),
        to_timestamp(col("order_delivered_carrier_date")).alias(
            "order_delivered_carrier_date"
        ),
        to_timestamp(col("order_delivered_customer_date")).alias(
            "order_delivered_customer_date"
        ),
        to_timestamp(col("order_estimated_delivery_date")).alias(
            "order_estimated_delivery_date"
        )
    )

    # Validate before writing
    total_orders = fact_orders.count()

    missing_customers = fact_orders.filter(
        col("customer_key").isNull()
    ).count()

    missing_dates = fact_orders.filter(
        col("purchase_date_key").isNull()
    ).count()

    print(f"Total orders: {total_orders}")
    print(f"Missing customer keys: {missing_customers}")
    print(f"Missing purchase date keys: {missing_dates}")

    if missing_customers > 0 or missing_dates > 0:
        raise ValueError(
            "Dimension lookup failed. Fact table was not loaded."
        )

    # Write to PostgreSQL
    (
        fact_orders.write
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", "gold.fact_orders")
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", "1000")
        .mode("append")
        .save()
    )

    print("SUCCESS: Loaded gold.fact_orders")


if __name__ == "__main__":

    if not all([POSTGRES_USER, POSTGRES_PASSWORD]):
        raise ValueError("Missing PostgreSQL credentials in .env")

    spark = create_spark_session()

    try:
        load_fact_orders(spark)
    finally:
        spark.stop()
