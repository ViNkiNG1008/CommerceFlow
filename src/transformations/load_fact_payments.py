
import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast
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
        .appName("CommerceFlow-Fact-Payments")
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


def load_fact_payments(spark):

    # Read Silver payments
    payments = spark.read.parquet(
        str(SILVER_DIR / "olist_order_payments_dataset")
    )

    # Read customer keys through Gold fact_orders
    orders = read_gold_table(
        spark, "gold.fact_orders"
    ).select("order_id", "customer_key")

    # Join payments with customer keys
    payments = payments.join(
        broadcast(orders),
        on="order_id",
        how="left"
    )

    # Select target fact columns
    fact_payments = payments.select(
        "order_id",
        "customer_key",
        col("payment_sequential").cast("int").alias(
            "payment_sequential"
        ),
        "payment_type",
        col("payment_installments").cast("int").alias(
            "payment_installments"
        ),
        col("payment_value").cast(DecimalType(12, 2)).alias(
            "payment_value"
        )
    )

    # Validate before writing
    total_payments = fact_payments.count()

    missing_customers = fact_payments.filter(
        col("customer_key").isNull()
    ).count()

    duplicate_payments = (
        fact_payments
        .groupBy("order_id", "payment_sequential")
        .count()
        .filter(col("count") > 1)
        .count()
    )

    print(f"Total payments: {total_payments}")
    print(f"Missing customer keys: {missing_customers}")
    print(f"Duplicate payments: {duplicate_payments}")

    if missing_customers > 0 or duplicate_payments > 0:
        raise ValueError(
            "Validation failed. Fact table was not loaded."
        )

    # Write to PostgreSQL
    (
        fact_payments.write
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", "gold.fact_payments")
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", "1000")
        .mode("append")
        .save()
    )

    print("SUCCESS: Loaded gold.fact_payments")


if __name__ == "__main__":

    if not all([POSTGRES_USER, POSTGRES_PASSWORD]):
        raise ValueError("Missing PostgreSQL credentials in .env")

    spark = create_spark_session()

    try:
        load_fact_payments(spark)
    finally:
        spark.stop()
