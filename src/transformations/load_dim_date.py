import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, min, max, sequence, explode,
    lit, expr, date_format, dayofmonth,
    dayofweek, weekofyear, month, quarter, year
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SILVER_DIR = Path.home() / "commerceflow-data" / "silver_spark"

load_dotenv(PROJECT_ROOT / ".env")

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

POSTGRES_URL = "jdbc:postgresql://localhost:5432/commerceflow_spark_test"
JDBC_JAR = "/usr/share/java/postgresql-42.7.10.jar"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("CommerceFlow-Dim-Date")
        .master("local[*]")
        .config("spark.jars", JDBC_JAR)
        .getOrCreate()
    )


def load_dim_date(spark):

    orders = spark.read.parquet(
        str(SILVER_DIR / "olist_orders_dataset")
    )

    purchase_dates = orders.select(
        to_date(col("order_purchase_timestamp")).alias("purchase_date")
    )

    date_bounds = purchase_dates.agg(
        min("purchase_date").alias("min_date"),
        max("purchase_date").alias("max_date")
    ).first()

    min_date = date_bounds["min_date"]
    max_date = date_bounds["max_date"]

    print(f"Date range: {min_date} to {max_date}")

    dates = spark.range(1).select(
        explode(
            sequence(
                lit(min_date),
                lit(max_date),
                expr("INTERVAL 1 DAY")
            )
        ).alias("full_date")
    )

    dim_date = dates.select(
        date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
        col("full_date"),
        dayofmonth("full_date").alias("day_of_month"),
        date_format("full_date", "EEEE").alias("day_name"),
        dayofweek("full_date").alias("day_of_week"),
        weekofyear("full_date").alias("week_of_year"),
        month("full_date").alias("month_number"),
        date_format("full_date", "MMMM").alias("month_name"),
        quarter("full_date").alias("quarter_number"),
        year("full_date").alias("year_number"),
        dayofweek("full_date").isin(1, 7).alias("is_weekend")
    )

    dim_date.show(5, truncate=False)

    (
        dim_date.write
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", "gold.dim_date")
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )

    print("SUCCESS: Loaded gold.dim_date")


if __name__ == "__main__":

    if not all([POSTGRES_USER, POSTGRES_PASSWORD]):
        raise ValueError("Missing PostgreSQL credentials in .env")

    spark = create_spark_session()

    try:
        load_dim_date(spark)
    finally:
        spark.stop()
