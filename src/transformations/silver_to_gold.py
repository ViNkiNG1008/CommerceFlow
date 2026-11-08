import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


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
        .appName("CommerceFlow-Silver-to-Gold")
        .master("local[*]")
        .config("spark.jars", JDBC_JAR)
        .getOrCreate()
    )


def write_to_postgres(df, table_name):
    (
        df.write
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", table_name)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )

    print(f"SUCCESS: Loaded {table_name}")


def load_dimensions(spark):

    # Customers
    customers = spark.read.parquet(
        str(SILVER_DIR / "olist_customers_dataset")
    )

    customers = customers.select(
        "customer_id",
        "customer_unique_id",
        col("customer_zip_code_prefix").cast("int").alias("customer_zip_code_prefix"),
        "customer_city",
        "customer_state"
    )

    write_to_postgres(customers, "gold.dim_customers")

    # Products
    products = spark.read.parquet(
        str(SILVER_DIR / "olist_products_dataset")
    )

    products = products.select(
        "product_id",
        "product_category_name",
        col("product_name_lenght").cast("int").alias("product_name_length"),
        col("product_description_lenght").cast("int").alias("product_description_length"),
        col("product_photos_qty").cast("int").alias("product_photos_qty"),
        col("product_weight_g").cast("int").alias("product_weight_g"),
        col("product_length_cm").cast("int").alias("product_length_cm"),
        col("product_height_cm").cast("int").alias("product_height_cm"),
        col("product_width_cm").cast("int").alias("product_width_cm")
    )

    translation = spark.read.parquet(
        str(SILVER_DIR / "product_category_name_translation")
    )

    products = products.join(
        translation.select(
            "product_category_name",
            "product_category_name_english"
        ),
        on="product_category_name",
        how="left"
    )

    write_to_postgres(products, "gold.dim_products")

    # Sellers
    sellers = spark.read.parquet(
        str(SILVER_DIR / "olist_sellers_dataset")
    )

    sellers = sellers.select(
        "seller_id",
        col("seller_zip_code_prefix").cast("int").alias("seller_zip_code_prefix"),
        "seller_city",
        "seller_state"
    )

    write_to_postgres(sellers, "gold.dim_sellers")


if __name__ == "__main__":

    if not all([POSTGRES_USER, POSTGRES_PASSWORD]):
        raise ValueError("Missing PostgreSQL credentials in .env")

    spark = create_spark_session()

    try:
        load_dimensions(spark)
    finally:
        spark.stop()
