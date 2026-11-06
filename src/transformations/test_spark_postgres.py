import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

if not all([POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD]):
    raise ValueError("Missing PostgreSQL credentials in .env")

JDBC_JAR = "/usr/share/java/postgresql-42.7.10.jar"

spark = (
    SparkSession.builder
    .appName("CommerceFlow-Postgres-Connection")
    .master("local[*]")
    .config("spark.jars", JDBC_JAR)
    .getOrCreate()
)

try:
    df = (
        spark.read
        .format("jdbc")
        .option("url", f"jdbc:postgresql://localhost:5432/{POSTGRES_DB}")
        .option(
            "dbtable",
            "(SELECT current_database() AS database_name, version() AS postgres_version) AS test"
        )
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    df.show(truncate=False)
    print("SUCCESS: Spark connected to PostgreSQL!")

finally:
    spark.stop()
