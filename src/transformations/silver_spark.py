from pathlib import Path
import os
import sys

from pyspark.sql import SparkSession


PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
SILVER_DIR = Path.home() / "commerceflow-data" / "silver_spark"


def create_spark_session():

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    return (
        SparkSession.builder
        .appName("CommerceFlow-Silver")
        .master("local[*]")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )


def transform_to_silver(spark):

    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    parquet_files = sorted(BRONZE_DIR.glob("*.parquet"))

    if not parquet_files:
        print("No Bronze datasets found.")
        return

    print(f"Found {len(parquet_files)} Bronze datasets")
    print("=" * 60)

    for file in parquet_files:

        try:
            df = spark.read.parquet(str(file))

            original_rows = df.count()

            # Remove exact duplicate records
            df = df.dropDuplicates()

            # Standardize column names
            for column in df.columns:
                standardized_name = (
                    column.strip()
                    .lower()
                    .replace(" ", "_")
                )

                if column != standardized_name:
                    df = df.withColumnRenamed(
                        column, standardized_name
                    )

            transformed_rows = df.count()

            output_path = SILVER_DIR / file.stem

            df.write.mode("overwrite").parquet(str(output_path))

            print(f"SUCCESS: {file.name}")
            print(f"Original rows: {original_rows}")
            print(f"Silver rows: {transformed_rows}")
            print(f"Duplicates removed: {original_rows - transformed_rows}")
            print("-" * 60)

        except Exception as error:
            print(f"FAILED: {file.name} - {error}")


if __name__ == "__main__":

    spark = create_spark_session()

    try:
        transform_to_silver(spark)
    finally:
        spark.stop()
