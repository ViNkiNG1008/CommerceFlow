from pathlib import Path
from pyspark.sql import SparkSession

SILVER_DIR = Path.home() / "commerceflow-data" / "silver_spark"

spark = (
    SparkSession.builder
    .appName("CommerceFlow-Silver-Validation")
    .master("local[*]")
    .getOrCreate()
)

try:
    print("\nSILVER LAYER VALIDATION")
    print("=" * 65)

    for dataset in sorted(SILVER_DIR.iterdir()):
        if dataset.is_dir():
            df = spark.read.parquet(str(dataset))
            print(f"\nDataset: {dataset.name}")
            print(f"Rows: {df.count():,}")
            print(f"Columns ({len(df.columns)}): {df.columns}")

    print("\n" + "=" * 65)
    print("VALIDATION COMPLETED")

finally:
    spark.stop()
