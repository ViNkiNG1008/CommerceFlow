
from pathlib import Path
from pyspark.sql import SparkSession

# Project-relative path (works in both WSL and Airflow Docker)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
SILVER_DIR = PROJECT_ROOT / "data" / "silver_spark"


def validate_silver_spark():
    spark = (
        SparkSession.builder
        .appName("CommerceFlow-Silver-Validation")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        print("\nSILVER LAYER VALIDATION")
        print("=" * 65)

        if not SILVER_DIR.exists():
            raise FileNotFoundError(
                f"Silver directory not found: {SILVER_DIR}"
            )

        datasets = sorted(
            dataset for dataset in SILVER_DIR.iterdir()
            if dataset.is_dir()
        )

        if not datasets:
            raise RuntimeError("No Silver datasets found!")

        for dataset in datasets:
            df = spark.read.parquet(str(dataset))

            print(f"\nDataset: {dataset.name}")
            print(f"Rows: {df.count():,}")
            print(f"Columns ({len(df.columns)}): {df.columns}")

        print("\n" + "=" * 65)
        print("VALIDATION COMPLETED")

    finally:
        spark.stop()


if __name__ == "__main__":
    validate_silver_spark()
