
from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"


def validate_bronze():

    parquet_files = list(BRONZE_DIR.glob("*.parquet"))

    print(f"Found {len(parquet_files)} Parquet files")
    print("=" * 60)

    for file in parquet_files:
        df = pd.read_parquet(file)

        has_metadata = (
            "_source_file" in df.columns
            and "_ingested_at" in df.columns
        )

        print(f"Dataset: {file.name}")
        print(f"Rows: {len(df)}")
        print(f"Columns: {len(df.columns)}")
        print(f"Metadata present: {has_metadata}")
        print(f"File size: {file.stat().st_size / (1024 * 1024):.2f} MB")
        print("-" * 60)


if __name__ == "__main__":
    validate_bronze()
