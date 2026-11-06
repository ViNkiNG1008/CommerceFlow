
from pathlib import Path
import pandas as pd


# Project directories
PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
SILVER_DIR = PROJECT_ROOT / "data" / "silver"


def transform_to_silver():

    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    parquet_files = list(BRONZE_DIR.glob("*.parquet"))

    if not parquet_files:
        print("No Bronze files found.")
        return

    print(f"Found {len(parquet_files)} Bronze datasets")
    print("=" * 60)

    for file in parquet_files:

        try:
            # Read Bronze dataset
            df = pd.read_parquet(file)

            original_rows = len(df)

            # Remove exact duplicate rows
            df = df.drop_duplicates()

            # Standardize column names
            df.columns = (
                df.columns
                .str.strip()
                .str.lower()
                .str.replace(" ", "_")
            )

            # Save transformed dataset
            output_file = SILVER_DIR / file.name

            df.to_parquet(
                output_file,
                engine="pyarrow",
                index=False
            )

            print(f"SUCCESS: {file.name}")
            print(f"Original rows: {original_rows}")
            print(f"Rows after deduplication: {len(df)}")
            print(f"Duplicates removed: {original_rows - len(df)}")
            print("-" * 60)

        except Exception as error:
            print(f"FAILED: {file.name} - {error}")


if __name__ == "__main__":
    transform_to_silver()
