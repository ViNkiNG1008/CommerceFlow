
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd


# Project directories
PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"


def ingest_csv_to_bronze():
    """Convert all raw CSV files into Bronze Parquet files."""

    # Create Bronze directory if it doesn't exist
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    # Find all CSV files
    csv_files = list(RAW_DIR.glob("*.csv"))

    if not csv_files:
        print(f"No CSV files found in {RAW_DIR}")
        return

    print(f"Found {len(csv_files)} CSV files.")
    print("-" * 50)

    ingestion_time = datetime.now(timezone.utc).isoformat()

    for csv_file in csv_files:
        try:
            # Read raw CSV
            df = pd.read_csv(csv_file)

            # Add ingestion metadata
            df["_source_file"] = csv_file.name
            df["_ingested_at"] = ingestion_time

            # Create corresponding Parquet filename
            parquet_file = BRONZE_DIR / f"{csv_file.stem}.parquet"

            # Write Parquet
            df.to_parquet(
                parquet_file,
                engine="pyarrow",
                index=False
            )

            print(
                f"SUCCESS: {csv_file.name} "
                f"-> {parquet_file.name} "
                f"({len(df)} rows)"
            )

        except Exception as error:
            print(f"FAILED: {csv_file.name} - {error}")

    print("-" * 50)
    print("Bronze ingestion completed.")


if __name__ == "__main__":
    ingest_csv_to_bronze()
