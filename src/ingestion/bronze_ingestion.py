
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd


# Project directories
PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"


def ingest_csv_to_bronze():
    """Convert all raw CSV files into Bronze Parquet files."""

    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    csv_files = sorted(RAW_DIR.glob("*.csv"))

    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {RAW_DIR}")

    print(f"Found {len(csv_files)} CSV files.")
    print("-" * 50)

    ingestion_time = datetime.now(timezone.utc).isoformat()

    successful_files = 0
    failed_files = []

    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file)

            # Add ingestion metadata
            df["_source_file"] = csv_file.name
            df["_ingested_at"] = ingestion_time

            parquet_file = BRONZE_DIR / f"{csv_file.stem}.parquet"

            df.to_parquet(
                parquet_file,
                engine="pyarrow",
                index=False
            )

            successful_files += 1

            print(
                f"SUCCESS: {csv_file.name} "
                f"-> {parquet_file.name} "
                f"({len(df)} rows)"
            )

        except Exception as error:
            failed_files.append(csv_file.name)
            print(f"FAILED: {csv_file.name} - {error}")

    print("-" * 50)
    print(f"Successful files: {successful_files}")
    print(f"Failed files: {len(failed_files)}")

    if failed_files:
        raise RuntimeError(
            f"Bronze ingestion failed for: {', '.join(failed_files)}"
        )

    print("Bronze ingestion completed successfully.")


if __name__ == "__main__":
    ingest_csv_to_bronze()
