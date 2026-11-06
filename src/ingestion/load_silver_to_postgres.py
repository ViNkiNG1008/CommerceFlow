
from pathlib import Path
import os

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from dotenv import load_dotenv


# Project directories
PROJECT_ROOT = Path(__file__).resolve().parents[2]
SILVER_DIR = PROJECT_ROOT / "data" / "silver"
ENV_FILE = PROJECT_ROOT / ".env"

load_dotenv(ENV_FILE)


def get_database_engine():

    database_url = URL.create(
        drivername="postgresql+psycopg",
        username=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host="localhost",
        port=5432,
        database=os.getenv("POSTGRES_DB"),
    )

    return create_engine(database_url)


def load_silver_to_postgres():

    engine = get_database_engine()

    parquet_files = list(SILVER_DIR.glob("*.parquet"))

    if not parquet_files:
        print("No Silver Parquet files found.")
        return

    print(f"Found {len(parquet_files)} Silver datasets")
    print("=" * 60)

    for file in parquet_files:

        try:
            # Read Silver Parquet
            df = pd.read_parquet(file)

            # Generate PostgreSQL table name
            table_name = file.stem.replace("_dataset", "")

            # Load into PostgreSQL
            df.to_sql(
    name=table_name,
    con=engine,
    schema="public",
    if_exists="replace",
    index=False,
    chunksize=5000,
    method=None
)

            print(f"SUCCESS: {table_name}")
            print(f"Rows loaded: {len(df)}")
            print("-" * 60)

        except Exception as error:
            print(f"FAILED: {file.name} - {error}")

    engine.dispose()
    print("PostgreSQL loading completed.")


if __name__ == "__main__":
    load_silver_to_postgres()
