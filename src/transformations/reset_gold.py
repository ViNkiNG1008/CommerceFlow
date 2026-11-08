
import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

# Local defaults; Airflow overrides these through environment variables.
POSTGRES_HOST = os.getenv("COMMERCEFLOW_POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("COMMERCEFLOW_POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv(
    "COMMERCEFLOW_POSTGRES_DB",
    "commerceflow_spark_test"
)

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")


def reset_gold():

    # Safety check: prevent accidental deletion.
    if os.getenv("CONFIRM_GOLD_RESET") != "YES":
        raise ValueError(
            "Gold reset blocked. Set CONFIRM_GOLD_RESET=YES to proceed."
        )

    if not POSTGRES_USER or not POSTGRES_PASSWORD:
        raise ValueError("PostgreSQL credentials are missing.")

    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    TRUNCATE TABLE
                        gold.fact_order_items,
                        gold.fact_payments,
                        gold.fact_orders,
                        gold.dim_customers,
                        gold.dim_products,
                        gold.dim_sellers,
                        gold.dim_date
                    RESTART IDENTITY;
                """)

        print("SUCCESS: Gold tables reset.")
        print(f"Database: {POSTGRES_DB}")
        print(f"Host: {POSTGRES_HOST}")

    finally:
        conn.close()


if __name__ == "__main__":
    reset_gold()
