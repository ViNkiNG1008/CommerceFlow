
from pathlib import Path
import os

import snowflake.connector
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

SILVER_ROOT = Path.home() / "commerceflow-data" / "silver_spark"

STAGE = "@COMMERCEFLOW_DB.SILVER.COMMERCEFLOW_STAGE"
FILE_FORMAT = "COMMERCEFLOW_DB.SILVER.PARQUET_FORMAT"

# Customers are excluded because they are already loaded and verified.
DATASETS = {
    "olist_geolocation_dataset": "OLIST_GEOLOCATION",
    "olist_orders_dataset": "OLIST_ORDERS",
    "olist_order_items_dataset": "OLIST_ORDER_ITEMS",
    "olist_order_payments_dataset": "OLIST_ORDER_PAYMENTS",
    "olist_order_reviews_dataset": "OLIST_ORDER_REVIEWS",
    "olist_products_dataset": "OLIST_PRODUCTS",
    "olist_sellers_dataset": "OLIST_SELLERS",
    "product_category_name_translation": "PRODUCT_CATEGORY_NAME_TRANSLATION",
}


def create_connection():
    private_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")

    with open(private_key_path, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
        )

    private_key_der = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        authenticator="SNOWFLAKE_JWT",
        private_key=private_key_der,
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )


def process_dataset(cursor, folder_name, table_name):
    local_path = SILVER_ROOT / folder_name
    stage_path = f"{STAGE}/{table_name.lower()}"

    parquet_files = list(local_path.glob("*.parquet"))

    if not parquet_files:
        print(f"SKIPPED: No Parquet files found in {folder_name}")
        return

    print(f"\n{'=' * 60}")
    print(f"Processing: {folder_name}")
    print(f"Target table: {table_name}")
    print(f"Parquet files: {len(parquet_files)}")

    # Upload Parquet files to Snowflake internal stage.
    put_sql = (
        f"PUT 'file://{local_path}/*.parquet' "
        f"{stage_path} "
        "AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
    )

    cursor.execute(put_sql)
    upload_results = cursor.fetchall()

    uploaded = sum(
        1 for row in upload_results
        if row[6] in ("UPLOADED", "SKIPPED")
    )
    print(f"Upload results received: {len(upload_results)}")

    # Infer the table structure from the staged Parquet files.
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS
        COMMERCEFLOW_DB.SILVER.{table_name}
        USING TEMPLATE (
            SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
            FROM TABLE(
                INFER_SCHEMA(
                    LOCATION => '{stage_path}/',
                    FILE_FORMAT => '{FILE_FORMAT}'
                )
            )
        )
    """

    cursor.execute(create_table_sql)
    print(f"Table ready: {table_name}")

    # Load the staged Parquet files into the table.
    copy_sql = f"""
        COPY INTO COMMERCEFLOW_DB.SILVER.{table_name}
        FROM {stage_path}/
        FILE_FORMAT = (FORMAT_NAME = '{FILE_FORMAT}')
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    """

    cursor.execute(copy_sql)
    load_results = cursor.fetchall()

    total_loaded = sum(
        row[3] for row in load_results
        if row[1] == "LOADED"
    )

    print(f"Rows loaded in this run: {total_loaded}")

    # Verify the final table count.
    cursor.execute(
        f"SELECT COUNT(*) "
        f"FROM COMMERCEFLOW_DB.SILVER.{table_name}"
    )

    total_rows = cursor.fetchone()[0]
    print(f"Total rows in Snowflake: {total_rows}")
    print(f"SUCCESS: {table_name}")


def main():
    connection = None

    try:
        print("Connecting to Snowflake...")
        connection = create_connection()
        print("Connection successful!")

        with connection.cursor() as cursor:
            for folder_name, table_name in DATASETS.items():
                try:
                    process_dataset(cursor, folder_name, table_name)
                except Exception as error:
                    print(f"FAILED: {table_name}: {error}")

        print("\nAll dataset processing attempts completed.")

    except Exception as error:
        print(f"Connection or pipeline error: {error}")

    finally:
        if connection:
            connection.close()
            print("Snowflake connection closed.")


if __name__ == "__main__":
    main()
