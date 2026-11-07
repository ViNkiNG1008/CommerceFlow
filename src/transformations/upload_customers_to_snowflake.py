
from pathlib import Path
import os

import snowflake.connector
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

SILVER_PATH = (
    Path.home()
    / "commerceflow-data"
    / "silver_spark"
    / "olist_customers_dataset"
)


def upload_customers():
    connection = None

    try:
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

        connection = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            authenticator="SNOWFLAKE_JWT",
            private_key=private_key_der,
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            role=os.getenv("SNOWFLAKE_ROLE"),
        )

        print("Connected to Snowflake.")

        with connection.cursor() as cursor:
            upload_command = (
                f"PUT 'file://{SILVER_PATH}/*.parquet' "
                "@COMMERCEFLOW_DB.SILVER.COMMERCEFLOW_STAGE/olist_customers "
                "AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            )

            print("Uploading customer Parquet files...")
            cursor.execute(upload_command)

            for row in cursor.fetchall():
                print(row)

            cursor.execute(
                "LIST @COMMERCEFLOW_DB.SILVER.COMMERCEFLOW_STAGE/olist_customers"
            )

            files = cursor.fetchall()

            print(f"\nFiles available in Snowflake stage: {len(files)}")

            for file in files:
                print(file[0])

    except Exception as error:
        print(f"Upload failed: {error}")

    finally:
        if connection:
            connection.close()
            print("\nSnowflake connection closed.")


if __name__ == "__main__":
    upload_customers()
