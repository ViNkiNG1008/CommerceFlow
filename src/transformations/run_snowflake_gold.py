
from pathlib import Path
import os

import snowflake.connector
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv


# -------------------------------------------------
# Project Configuration
# -------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

SQL_DIRECTORY = PROJECT_ROOT / "sql" / "snowflake" / "gold"

SQL_FILES = [
    "01_create_dimensions.sql",
    "02_create_facts.sql",
    "03_load_dimensions.sql",
    "04_load_facts.sql",
    "05_create_views.sql",
    "06_validate_gold.sql",
]


# -------------------------------------------------
# Load Environment Variables
# -------------------------------------------------

load_dotenv(PROJECT_ROOT / ".env")


# -------------------------------------------------
# Snowflake Connection
# -------------------------------------------------

def create_snowflake_connection():

    print("Loading RSA private key...")

    private_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")

    if not private_key_path:
        raise ValueError("SNOWFLAKE_PRIVATE_KEY_PATH is missing.")

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

    print("Connecting to Snowflake...")

    connection = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        authenticator=os.getenv("SNOWFLAKE_AUTHENTICATOR"),
        private_key=private_key_der,
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema="GOLD",
        role=os.getenv("SNOWFLAKE_ROLE"),
    )

    print("Snowflake connection successful!")

    return connection


# -------------------------------------------------
# Execute SQL Files
# -------------------------------------------------

def execute_sql_file(connection, filename):

    file_path = SQL_DIRECTORY / filename

    if not file_path.exists():
        raise FileNotFoundError(f"SQL file not found: {file_path}")

    print("\n" + "=" * 60)
    print(f"Executing: {filename}")
    print("=" * 60)

    sql_script = file_path.read_text(encoding="utf-8")

    cursors = connection.execute_string(sql_script)

    for cursor in cursors:

        try:
            if cursor.description:
                rows = cursor.fetchall()

                for row in rows:
                    print(row)

            else:
                print(f"Statement executed successfully.")

        finally:
            cursor.close()

    print(f"Completed: {filename}")


# -------------------------------------------------
# Main Pipeline
# -------------------------------------------------

def run_gold_pipeline():

    connection = None

    try:
        connection = create_snowflake_connection()

        for filename in SQL_FILES:
            execute_sql_file(connection, filename)

        print("\n" + "=" * 60)
        print("COMMERCEFLOW SNOWFLAKE GOLD PIPELINE COMPLETED")
        print("=" * 60)

    except Exception as error:
        print(f"\nPipeline failed: {error}")
        raise

    finally:
        if connection:
            connection.close()
            print("\nSnowflake connection closed.")


if __name__ == "__main__":
    run_gold_pipeline()
