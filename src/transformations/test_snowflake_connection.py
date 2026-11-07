
from pathlib import Path
import os

import snowflake.connector
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")


def test_snowflake_connection():
    connection = None

    try:
        print("Loading RSA private key...")

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

        print("Connecting to Snowflake using RSA authentication...")

        connection = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            authenticator=os.getenv("SNOWFLAKE_AUTHENTICATOR"),
            private_key=private_key_der,
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            role=os.getenv("SNOWFLAKE_ROLE"),
        )

        print("\nConnection successful!")

        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT
                    CURRENT_USER(),
                    CURRENT_ACCOUNT(),
                    CURRENT_DATABASE(),
                    CURRENT_SCHEMA(),
                    CURRENT_WAREHOUSE(),
                    CURRENT_ROLE()
            """)

            result = cursor.fetchone()

            print("\nSnowflake Connection Details")
            print("-" * 40)
            print(f"User:      {result[0]}")
            print(f"Account:   {result[1]}")
            print(f"Database:  {result[2]}")
            print(f"Schema:    {result[3]}")
            print(f"Warehouse: {result[4]}")
            print(f"Role:      {result[5]}")

    except Exception as error:
        print(f"\nConnection failed: {error}")

    finally:
        if connection:
            connection.close()
            print("\nConnection closed.")


if __name__ == "__main__":
    test_snowflake_connection()
