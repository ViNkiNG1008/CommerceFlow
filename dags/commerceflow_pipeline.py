
from datetime import datetime

from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator


PROJECT_DIR = "/opt/airflow"
JAVA_ENV = {
    "JAVA_HOME": "/usr/lib/jvm/java-17-openjdk-amd64"
}


@dag(
    dag_id="commerceflow_pipeline",
    description="End-to-end CommerceFlow data engineering pipeline",
    start_date=datetime(2026, 11, 8),
    schedule=None,
    catchup=False,
    tags=["commerceflow", "data-engineering", "etl"],
)
def commerceflow_pipeline():

    # ---------------- BRONZE ----------------

    bronze_ingestion = BashOperator(
        task_id="bronze_ingestion",
        bash_command=f"python {PROJECT_DIR}/src/ingestion/bronze_ingestion.py",
    )

    validate_bronze = BashOperator(
        task_id="validate_bronze",
        bash_command=f"python {PROJECT_DIR}/src/quality/validate_bronze.py",
    )

    # ---------------- SILVER ----------------

    silver_pandas = BashOperator(
        task_id="silver_pandas_transformation",
        bash_command=f"python {PROJECT_DIR}/src/transformations/silver_transformation.py",
    )

    validate_silver = BashOperator(
        task_id="validate_silver",
        bash_command=f"python {PROJECT_DIR}/src/quality/validate_silver.py",
    )

    silver_spark = BashOperator(
        task_id="silver_spark_transformation",
        bash_command=f"python {PROJECT_DIR}/src/transformations/silver_spark.py",
        env=JAVA_ENV,
        append_env=True,
    )

    validate_silver_spark = BashOperator(
        task_id="validate_silver_spark",
        bash_command=f"python {PROJECT_DIR}/src/quality/validate_silver_spark.py",
        env=JAVA_ENV,
        append_env=True,
    )

    silver_quality_checks = BashOperator(
        task_id="silver_quality_checks",
        bash_command=f"python {PROJECT_DIR}/src/quality/silver_quality_checks.py",
        env=JAVA_ENV,
        append_env=True,
    )

    # ---------------- GOLD: RESET ----------------

    reset_gold = BashOperator(
        task_id="reset_gold",
        bash_command=f"python {PROJECT_DIR}/src/transformations/reset_gold.py",
        env={"CONFIRM_GOLD_RESET": "YES"},
        append_env=True,
        retries=0,
    )

    # ---------------- GOLD: DIMENSIONS ----------------

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=f"python {PROJECT_DIR}/src/transformations/silver_to_gold.py",
        env=JAVA_ENV,
        append_env=True,
        retries=0,
    )

    load_dim_date = BashOperator(
        task_id="load_dim_date",
        bash_command=f"python {PROJECT_DIR}/src/transformations/load_dim_date.py",
        env=JAVA_ENV,
        append_env=True,
        retries=0,
    )

    # ---------------- GOLD: FACT TABLES ----------------

    load_fact_orders = BashOperator(
        task_id="load_fact_orders",
        bash_command=f"python {PROJECT_DIR}/src/transformations/load_fact_orders.py",
        env=JAVA_ENV,
        append_env=True,
        retries=0,
    )

    load_fact_order_items = BashOperator(
        task_id="load_fact_order_items",
        bash_command=f"python {PROJECT_DIR}/src/transformations/load_fact_order_items.py",
        env=JAVA_ENV,
        append_env=True,
        retries=0,
    )

    load_fact_payments = BashOperator(
        task_id="load_fact_payments",
        bash_command=f"python {PROJECT_DIR}/src/transformations/load_fact_payments.py",
        env=JAVA_ENV,
        append_env=True,
        retries=0,
    )

    # ---------------- DEPENDENCIES ----------------

    (
        bronze_ingestion
        >> validate_bronze
        >> silver_pandas
        >> validate_silver
        >> silver_spark
        >> validate_silver_spark
        >> silver_quality_checks
        >> reset_gold
        >> silver_to_gold
        >> load_dim_date
        >> load_fact_orders
    )

    # These two fact loaders run in parallel.
    load_fact_orders >> [load_fact_order_items, load_fact_payments]


commerceflow_pipeline()
