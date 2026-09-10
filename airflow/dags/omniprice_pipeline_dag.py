from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_HOME = "/home/talentum/omniprice"


default_args = {
    "owner": "omniprice",
    "depends_on_past": False,
    "retries": 1,
}


with DAG(
    dag_id="omniprice_end_to_end_pipeline",
    default_args=default_args,
    description="OmniPrice Big Data Pipeline",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["omniprice", "big-data", "pyspark", "hive"],
) as dag:

    # =========================================================
    # TASK 1
    # =========================================================

    create_hdfs_structure = BashOperator(
        task_id="create_hdfs_structure",

        bash_command=(
            f"cd {PROJECT_HOME} && "
            "./scripts/create_hdfs_structure.sh"
        ),
    )


    # =========================================================
    # TASK 2
    # =========================================================

    upload_raw_data = BashOperator(
        task_id="upload_raw_data",

        bash_command=(
            f"cd {PROJECT_HOME} && "
            "./scripts/upload_raw_to_hdfs.sh"
        ),
    )


    # =========================================================
    # TASK 3
    # =========================================================

    run_batch_etl = BashOperator(
        task_id="run_batch_etl",

        bash_command=(
            f"cd {PROJECT_HOME} && "
            "./scripts/run_batch_etl.sh"
        ),
    )


    # =========================================================
    # TASK 4
    # =========================================================

    dynamic_pricing = BashOperator(
        task_id="dynamic_pricing",

        bash_command=(
            f"cd {PROJECT_HOME} && "
            "spark-submit "
            "spark/decision_engine/"
            "01_dynamic_pricing.py"
        ),
    )


    # =========================================================
    # TASK 5
    # =========================================================

    inventory_optimization = BashOperator(
        task_id="inventory_optimization",

        bash_command=(
            f"cd {PROJECT_HOME} && "
            "spark-submit "
            "spark/decision_engine/"
            "02_inventory_optimization.py"
        ),
    )


    # =========================================================
    # TASK 6
    # =========================================================

    create_hive_tables = BashOperator(
        task_id="create_hive_tables",

        bash_command=(
            f"cd {PROJECT_HOME} && "
            "hive -f "
            "hive/create_hive_tables.sql"
        ),
    )


    # =========================================================
    # TASK DEPENDENCIES
    # =========================================================

    (
        create_hdfs_structure
        >> upload_raw_data
        >> run_batch_etl
        >> [
            dynamic_pricing,
            inventory_optimization,
        ]
        >> create_hive_tables
    )
