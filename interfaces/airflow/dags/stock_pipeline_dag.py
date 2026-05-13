from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_DIR = "/opt/project"
COMPOSE_FILE = f"{PROJECT_DIR}/docker-compose.yaml"
PROJECT_NAME = "fintechfinance"


with DAG(
    dag_id="stock_pipeline_start",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["stocks", "streaming", "portfolio"],
) as start_dag:
    check_dependencies = BashOperator(
        task_id="check_dependencies",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"docker compose -p {PROJECT_NAME} ps kafka trino spark-master hive-metastore"
        ),
    )

    ensure_kafka_topic = BashOperator(
        task_id="ensure_kafka_topic",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"docker compose -p {PROJECT_NAME} exec -T stock-producer "
            "python3 scripts/bootstrap_kafka_topic.py"
        ),
    )

    ensure_iceberg_tables = BashOperator(
        task_id="ensure_iceberg_tables",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"docker compose -p {PROJECT_NAME} exec -T spark-master "
            "/opt/spark/bin/spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 --conf spark.sql.catalog.stock_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.stock_catalog.type=hive --conf spark.sql.catalog.stock_catalog.uri=thrift://hive-metastore:9083 --conf spark.sql.catalog.stock_catalog.warehouse=file:///data/warehouse scripts/bootstrap_iceberg.py"
        ),
    )

    start_spark_job = BashOperator(
        task_id="start_spark_job",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"docker compose -p {PROJECT_NAME} start spark-streaming-job"
        ),
    )

    start_producer = BashOperator(
        task_id="start_producer",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"docker compose -p {PROJECT_NAME} start stock-producer"
        ),
    )

    check_dependencies >> ensure_kafka_topic >> ensure_iceberg_tables >> start_spark_job >> start_producer


with DAG(
    dag_id="stock_pipeline_monitor",
    start_date=datetime(2026, 1, 1),
    schedule_interval="*/5 * * * *",
    catchup=False,
    tags=["stocks", "monitoring", "portfolio"],
) as monitor_dag:
    health_check = BashOperator(
        task_id="health_check",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"docker compose -p {PROJECT_NAME} exec -T spark-master "
            "python scripts/check_pipeline_health.py"
        ),
    )


with DAG(
    dag_id="stock_pipeline_stop",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["stocks", "streaming", "portfolio"],
) as stop_dag:
    stop_streaming_components = BashOperator(
        task_id="stop_streaming_components",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"docker compose -p {PROJECT_NAME} stop stock-producer spark-streaming-job"
        ),
    )
