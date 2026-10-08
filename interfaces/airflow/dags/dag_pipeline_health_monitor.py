"""Airflow DAG for automated pipeline health monitoring across data platform services."""

from __future__ import annotations

import os
import socket
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def check_tcp_endpoint(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(5)
        return sock.connect_ex((host, port)) == 0


def verify_platform_services(**context) -> None:
    kafka_broker = os.getenv("KAFKA_BROKER", "kafka:9092")
    kafka_host, kafka_port = kafka_broker.split(",", 1)[0].split(":")
    trino_host = os.getenv("TRINO_HOST", "trino")
    trino_port = int(os.getenv("TRINO_PORT", "8080"))

    kafka_ok = check_tcp_endpoint(kafka_host, int(kafka_port))
    trino_ok = check_tcp_endpoint(trino_host, trino_port)
    minio_ok = check_tcp_endpoint("minio", 9000)

    print(f"Service Health: Kafka={kafka_ok}, Trino={trino_ok}, MinIO={minio_ok}")

    if not all([kafka_ok, trino_ok, minio_ok]):
        raise RuntimeError(f"One or more core services unreachable! Kafka={kafka_ok}, Trino={trino_ok}, MinIO={minio_ok}")


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="pipeline_health_monitor",
    default_args=default_args,
    description="Periodic health probe for Kafka, Trino, and MinIO storage services",
    schedule_interval="*/10 * * * *",  # Every 10 minutes
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["monitoring", "healthcheck", "platform"],
) as dag:
    health_task = PythonOperator(
        task_id="verify_service_connectivity",
        python_callable=verify_platform_services,
    )

