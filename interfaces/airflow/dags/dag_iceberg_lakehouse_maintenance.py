"""Airflow DAG for automated Apache Iceberg Lakehouse Maintenance.

Solves the Small Files Problem, optimizes table layouts, and cleans metadata.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def get_trino_connection():
    from trino.dbapi import connect

    host = os.getenv("TRINO_HOST", "trino")
    port = int(os.getenv("TRINO_PORT", "8080"))
    catalog = os.getenv("TRINO_CATALOG", "iceberg")
    schema = os.getenv("TRINO_SCHEMA", "stock")

    return connect(
        host=host,
        port=port,
        user="airflow",
        catalog=catalog,
        schema=schema,
    )


def compact_iceberg_tables(**context) -> None:
    """Compacts small Parquet files produced by streaming micro-batches."""
    tables = ["raw_stream_data", "aggregated_data", "market_anomalies"]
    conn = get_trino_connection()
    cursor = conn.cursor()

    for table in tables:
        print(f"Triggering compaction for table: stock.{table}")
        query = f"ALTER TABLE iceberg.stock.{table} EXECUTE optimize(file_size_threshold => '32MB')"
        try:
            cursor.execute(query)
            print(f"Successfully optimized table stock.{table}")
        except Exception as exc:
            # Fallback to system procedure if optimize syntax varies across Trino releases
            print(f"Standard optimize query exception: {exc}. Trying rewrite_data_files procedure...")
            try:
                cursor.execute(f"CALL iceberg.system.rewrite_data_files(schema => 'stock', table => '{table}')")
                print(f"Successfully ran rewrite_data_files on {table}")
            except Exception as e:
                print(f"Warning: Compaction for {table} failed: {e}")

    conn.close()


def expire_iceberg_snapshots(**context) -> None:
    """Expires old Iceberg table snapshots to prevent metadata bloat."""
    tables = ["raw_stream_data", "aggregated_data"]
    conn = get_trino_connection()
    cursor = conn.cursor()

    for table in tables:
        print(f"Expiring snapshots older than 7 days for table: stock.{table}")
        query = f"ALTER TABLE iceberg.stock.{table} EXECUTE expire_snapshots(retention_threshold => '7d')"
        try:
            cursor.execute(query)
            print(f"Successfully expired snapshots for stock.{table}")
        except Exception as exc:
            print(f"Snapshot expiration notice for {table}: {exc}")

    conn.close()


def remove_orphan_files(**context) -> None:
    """Removes orphan files from MinIO S3 warehouse directory."""
    conn = get_trino_connection()
    cursor = conn.cursor()

    for table in ["raw_stream_data", "aggregated_data"]:
        print(f"Removing orphan files for table: stock.{table}")
        query = f"ALTER TABLE iceberg.stock.{table} EXECUTE remove_orphan_files(retention_threshold => '7d')"
        try:
            cursor.execute(query)
            print(f"Orphan file cleanup completed for {table}")
        except Exception as exc:
            print(f"Orphan files removal notice for {table}: {exc}")

    conn.close()


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="iceberg_lakehouse_maintenance",
    default_args=default_args,
    description="Automated daily maintenance for Iceberg tables: Compaction, Expire Snapshots, Orphan Files",
    schedule_interval="0 2 * * *",  # Daily at 02:00 AM UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lakehouse", "iceberg", "maintenance", "trino"],
) as dag:
    task_compaction = PythonOperator(
        task_id="compact_data_files",
        python_callable=compact_iceberg_tables,
    )

    task_expire_snapshots = PythonOperator(
        task_id="expire_old_snapshots",
        python_callable=expire_iceberg_snapshots,
    )

    task_remove_orphans = PythonOperator(
        task_id="remove_orphan_files",
        python_callable=remove_orphan_files,
    )

    task_compaction >> task_expire_snapshots >> task_remove_orphans

