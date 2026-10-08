"""Airflow DAG for Data Quality Validation and Reconciliation across Lakehouse tiers.

Enforces financial data contracts and invariants on Iceberg tables via Trino.
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


def assert_raw_trade_quality(**context) -> None:
    """Validates that raw_stream_data has no invalid prices or negative volumes."""
    conn = get_trino_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT count(*)
        FROM iceberg.stock.raw_stream_data
        WHERE price <= 0 OR volume < 0
    """)
    invalid_rows = cursor.fetchone()[0]
    conn.close()

    if invalid_rows > 0:
        raise ValueError(f"Data Quality Violation: Found {invalid_rows} raw trades with non-positive price or negative volume!")

    print("Data Quality PASSED: 0 invalid trades found in raw_stream_data.")


def assert_candlestick_invariants(**context) -> None:
    """Validates financial invariants on OHLCV candlesticks: High >= Low, High >= Open, High >= Close."""
    conn = get_trino_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT count(*)
        FROM iceberg.stock.aggregated_data
        WHERE high_price < low_price
           OR high_price < open_price
           OR high_price < close_price
           OR low_price > open_price
           OR low_price > close_price
    """)
    inconsistent_candles = cursor.fetchone()[0]
    conn.close()

    if inconsistent_candles > 0:
        raise ValueError(f"Data Quality Violation: Found {inconsistent_candles} candlesticks violating OHLC consistency bounds!")

    print("Data Quality PASSED: All candlesticks adhere to OHLC financial invariants.")


def check_tier_reconciliation(**context) -> None:
    """Verifies data freshness and volume across tiers."""
    conn = get_trino_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT count(*), max(trade_timestamp) FROM iceberg.stock.raw_stream_data")
    raw_stats = cursor.fetchone()
    raw_count = raw_stats[0] if raw_stats else 0
    latest_trade = raw_stats[1] if raw_stats else "N/A"

    cursor.execute("SELECT count(*), sum(total_volume) FROM iceberg.stock.aggregated_data")
    agg_stats = cursor.fetchone()
    agg_count = agg_stats[0] if agg_stats else 0
    total_vol = agg_stats[1] if agg_stats else 0

    print(f"Reconciliation Summary: raw_records={raw_count}, aggregated_bars={agg_count}, total_volume={total_vol}, latest_trade={latest_trade}")
    conn.close()


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="data_quality_reconciliation",
    default_args=default_args,
    description="Automated Data Quality & Contract Assertions on Iceberg Financial Tables",
    schedule_interval="0 * * * *",  # Hourly
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["data-quality", "reconciliation", "iceberg", "trino"],
) as dag:
    task_raw_quality = PythonOperator(
        task_id="assert_raw_trade_quality",
        python_callable=assert_raw_trade_quality,
    )

    task_candle_quality = PythonOperator(
        task_id="assert_candlestick_invariants",
        python_callable=assert_candlestick_invariants,
    )

    task_reconciliation = PythonOperator(
        task_id="tier_reconciliation_summary",
        python_callable=check_tier_reconciliation,
    )

    [task_raw_quality, task_candle_quality] >> task_reconciliation

