"""Airflow DAG to synchronize dimension company profiles from Finnhub REST API into Iceberg.

Enriches the Lakehouse with Reference data for Star-Schema analytical queries.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator


def sync_finnhub_company_dimensions(**context) -> None:
    """Fetches company profile data from Finnhub REST API and inserts into Iceberg dim_company."""
    import requests
    from trino.dbapi import connect

    api_key = os.getenv("FINNHUB_API_KEY")
    rest_url = os.getenv("FINNHUB_REST_URL", "https://finnhub.io/api/v1")
    symbols_str = os.getenv("STOCK_SYMBOLS", "AAPL,MSFT,AMZN,GOOGL")
    symbols = [s.strip() for s in symbols_str.split(",") if s.strip()]

    host = os.getenv("TRINO_HOST", "trino")
    port = int(os.getenv("TRINO_PORT", "8080"))
    catalog = os.getenv("TRINO_CATALOG", "iceberg")
    schema = os.getenv("TRINO_SCHEMA", "stock")

    conn = connect(
        host=host,
        port=port,
        user="airflow",
        catalog=catalog,
        schema=schema,
    )
    cursor = conn.cursor()

    # Ensure table exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS iceberg.stock.dim_company (
            symbol VARCHAR,
            company_name VARCHAR,
            country VARCHAR,
            currency VARCHAR,
            exchange VARCHAR,
            ipo_date VARCHAR,
            market_capitalization DOUBLE,
            finnhub_industry VARCHAR,
            updated_at TIMESTAMP(6) WITH TIME ZONE
        )
    """)

    now_iso = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S.000000 UTC")

    for symbol in symbols:
        url = f"{rest_url}/stock/profile2"
        params = {"symbol": symbol, "token": api_key}
        try:
            resp = requests.get(url, params=params, timeout=10)
            if resp.status_code != 200:
                print(f"Failed to fetch profile for {symbol}: status {resp.status_code}")
                continue

            profile = resp.json()
            if not profile or not profile.get("name"):
                print(f"No profile returned for symbol {symbol}")
                continue

            company_name = profile.get("name", "").replace("'", "''")
            country = profile.get("country", "")
            currency = profile.get("currency", "")
            exchange = profile.get("exchange", "")
            ipo = profile.get("ipo", "")
            market_cap = float(profile.get("marketCapitalization", 0.0))
            industry = profile.get("finnhubIndustry", "").replace("'", "''")

            # Delete old record if exists, then insert updated dimension
            cursor.execute(f"DELETE FROM iceberg.stock.dim_company WHERE symbol = '{symbol}'")

            insert_query = f"""
                INSERT INTO iceberg.stock.dim_company (
                    symbol, company_name, country, currency, exchange, ipo_date,
                    market_capitalization, finnhub_industry, updated_at
                ) VALUES (
                    '{symbol}', '{company_name}', '{country}', '{currency}', '{exchange}',
                    '{ipo}', {market_cap}, '{industry}', TIMESTAMP '{now_iso}'
                )
            """
            cursor.execute(insert_query)
            print(f"Successfully synced dimension for {symbol}: {company_name}")

        except Exception as exc:
            print(f"Error syncing {symbol}: {exc}")

    conn.close()


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="finnhub_dimension_sync",
    default_args=default_args,
    description="Syncs company metadata from Finnhub REST API into Iceberg dim_company table",
    schedule_interval="0 4 * * 1-5",  # Weekdays at 04:00 AM UTC before market open
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["dimension", "finnhub", "rest-api", "iceberg", "trino"],
) as dag:
    sync_dimensions_task = PythonOperator(
        task_id="sync_company_profiles",
        python_callable=sync_finnhub_company_dimensions,
    )

