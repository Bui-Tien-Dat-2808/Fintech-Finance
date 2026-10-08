"""Seeds realistic historical candle data and anomalies into Apache Iceberg via Trino.

Generates realistic 1-minute OHLCV & VWAP bars and market anomalies for rich dashboards.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from trino.dbapi import connect


def seed_data(candles_per_symbol: int = 120) -> None:
    conn = connect(
        host="localhost",
        port=8080,
        user="admin",
        catalog="iceberg",
        schema="stock",
    )
    cursor = conn.cursor()

    symbols_meta = {
        "AAPL": {"base_price": 224.50, "volatility": 0.35, "base_vol": 850},
        "MSFT": {"base_price": 418.20, "volatility": 0.50, "base_vol": 620},
        "AMZN": {"base_price": 186.80, "volatility": 0.40, "base_vol": 780},
        "GOOGL": {"base_price": 165.40, "volatility": 0.30, "base_vol": 540},
        "BINANCE:BTCUSDT": {"base_price": 84200.0, "volatility": 120.0, "base_vol": 25},
    }

    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    print(f"Generating {candles_per_symbol} candles for each symbol up to {now}...")

    # 1. Seed OHLCV Candles into aggregated_data
    candle_rows = []
    for symbol, meta in symbols_meta.items():
        curr_price = meta["base_price"]
        for i in range(candles_per_symbol, 0, -1):
            w_start = now - timedelta(minutes=i)
            w_end = w_start + timedelta(minutes=1)
            trade_date = w_start.strftime("%Y-%m-%d")

            change = (random.random() - 0.49) * meta["volatility"]
            open_p = round(curr_price, 2)
            close_p = round(curr_price + change, 2)
            high_p = round(max(open_p, close_p) + abs(random.random() * meta["volatility"] * 0.5), 2)
            low_p = round(min(open_p, close_p) - abs(random.random() * meta["volatility"] * 0.5), 2)
            avg_p = round((open_p + high_p + low_p + close_p) / 4.0, 2)
            vwap_p = round((open_p + high_p * 2 + low_p + close_p * 2) / 6.0, 2)

            vol = int(meta["base_vol"] * random.uniform(0.6, 2.2))
            trade_cnt = int(max(5, vol // 15))

            w_start_str = w_start.strftime("%Y-%m-%d %H:%M:%S.000 UTC")
            w_end_str = w_end.strftime("%Y-%m-%d %H:%M:%S.000 UTC")

            candle_rows.append(
                f"('{symbol}', TIMESTAMP '{w_start_str}', TIMESTAMP '{w_end_str}', "
                f"{open_p}, {high_p}, {low_p}, {close_p}, {avg_p}, {vwap_p}, {vol}, {trade_cnt}, DATE '{trade_date}')"
            )
            curr_price = close_p

    # Batch insert candles in chunks of 50
    chunk_size = 50
    for i in range(0, len(candle_rows), chunk_size):
        chunk = candle_rows[i:i + chunk_size]
        query = (
            "INSERT INTO iceberg.stock.aggregated_data "
            "(symbol, window_start, window_end, open_price, high_price, low_price, "
            "close_price, avg_price, vwap, total_volume, trade_count, trade_date) VALUES "
            + ",\n".join(chunk)
        )
        cursor.execute(query)
        print(f"Inserted {min(i + chunk_size, len(candle_rows))}/{len(candle_rows)} candles into aggregated_data.")

    # 2. Seed Anomalies into market_anomalies
    print("Generating market anomaly events...")
    anomaly_samples = [
        ("AAPL", 228.80, 85000, "LARGE_BLOCK_TRADE", "Large institutional block trade detected for AAPL", "INFO", 15),
        ("MSFT", 425.10, 62000, "LARGE_BLOCK_TRADE", "Large buy block trade detected for MSFT", "INFO", 35),
        ("BINANCE:BTCUSDT", 86500.0, 150, "EXTREME_VOLATILITY", "Price moved >2.5% within 1 minute window", "WARNING", 55),
        ("AMZN", 182.20, 92000, "LARGE_BLOCK_TRADE", "Large sell block trade detected for AMZN", "INFO", 75),
        ("GOOGL", 168.90, 58000, "LARGE_BLOCK_TRADE", "High volume sweep order detected for GOOGL", "INFO", 90),
    ]

    anomaly_rows = []
    for sym, price, vol, a_type, desc, sev, min_ago in anomaly_samples:
        t_time = now - timedelta(minutes=min_ago)
        t_time_str = t_time.strftime("%Y-%m-%d %H:%M:%S.000 UTC")
        t_date = t_time.strftime("%Y-%m-%d")
        anomaly_rows.append(
            f"('{sym}', TIMESTAMP '{t_time_str}', {price}, {vol}, '{a_type}', '{desc}', '{sev}', DATE '{t_date}')"
        )

    anomaly_query = (
        "INSERT INTO iceberg.stock.market_anomalies "
        "(symbol, trade_timestamp, price, volume, anomaly_type, description, severity, trade_date) VALUES "
        + ",\n".join(anomaly_rows)
    )
    cursor.execute(anomaly_query)
    print("Successfully seeded anomalies into market_anomalies!")

    conn.close()
    print("All demo data seeded successfully into Apache Iceberg!")


if __name__ == "__main__":
    seed_data(candles_per_symbol=120)

