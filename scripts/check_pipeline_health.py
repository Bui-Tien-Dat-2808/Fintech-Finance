from __future__ import annotations

import socket

from application.services.pipeline_health_service import PipelineHealthService
from shared.config.settings import Settings
from shared.logging.logger import configure_logging, get_logger


def _check_socket(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(5)
        return sock.connect_ex((host, port)) == 0


def _parse_broker_endpoint(broker: str) -> tuple[str, int]:
    host, port = broker.split(",", maxsplit=1)[0].split(":")
    return host, int(port)


def main() -> None:
    settings = Settings.from_env()
    configure_logging(settings)
    logger = get_logger("pipeline_health")

    kafka_host, kafka_port = _parse_broker_endpoint(settings.kafka_broker)
    kafka_ok = _check_socket(kafka_host, kafka_port)
    trino_ok = _check_socket(settings.trino_host, settings.trino_port)
    checkpoint_ok = PipelineHealthService(settings).checkpoints_exist()

    logger.info(
        "Pipeline health status kafka_ok=%s trino_ok=%s checkpoint_ok=%s",
        kafka_ok,
        trino_ok,
        checkpoint_ok,
    )

    if not all([kafka_ok, trino_ok, checkpoint_ok]):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
