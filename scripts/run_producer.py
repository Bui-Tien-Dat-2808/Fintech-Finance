from application.services.streaming_service import StreamingService
from shared.config.settings import Settings
from shared.logging.logger import configure_logging


def main() -> None:
    settings = Settings.from_env()
    configure_logging(settings)
    service = StreamingService(settings)
    try:
        service.run()
    finally:
        service.close()


if __name__ == "__main__":
    main()
