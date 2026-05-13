from __future__ import annotations

from pathlib import Path

from shared.config.settings import Settings
from shared.logging.logger import get_logger


class PipelineHealthService:
    """Validates runtime dependencies used by the orchestration layer."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._logger = get_logger(self.__class__.__name__)

    def checkpoints_exist(self) -> bool:
        checkpoint_root = Path(self._settings.spark_checkpoint_root)
        exists = checkpoint_root.exists()
        self._logger.info(
            "Checkpoint root validation completed. path=%s exists=%s",
            checkpoint_root,
            exists,
        )
        return exists
