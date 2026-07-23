import logging
import os
import time

# The package-root logger. `get_logger(name)` returns a CHILD of this logger
# (via getChild), so configuring it once here applies to the whole package —
# regardless of each module's own import path.
PACKAGE_LOGGER_NAME = "clustering-benchmark-python"
DEFAULT_LOG_LEVEL = "INFO"


def _resolve_log_level() -> int:
    """Level from the LOG_LEVEL env var (e.g. DEBUG, INFO), default INFO."""
    raw = os.environ.get("LOG_LEVEL", DEFAULT_LOG_LEVEL).strip().upper()
    # getLevelName maps a valid name -> int; an unknown name -> "Level <name>" str.
    level = logging.getLevelName(raw)
    if not isinstance(level, int):
        logging.getLogger(PACKAGE_LOGGER_NAME).warning(
            "Invalid LOG_LEVEL=%r; falling back to %s.", raw, DEFAULT_LOG_LEVEL
        )
        return logging.INFO
    return level


def _configure_package_logger() -> None:
    package_logger = logging.getLogger(PACKAGE_LOGGER_NAME)

    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S %Z",
    )
    formatter.converter = time.gmtime
    handler.setFormatter(formatter)
    package_logger.addHandler(handler)
    # Don't also propagate to the root logger's handlers — avoids duplicate output
    # if the host app (or Spark) configures the root logger too.
    package_logger.propagate = False
    package_logger.setLevel(_resolve_log_level())


def get_logger(name: str) -> logging.Logger:
    package_logger = logging.getLogger(PACKAGE_LOGGER_NAME)
    # Configure once. Guard on its OWN handlers (not `hasHandlers()`, which walks
    # ancestors) so a host app configuring the root logger doesn't skip our setup.
    if not package_logger.handlers:
        _configure_package_logger()

    return package_logger.getChild(name)
