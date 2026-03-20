import logging
import sys


LOGGER_NAME = "PegasusExtract"


def get_logger() -> logging.Logger:
    logger = logging.getLogger(LOGGER_NAME)

    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


log = get_logger()

