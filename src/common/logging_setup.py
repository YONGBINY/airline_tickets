# src/common/logging_setup.py
import logging
from concurrent_log_handler import ConcurrentRotatingFileHandler
from .paths import LOG_DIR

def setup_logging(name="airline", log_file="pipeline.log"):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")

    fh = ConcurrentRotatingFileHandler(
        LOG_DIR / log_file,
        maxBytes=5_000_000,
        backupCount=5,
        encoding="utf-8"
    )
    fh.setFormatter(fmt)

    # 콘솔 로그
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)

    if not logger.handlers:  # 중복 방지
        logger.addHandler(fh)
        logger.addHandler(ch)

    return logger