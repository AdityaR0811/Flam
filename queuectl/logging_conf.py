"""Logging configuration with rotating file handler."""

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path


def setup_logging(
    worker_id: str | None = None,
    log_dir: Path | None = None,
    level: int = logging.INFO,
) -> logging.Logger:
    """Setup logging with rotating file handler.
    
    Args:
        worker_id: Optional worker ID for worker-specific logs.
        log_dir: Optional log directory (default: ~/.queuectl/logs).
        level: Logging level.
        
    Returns:
        Configured logger.
    """
    if log_dir is None:
        log_dir = Path.home() / ".queuectl" / "logs"
    
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Determine log file name
    if worker_id:
        log_file = log_dir / f"worker-{worker_id}.log"
        logger_name = f"queuectl.worker.{worker_id}"
    else:
        log_file = log_dir / "queuectl.log"
        logger_name = "queuectl"
    
    # Create logger
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    logger.propagate = False
    
    # Remove existing handlers
    logger.handlers.clear()
    
    # File handler with rotation (10MB max, 5 backups)
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setLevel(level)
    
    # Console handler for errors
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(logging.ERROR)
    
    # Format
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger
