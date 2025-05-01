"""
Logging utilities for TaskMasterPy.

This module provides utilities for configuring logging.
"""
import os
import logging
from logging.handlers import RotatingFileHandler
from typing import Optional


def configure_logging(
    log_level: int = logging.INFO,
    log_file: Optional[str] = None,
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
) -> None:
    """Configure logging for TaskMasterPy.
    
    Args:
        log_level: The logging level
        log_file: Path to the log file (if None, logs to console only)
        log_format: The log message format
    """
    # Create a logger
    logger = logging.getLogger("taskmaster")
    logger.setLevel(log_level)
    
    # Remove any existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Create a formatter
    formatter = logging.Formatter(log_format)
    
    # Create a console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Create a file handler if a log file is specified
    if log_file:
        # Create the directory if it doesn't exist
        log_dir = os.path.dirname(os.path.abspath(log_file))
        os.makedirs(log_dir, exist_ok=True)
        
        # Create a rotating file handler
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5
        )
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    # Prevent propagation to the root logger
    logger.propagate = False


def get_logger(name: str) -> logging.Logger:
    """Get a logger with the specified name.
    
    Args:
        name: The logger name
        
    Returns:
        The logger
    """
    return logging.getLogger(f"taskmaster.{name}")
