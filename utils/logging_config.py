"""
Centralized logging configuration for the Dutch Real Estate Scraper.
This module provides utilities to configure logging for different components,
with support for log rotation to manage file sizes.
"""

import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from typing import List, Optional


def configure_logging(
    name: str,
    log_level: int = logging.INFO,
    log_file: Optional[str] = None,
    log_format: Optional[str] = None,
    disable_loggers: Optional[List[str]] = None,
    max_bytes: int = 10 * 1024 * 1024,  # 10 MB default
    backup_count: int = 5  # Keep 5 backup files by default
) -> logging.Logger:
    """
    Configure a logger with consistent settings and log rotation.
    
    Args:
        name: The name of the logger
        log_level: The logging level (default: INFO)
        log_file: Path to the log file (default: None, logs only to console)
        log_format: Format string for log messages (default: standard format)
        disable_loggers: List of logger names to suppress (e.g., 'httpx', 'telegram')
        max_bytes: Maximum size of log file before rotation (default: 10MB)
        backup_count: Number of backup log files to keep (default: 5)
    
    Returns:
        The configured logger
    """
    # Create logs directory if it doesn't exist and a log file is specified
    if log_file and not os.path.exists(os.path.dirname(log_file)):
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    # Use default format if not provided
    if not log_format:
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Get or create the logger
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    
    # Clear existing handlers to avoid duplicates if configure_logging is called multiple times
    if logger.handlers:
        logger.handlers.clear()
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(logging.Formatter(log_format))
    logger.addHandler(console_handler)
    
    # Add rotating file handler if log_file is specified
    if log_file:
        file_handler = RotatingFileHandler(
            filename=log_file,
            maxBytes=max_bytes,
            backupCount=backup_count
        )
        file_handler.setLevel(log_level)
        file_handler.setFormatter(logging.Formatter(log_format))
        logger.addHandler(file_handler)
    
    # Disable other loggers if specified
    if disable_loggers:
        for logger_name in disable_loggers:
            logging.getLogger(logger_name).setLevel(logging.WARNING)
    
    return logger


def configure_scraper_logging(
    log_level: int = logging.INFO,
    log_to_file: bool = True,
    max_bytes: int = 10 * 1024 * 1024,  # 10 MB
    backup_count: int = 5
) -> logging.Logger:
    """
    Configure logging specifically for the scraper component with log rotation.
    
    Args:
        log_level: The logging level (default: INFO)
        log_to_file: Whether to log to a file (default: True)
        max_bytes: Maximum size of log file before rotation (default: 10MB)
        backup_count: Number of backup log files to keep (default: 5)
    
    Returns:
        The configured logger
    """
    log_file = "logs/scraper.log" if log_to_file else None
    return configure_logging(
        name="realestate_scraper",
        log_level=log_level,
        log_file=log_file,
        disable_loggers=["urllib3", "httpx"],
        max_bytes=max_bytes,
        backup_count=backup_count
    )


def configure_cli_logging(
    log_level: int = logging.INFO,
    log_to_file: bool = True,
    max_bytes: int = 10 * 1024 * 1024,  # 10 MB
    backup_count: int = 5
) -> logging.Logger:
    """
    Configure logging specifically for the CLI component with log rotation.
    
    Args:
        log_level: The logging level (default: INFO)
        log_to_file: Whether to log to a file (default: True)
        max_bytes: Maximum size of log file before rotation (default: 10MB)
        backup_count: Number of backup log files to keep (default: 5)
    
    Returns:
        The configured logger
    """
    log_file = "logs/cli.log" if log_to_file else None
    return configure_logging(
        name="realestate_cli",
        log_level=log_level,
        log_file=log_file,
        max_bytes=max_bytes,
        backup_count=backup_count
    )


def configure_telegram_logging(
    log_level: int = logging.INFO,
    log_to_file: bool = True,
    max_bytes: int = 10 * 1024 * 1024,  # 10 MB
    backup_count: int = 5
) -> logging.Logger:
    """
    Configure logging specifically for the Telegram component with log rotation.
    
    Args:
        log_level: The logging level (default: INFO)
        log_to_file: Whether to log to a file (default: True)
        max_bytes: Maximum size of log file before rotation (default: 10MB)
        backup_count: Number of backup log files to keep (default: 5)
    
    Returns:
        The configured logger
    """
    log_file = "logs/telegram.log" if log_to_file else None
    logger = configure_logging(
        name="telegram_bot",
        log_level=log_level,
        log_file=log_file,
        disable_loggers=["httpx", "telegram"],
        max_bytes=max_bytes,
        backup_count=backup_count
    )
    
    # Suppress verbose logs from httpx and telegram libraries
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("telegram").setLevel(logging.WARNING)
    
    return logger


def get_logger(name: str, parent_logger_name: str = None) -> logging.Logger:
    """
    Get a child logger from a parent logger.
    This ensures proper logger hierarchy without creating duplicate handlers.
    
    Args:
        name: The name for this logger (will be appended to parent name)
        parent_logger_name: The name of the parent logger
        
    Returns:
        A properly configured logger
    """
    if parent_logger_name:
        full_name = f"{parent_logger_name}.{name}"
    else:
        full_name = name
        
    return logging.getLogger(full_name)


def get_telegram_logger(component_name: str) -> logging.Logger:
    """
    Get a child logger of the telegram logger.
    
    Args:
        component_name: The name of the component (e.g., 'bot', 'notification_manager')
        
    Returns:
        A configured logger
    """
    return get_logger(component_name, "telegram_bot")


def get_scraper_logger(component_name: str) -> logging.Logger:
    """
    Get a child logger of the scraper logger.
    
    Args:
        component_name: The name of the component (e.g., 'http_client', 'parser')
        
    Returns:
        A configured logger
    """
    return get_logger(component_name, "realestate_scraper")