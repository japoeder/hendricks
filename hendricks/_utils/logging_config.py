"""
Logging configuration utility for Hendricks.
"""

import logging
import logging.handlers
import queue
import sys


def setup_logging():
    """Configure logging for the application."""
    log_queue = queue.Queue()
    queue_handler = logging.handlers.QueueHandler(log_queue)

    root = logging.getLogger()
    root.addHandler(queue_handler)
    root.setLevel(logging.INFO)

    # Set up a listener to handle log messages
    listener = logging.handlers.QueueListener(
        log_queue, logging.FileHandler("app.log"), logging.StreamHandler(sys.stdout)
    )
    listener.start()


# import logging
# from logging.handlers import RotatingFileHandler
# from hendricks._utils.get_path import get_path


# def setup_logging():
#     """Configure logging with both file and console handlers."""
#     app_log_path = get_path("log")

#     # Configure logging with RotatingFileHandler
#     file_handler = RotatingFileHandler(
#         app_log_path, maxBytes=5 * 1024 * 1024, backupCount=5
#     )  # 5 MB max size, keep 5 backups
#     file_handler.setLevel(logging.DEBUG)
#     formatter = logging.Formatter(
#         "%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s"
#     )
#     file_handler.setFormatter(formatter)

#     # Configure console handler
#     console_handler = logging.StreamHandler()
#     console_handler.setLevel(logging.DEBUG)
#     console_handler.setFormatter(formatter)

#     # Set up root logger
#     root_logger = logging.getLogger()
#     root_logger.setLevel(logging.DEBUG)

#     # Remove any existing handlers to avoid duplicates
#     root_logger.handlers = []

#     # Add both handlers
#     root_logger.addHandler(file_handler)
#     root_logger.addHandler(console_handler)
