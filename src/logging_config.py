"""
Logging Configuration
======================
Central logging setup for the AI Data Quality Agent.

WHY A SHARED CONFIG:
  print() has no severity levels, can't be filtered, and can't be
  redirected anywhere except stdout. In anything beyond a local demo —
  a container, a scheduled job, a real deployment — you need to ask
  "show me only warnings and errors" or "send logs somewhere other
  than the terminal" without touching every call site.

  One shared get_logger() means every module configures logging the
  same way, and the level can change globally via one env var instead
  of hunting through the codebase.
"""

import logging
import os

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def get_logger(name: str) -> logging.Logger:
    """
    Get a module-level logger. Call once per file:
        logger = get_logger(__name__)
    """
    return logging.getLogger(name)