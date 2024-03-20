import contextlib
import logging
import random
import string
import sys
import typing as t

import structlog
from structlog import contextvars


Logger: t.TypeAlias = structlog.stdlib.BoundLogger


class LoggingConfig(t.TypedDict):
    level: int


LOGGERS: t.Dict[str, LoggingConfig] = {
    "urllib3.connectionpool": {
        "level": logging.WARNING,
    },
    "google.auth._default": {
        "level": logging.WARNING,
    },
}


def gen_collation_id(length: int = 3, letters: str = string.ascii_letters):
    """
    Generate a random string to use as a correlation ID.
    """
    return "".join(random.choice(letters) for i in range(length))


def get_logger(name: str) -> Logger:
    """
    Get a logger with the given name.
    """
    return t.cast(Logger, structlog.getLogger(name))


@contextlib.contextmanager
def context(**kwargs: str):
    """
    Context manager to bind context variables to the current context.
    """
    with contextvars.bound_contextvars(**kwargs):
        yield


def structlog_processors() -> t.List:
    """
    Get the structlog processors to use.
    """
    # Configure structlog to print pretty stack traces when we run in a terminal
    # session, and to print JSON when we run in a Docker container.
    shared_processors = [
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
    ]

    if sys.stderr.isatty():
        # Pretty printing when we run in a terminal session.
        # Automatically prints pretty stack traces when "rich" is installed

        processors = shared_processors + [
            structlog.dev.ConsoleRenderer(),
        ]
    else:
        # Print JSON when we run, e.g., in a Docker container.
        # Also print structured stack traces.

        processors = shared_processors + [
            structlog.processors.format_exc_info,
            structlog.processors.dict_tracebacks,
            structlog.processors.JSONRenderer(),
        ]

    return processors


def configure(loggers: t.Dict[str, LoggingConfig] | None = None) -> None:
    """
    Configure the logging system.
    """
    structlog.configure(
        processors=structlog_processors(),
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    for name, config in LOGGERS.items():
        logging.getLogger(name).setLevel(config["level"])

    for name, config in (loggers or {}).items():
        logging.getLogger(name).setLevel(config["level"])
