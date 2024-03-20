"""
Helper code to handle transient database errors. This module provides a
decorator that can be used to retry a function if it raises a transient
database error.
"""

import typing as t

from sqlalchemy import exc

try:
    import turbodbc  # type: ignore
except ImportError:
    turbodbc = None

try:
    import pymssql  # type: ignore
except ImportError:
    pymssql = None

try:
    import MySQLdb
except ImportError:
    MySQLdb = None  # type: ignore

try:
    import pymysql
except ImportError:
    pymysql = None  # type: ignore


from paralyze import logging, context

__all__ = (
    "retry",
    "is_transient_error",
)

P = t.ParamSpec("P")
R = t.TypeVar("R")

logger = logging.get_logger(__name__)


def unwrap_sqlalchemy_error(err: BaseException) -> BaseException | None:
    """
    Unwrap the error to the original exception.
    """
    if isinstance(err, exc.DBAPIError):
        orig = err.orig

        if orig is None:
            # this should never happen
            return None

        return t.cast(BaseException, orig)

    return err


def is_transient_error(err: BaseException) -> bool:
    """
    Check if the error is a transient error. If the error is transient, then it
    is recoverable and can be retried.
    """
    my_err = unwrap_sqlalchemy_error(err)

    if my_err is None:
        return False

    if is_transient_error_turbodbc(my_err):
        return True

    if is_transient_error_pymssql(my_err):
        return True

    if is_transient_error_MySQLdb(my_err):
        return True

    if is_transient_error_pymysql(my_err):
        return True

    return False


def is_transient_error_turbodbc(err: BaseException) -> bool:
    """
    Check if the error is a transient error for turbodbc.

    This function should not raise any unnecessary exceptions.
    """
    if not turbodbc:
        return False

    if not isinstance(err, turbodbc.DatabaseError):
        return False

    try:
        msg = str(err.args[0])
    except BaseException:
        logger.exception(args=err.args, module="turbodbc")

        return False

    if "state: HYT00" in msg and "native error code: 0" in msg:
        return True

    return False


def is_transient_error_pymssql(err: BaseException) -> bool:
    """
    Check if the error is a transient error for pymssql.

    This function should not raise any unnecessary exceptions.
    """
    if not pymssql:
        return False

    # this is blank for now

    return False


def is_transient_error_MySQLdb(err: BaseException) -> bool:
    """
    Check if the error is a transient error for MySQL.

    This function should not raise any unnecessary exceptions.
    """
    if not MySQLdb:
        return False

    if not isinstance(err, MySQLdb.OperationalError):
        return False

    try:
        code, msg = err.args

        if code == 2013 and "Lost connection to MySQL server" in msg:
            return True
    except BaseException:
        logger.exception("is-transient-error", args=err.args, module="MySQLdb")

    return False


def is_transient_error_pymysql(err: BaseException) -> bool:
    if not pymysql:
        return False

    if not isinstance(err, pymysql.err.OperationalError):
        return False

    try:
        code, msg = err.args

        if code == 2013 and "Lost connection to MySQL server" in msg:
            return True
    except BaseException:
        logger.exception("is-transient-error", args=err.args, module="pymysql")

    return False


def retry(
    ctx: context.Context[context.Config],
    func: t.Callable[P, R],
) -> context.Retryable[context.Config, P, R]:
    """
    Retry the function if it raises a transient error.
    """
    return ctx.retry(func).error_handler(is_transient_error)
