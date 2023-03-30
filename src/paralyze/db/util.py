import functools
import time
import typing as t

from sqlalchemy import exc

__all__ = (
    "retry_on_disconnect",
)


P = t.ParamSpec("P")
R = t.TypeVar("R")


def is_disconnect(err: Exception) -> bool:
    try:
        if is_turbodbc_disconnect(err):
            return True
    except ImportError:
        pass

    try:
        if is_pymssql_disconnect(err):
            return True
    except ImportError:
        pass

    try:
        if is_mysql_disconnect(err):
            return True
    except ImportError:
        pass

    return False


def is_turbodbc_disconnect(err: Exception) -> bool:
    import turbodbc  # type: ignore

    if not isinstance(err, exc.DatabaseError):
        return False

    api_error = err.orig

    if api_error is None:
        return False

    if not isinstance(api_error, turbodbc.DatabaseError):
        return False

    try:
        msg = str(api_error.args[0])
    except Exception:
        print(api_error.args)
        return False

    if "state: HYT00" in msg and "native error code: 0" in msg:
        return True

    print(api_error.args)

    return False


def is_pymssql_disconnect(err: Exception) -> bool:
    import pymssql  # type: ignore

    return False


def is_mysql_disconnect(err: Exception) -> bool:
    import MySQLdb  # type: ignore

    if not isinstance(err, exc.OperationalError):
        return False

    orig = err.orig

    if not isinstance(orig, MySQLdb.OperationalError):
        return False

    try:
        code, msg = orig.args

        if code == 2013 and "Lost connection to MySQL server" in msg:
            return True
    except Exception:
        print(orig.args)

    return False


def retry_on_disconnect(
    max_retries: int = 3,
    delay: float = 0.1,
):
    """
    Retries a function if it throws a disconnect error
    """
    def wrapped(func: t.Callable[P, R]) -> t.Callable[P, R]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            count = 0
            err: Exception | None = None

            while count < max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as exc:
                    if not is_disconnect(exc):
                        raise exc from exc

                    err = exc

                    count += 1

                    time.sleep(delay)

            if err is None:
                # this should never happen :)
                raise Exception("Failed to reconnect")

            raise err from err

        return wrapper

    return wrapped
