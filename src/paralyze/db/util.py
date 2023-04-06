import functools
import time
import typing as t

from sqlalchemy import exc

__all__ = (
    "retry_on_disconnect",
)


P = t.ParamSpec("P")
R = t.TypeVar("R")


def unwrap_sqlalchemy_error(err: BaseException) -> BaseException | None:
    if isinstance(err, exc.DBAPIError):
        return err.orig

    return err


def is_disconnect(err: BaseException) -> bool:
    err = unwrap_sqlalchemy_error(err)

    if err is None:
        return False

    if is_turbodbc_disconnect(err):
        return True

    if is_pymssql_disconnect(err):
        return True

    if is_mysql_disconnect(err):
        return True

    if is_pymysql_disconnect(err):
        return True

    return False


def is_turbodbc_disconnect(err: Exception) -> bool:
    import turbodbc  # noqa

    if not isinstance(err, turbodbc.DatabaseError):
        return False

    try:
        msg = str(err.args[0])
    except Exception:
        print(err.args)
        return False

    if "state: HYT00" in msg and "native error code: 0" in msg:
        return True

    print(err.args)

    return False


def is_pymssql_disconnect(err: Exception) -> bool:
    import pymssql  # noqa

    return False


def is_mysql_disconnect(err: Exception) -> bool:
    import MySQLdb  # noqa

    if not isinstance(err, MySQLdb.OperationalError):
        return False

    try:
        code, msg = err.args

        if code == 2013 and "Lost connection to MySQL server" in msg:
            return True
    except Exception:
        print(err.args)

    return False


def is_pymysql_disconnect(err: Exception) -> bool:
    import pymysql  # noqa

    if not isinstance(err, pymysql.err.OperationalError):
        return False

    try:
        code, msg = err.args

        if code == 2013 and "Lost connection to MySQL server" in msg:
            return True
    except Exception:
        print(err.args)

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
