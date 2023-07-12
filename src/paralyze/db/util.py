import functools
import time
import typing as t

from sqlalchemy import exc

try:
    import turbodbc
except ImportError:
    turbodbc = None

try:
    import pymssql
except ImportError:
    pymssql = None

try:
    import MySQLdb
except ImportError:
    MySQLdb = None

try:
    import pymysql
except ImportError:
    pymysql = None

__all__ = (
    "retry_on_disconnect",
)


P = t.ParamSpec("P")
R = t.TypeVar("R")


def unwrap_sqlalchemy_error(err: BaseException) -> BaseException | None:
    if isinstance(err, exc.DBAPIError):
        return t.cast(Exception, err.orig)

    return err


def is_disconnect(err: BaseException) -> bool:
    my_err = unwrap_sqlalchemy_error(err)

    if my_err is None:
        return False

    if is_turbodbc_disconnect(my_err):
        return True

    if is_pymssql_disconnect(my_err):
        return True

    if is_mysql_disconnect(my_err):
        return True

    if is_pymysql_disconnect(my_err):
        return True

    return False


def is_turbodbc_disconnect(err: BaseException) -> bool:
    if not turbodbc:
        return False

    if not isinstance(err, turbodbc.DatabaseError):
        return False

    try:
        msg = str(err.args[0])
    except BaseException:
        print(err.args)
        return False

    if "state: HYT00" in msg and "native error code: 0" in msg:
        return True

    print(err.args)

    return False


def is_pymssql_disconnect(err: BaseException) -> bool:
    if not pymssql:
        return False

    return False


def is_mysql_disconnect(err: BaseException) -> bool:
    if not MySQLdb:
        return False

    if not isinstance(err, MySQLdb.OperationalError):
        return False

    try:
        code, msg = err.args

        if code == 2013 and "Lost connection to MySQL server" in msg:
            return True
    except BaseException:
        print(err.args)

    return False


def is_pymysql_disconnect(err: BaseException) -> bool:
    if not pymysql:
        return False

    if not isinstance(err, pymysql.err.OperationalError):
        return False

    try:
        code, msg = err.args

        if code == 2013 and "Lost connection to MySQL server" in msg:
            return True
    except BaseException:
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
            err: BaseException | None = None

            while count < max_retries:
                try:
                    return func(*args, **kwargs)
                except BaseException as exc:
                    print(type(exc), exc, exc.__dict__)

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
