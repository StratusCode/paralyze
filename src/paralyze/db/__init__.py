import contextlib
import threading
import typing as t

from sqlalchemy import engine, orm

from paralyze import core
from paralyze.db import util

__all__ = (
    "make_session_class",
    "with_transaction",
    "validate_engine",
)


@t.overload
def make_session_class(
    bind: engine.Engine,
    scoped: t.Literal[False] = False,
) -> "orm.sessionmaker[orm.Session]":
    ...


@t.overload
def make_session_class(
    bind: engine.Engine,
    scoped: t.Literal[True],
) -> "orm.scoped_session[orm.Session]":
    ...


def make_session_class(bind: engine.Engine, scoped: bool = False):
    ret = orm.sessionmaker(bind=bind)

    if scoped:
        return orm.scoped_session(ret)

    return ret


@contextlib.contextmanager
def with_transaction(session: orm.Session, stopping: threading.Event):
    """
    Context manager that emits a ROLLBACK if the stopping event is set.
    """
    with session.begin() as transaction:
        try:
            yield transaction
        except Exception:
            if not stopping.is_set():
                stopping.set()

            raise

        if stopping.is_set():
            transaction.rollback()

            raise core.Stopping()


@util.retry_on_disconnect(delay=1, max_retries=5)
def validate_engine(eng: engine.Engine) -> None:
    eng.connect()
