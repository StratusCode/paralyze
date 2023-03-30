import contextlib
import threading

from sqlalchemy import engine, orm

from paralyze import core
from paralyze.db import util

__all__ = (
    "make_session_class",
    "with_transaction",
    "validate_engine",
)


def make_session_class(bind: engine.Engine) -> "orm.sessionmaker[orm.Session]":
    return orm.sessionmaker(bind=bind)


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
