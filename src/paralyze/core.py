from concurrent import futures
import contextlib
import functools
import signal
import time
import threading
import typing as t

from paralyze import logging


P = t.ParamSpec("P")
T = t.TypeVar("T")


class Stopping(Exception):
    """
    Raised when the application is stopping but there is no error.

    This is used to stop the application gracefully. See `error_boundary`.
    """


@contextlib.contextmanager
def error_boundary(
    name: str,
    stopping: threading.Event,
    logger: logging.Logger,
    raise_: bool = False,
):
    """
    Context manager that sets the stopping event on exit.

    :param name: The name of the context.
    :param stopping: The stopping event.
    :param logger: The logger to use.
    """
    log = logger.bind(error_boundary=name)

    log.info("start")

    try:
        yield log
    except Exception as err:
        if not stopping.is_set():
            stopping.set()

        match err:
            case Stopping():
                pass
            case _:
                log.exception("error")

        if raise_:
            raise
    finally:
        log.info("stop")


def sleep(stopping: threading.Event, delay: float, step: float = 1.0) -> None:
    """
    Sleep for the given amount of time, or until the stopping event is set.
    """
    amount = delay

    if amount <= 0:
        return

    while amount > 0:
        if stopping.is_set():
            return

        time.sleep(min(step, amount))

        amount -= step


ErrorRet = t.TypeVar("ErrorRet")
ErrorSpec = t.ParamSpec("ErrorSpec")


def stop_on_error(boundary: str):
    def decorator(
        func: t.Callable[
            t.Concatenate[threading.Event, logging.Logger, ErrorSpec],
            ErrorRet,
        ],
    ) -> t.Callable[
        t.Concatenate[threading.Event, ErrorSpec],
        ErrorRet,
    ]:
        @functools.wraps(func)
        def wrapper(
            stopping: threading.Event,
            *args: ErrorSpec.args,
            **kwargs: ErrorSpec.kwargs,
        ) -> ErrorRet:
            logger = logging.get_logger(boundary)

            with error_boundary(boundary, stopping, logger) as log:
                return func(stopping, log, *args, **kwargs)

        return wrapper

    return decorator


def interval(
    stopping: threading.Event,
    interval: float,
    func: t.Callable[P, None],
    *args: P.args,
    **kwargs: P.kwargs,
) -> None:
    """
    Call a function every interval seconds.
    """
    end = time.time()

    func(*args, **kwargs)

    while not stopping.is_set():
        start = time.time()

        sleep_for = max(0.0, interval - (end - start))

        sleep(stopping, sleep_for)

        func(*args, **kwargs)

        end = time.time()


def install_signals(stopping: threading.Event, logger: logging.Logger) -> None:
    def shutdown(num: int, stopping: threading.Event) -> None:
        if stopping.is_set():
            return

        logger.info("shutdown.signal", num=num)

        stopping.set()

    signal.signal(signal.SIGTERM, lambda sig, _: shutdown(sig, stopping))
    signal.signal(signal.SIGINT, lambda sig, _: shutdown(sig, stopping))


def wait(
    stopping: threading.Event,
    event: threading.Event,
    timeout: float | None = None
) -> bool:
    """
    Wait for the given event, or until the stopping event is set.
    """
    start = time.time()

    while not stopping.is_set():
        if event.wait(5.0):
            return True

        end = time.time()

        if timeout is not None and end - start > timeout:
            return False

    return False


class ThreadPoolExecutor(futures.ThreadPoolExecutor):
    """
    Wraps each submitted function in an error boundary.
    """
    stopping: threading.Event
    log: logging.Logger

    def __init__(
        self,
        stopping: threading.Event,
        log: logging.Logger,
        *args: t.Any,
        **kwargs: t.Any,
    ) -> None:
        super().__init__(*args, **kwargs)

        self.stopping = stopping
        self.log = log

    def submit(  # type: ignore[override]
        self,
        fn: t.Callable[P, T],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> "futures.Future[T]":
        @functools.wraps(fn)
        def wrap() -> T:
            if self.stopping.is_set():
                raise Stopping

            try:
                ret = fn(*args, **kwargs)
            except Stopping:
                raise
            except Exception:
                self.stopping.set()
                self.log.exception("error")

                raise
            else:
                if self.stopping.is_set():
                    raise Stopping

                return ret

        return super().submit(wrap)
