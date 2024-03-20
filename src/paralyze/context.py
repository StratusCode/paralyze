from dataclasses import dataclass
import random
import sys
import threading
import typing as t

from paralyze import logging, metrics, core


Config = t.TypeVar("Config")
P = t.ParamSpec("P")
T = t.TypeVar("T")


@dataclass(frozen=True, kw_only=True)
class Context(t.Generic[Config]):
    # the stopping event, if this is set, the application should stop as soon
    # as possible
    stopping: threading.Event
    # the logger attached to this context
    log: logging.Logger

    # the configuration for this context
    cfg: Config

    # the metrics client attached to this context
    metrics: metrics.Client

    # the parent context, if any
    parent: "Context[Config] | None" = None
    # the root context, if any
    root: "Context[Config] | None" = None

    __slots__ = (
        "cfg",
        "log",
        "metrics",
        "parent",
        "root",
        "stopping",
    )

    def bind(
        self,
        logger: logging.Logger | None = None,
    ) -> "Context[Config]":
        return Context(
            cfg=self.cfg,
            log=logger or self.log,
            metrics=self.metrics,
            parent=self,
            root=self.root or self,
            stopping=self.stopping,
        )

    def thread_pool(
        self,
        max_workers: int | None = None,
    ) -> core.ThreadPoolExecutor:
        """
        Create a ThreadPoolExecutor that is aware of this context.
        """
        return core.ThreadPoolExecutor(
            self.stopping,
            self.log,
            max_workers=max_workers,
        )

    def maybe_stop(self) -> None:
        """
        If the stopping event is set, raise a Stopping exception, unless there
        is an exception already being raised.
        """
        if not self.stopping.is_set():
            return

        _, obj, tb = sys.exc_info()

        if obj is None:
            raise core.Stopping

        # if we get here, we're in an exception handler and we're going to
        # raise the exception again so that it can be handled by the caller
        raise obj.with_traceback(tb)

    def wait_event(
        self,
        event: threading.Event,
        timeout: float | None = None,
        step: float = 5.0,
    ) -> bool:
        """
        Wait for the event to be set, or for the stopping event to be set.

        If the stopping event is set, raise a Stopping exception.

        :param event: The event to wait for.
        :param timeout: The number of seconds to wait for the event to be set.
        :param interval: The number of seconds to actually sleep between wakeups
            to check for the event.
        """
        try:
            return core.wait(
                self.stopping,
                event,
                timeout=timeout,
                interval=step,
            )
        finally:
            self.maybe_stop()

    def sleep(self, timeout: float, step: float = 1.0) -> None:
        """
        Sleep for the given number of seconds, or until the stopping event is
        set.

        If the stopping event is set, raise a Stopping exception.

        :param timeout: The number of seconds to sleep.
        :param step: The number of seconds to actually sleep between wakeups
            to check for the stopping event.
        """
        try:
            core.sleep(self.stopping, timeout, step)
        finally:
            self.maybe_stop()

    def interval(
        self,
        interval: float,
        func: t.Callable[core.P, None],
        *args: core.P.args,
        **kwargs: core.P.kwargs,
    ) -> t.Never:
        core.interval(
            self.stopping,
            interval,
            func,
            *args,
            **kwargs,
        )

    def retry(self, func: t.Callable[P, T]) -> "Retryable[Config, P, T]":
        """
        Retry the supplied function if it raises an exception.
        """
        return Retryable(self, func)


class Retryable(t.Generic[Config, P, T]):
    """
    A builder pattern for retrying a function.

    Retries the function if it raises an exception that is handled by the error
    handler.
    """

    _ctx: Context[Config]
    _func: t.Callable[P, T]

    _max_retries: int
    _backoff: float | t.Callable[[], float]
    _jitter: bool
    _error_handler: t.Callable[[BaseException], bool]

    def __init__(
        self,
        ctx: Context[Config],
        func: t.Callable[P, T],
    ) -> None:
        self._ctx = ctx
        self._func = func

        self._max_retries = 3
        self._backoff = 0.1
        self._jitter = True
        # by default, we retry on any exception
        self._error_handler = lambda err: True

    @t.overload
    def func(self) -> t.Callable[P, T]:
        """
        Get the underlying function.
        """
        ...

    @t.overload
    def func(self, func: t.Callable[P, T]) -> "t.Self":
        """
        Set the underlying function.
        """
        ...

    def func(self, func: t.Callable[P, T] | None = None):
        if func is None:
            return self._func

        self._func = func

        return self

    @t.overload
    def max_retries(self) -> int:
        """
        Get the maximum number of retries.
        """
        ...

    @t.overload
    def max_retries(self, max_retries: int) -> "t.Self":
        """
        Set the maximum number of retries.
        """
        ...

    def max_retries(self, max_retries: int | None = None):
        if max_retries is None:
            return self._max_retries

        self._max_retries = max_retries

        return self

    @t.overload
    def backoff(self) -> float | t.Callable[[], float]:
        """
        Get the backoff delay.
        """
        ...

    @t.overload
    def backoff(self, delay: float | t.Callable[[], float]) -> "t.Self":
        """
        Set the backoff delay.
        """
        ...

    def backoff(self, delay: float | t.Callable[[], float] | None = None):
        if delay is None:
            return self._backoff

        self._backoff = delay

        return self

    @t.overload
    def jitter(self) -> bool:
        """
        Get whether or not to add jitter to the backoff delay.
        """
        ...

    @t.overload
    def jitter(self, jitter: bool) -> "t.Self":
        """
        Set whether or not to add jitter to the backoff delay.
        """
        ...

    def jitter(self, jitter: bool | None = None):
        if jitter is None:
            return self._jitter

        self._jitter = jitter

        return self

    @t.overload
    def error_handler(self) -> t.Callable[[BaseException], bool]:
        """
        Get the error handler.
        """
        ...

    @t.overload
    def error_handler(
        self,
        error_handler: t.Callable[[BaseException], bool],
    ) -> "t.Self":
        """
        Set the error handler.
        """
        ...

    def error_handler(
        self,
        error_handler: t.Callable[[BaseException], bool] | None = None,
    ):
        if error_handler is None:
            return self._error_handler

        self._error_handler = error_handler

        return self

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        count = 0
        err: BaseException | None = None

        while count < self._max_retries:
            self._ctx.maybe_stop()

            try:
                return self._func(*args, **kwargs)
            except BaseException as exc:
                if not self._error_handler(exc):
                    raise exc from exc

                err = exc
                wait = self._backoff

                if callable(wait):
                    wait = wait()

                if self._jitter:
                    wait = random.uniform(0, wait)

                self._ctx.sleep(wait)

                count += 1

        assert err is not None

        # if we get here, we've exhausted all retries
        raise err from err
