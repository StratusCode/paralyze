from dataclasses import dataclass
import sys
import threading
import typing as t

from paralyze import logging, metrics, core


Config = t.TypeVar("Config")


@dataclass(frozen=True, kw_only=True)
class Context(t.Generic[Config]):
    stopping: threading.Event
    log: logging.Logger

    cfg: Config

    metrics: metrics.Client

    parent: "Context[Config] | None" = None
    root: "Context[Config] | None" = None

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
        return core.ThreadPoolExecutor(
            self.stopping,
            self.log,
            max_workers=max_workers,
        )

    def maybe_stop(self) -> None:
        if not self.stopping.is_set():
            return

        _, obj, tb = sys.exc_info()

        if obj is None:
            raise core.Stopping

        # if we get here, we're in an exception handler
        # and we're going to raise the exception again
        # so that it can be handled by the caller
        raise obj.with_traceback(tb)

    def wait_event(
        self,
        event: threading.Event,
        timeout: float | None = None,
    ) -> bool:
        try:
            return core.wait(self.stopping, event, timeout)
        finally:
            self.maybe_stop()

    def sleep(self, timeout: float, step: float = 1.0) -> None:
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
