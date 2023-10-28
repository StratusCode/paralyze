import time
import threading
import typing as t
import weakref

from google.cloud import monitoring_v3
from google.api_core import exceptions as exc

from paralyze import logging, core
from paralyze.config import monitoring as config


P = t.ParamSpec("P")
PointTypes = t.Union[
    t.Type[str],
    t.Type[int],
    t.Type[float],
    t.Type[bool],
]

MetricValue = t.Union[str, bool, int, float]
PointType = t.TypeVar("PointType", bound=MetricValue)


METRIC_PREFIX = "custom.googleapis.com/"


logger = logging.get_logger(__name__)


class Point(t.Generic[PointType]):
    """
    A builder for points.
    """

    _value: PointType
    _start: int | None
    _end: int | None

    def __init__(self, value: PointType):
        self._value = value
        self._start = None
        self._end = None

    def interval(
        self,
        start: int | None = None,
        end: int | None = None,
    ) -> t.Self:
        """
        Set the interval for this point.
        """
        if start is not None:
            self._start = start

        if end is not None:
            self._end = end

        return self

    def start(self, start: int | None = None) -> t.Self:
        """
        Set the start time for this point.
        """
        if start is None:
            start = time.time_ns()

        self._start = start

        return self

    def end(self, end: int | None = None) -> t.Self:
        """
        Set the end time for this point.
        """
        if end is None:
            end = time.time_ns()

        self._end = end

        return self

    def to_value(self) -> t.Dict[str, t.Any]:
        """
        Returns a dict that can be used as the value for a point.
        """
        ret: t.Dict[str, t.Any] = {}

        match self._value:
            case str():
                ret["string_value"] = self._value
            case bool():
                ret["bool_value"] = self._value
            case int():
                ret["int64_value"] = self._value
            case float():
                ret["double_value"] = self._value
            case _:
                # mypy does not seem to recognize that the value is not Never.
                t.assert_never(self._value)  # type: ignore

        return ret

    def to_interval(self) -> t.Dict[str, t.Any]:
        """
        Returns a dict that can be used as the interval for a point.
        """
        ret: t.Dict[str, t.Any] = {}

        if self._start is not None:
            ret["start_time"] = to_timestamp(self._start)

        if self._end is not None:
            ret["end_time"] = to_timestamp(self._end)

        return ret

    def build(self) -> monitoring_v3.Point:
        """
        Build a point object.
        """
        return monitoring_v3.Point(
            {
                "interval": monitoring_v3.TimeInterval(self.to_interval()),
                "value": monitoring_v3.TypedValue(self.to_value()),
            }
        )


class BaseBuilder:
    _client: "Client"
    _metric_type: str

    def __init__(self, client: "Client", metric_type: str):
        self._client = client
        self._metric_type = metric_type


ResourceKey = t.Literal[
    "project_id",
    "location",
    "namespace",
    "job",
    "task_id",
]


class TimeSeries(BaseBuilder, t.Generic[PointType]):
    """
    A builder for time series.
    """

    _metric_labels: t.Dict[str, MetricValue]
    _resource_type: str = "generic_task"
    _resource_labels: t.Dict[ResourceKey, str]
    _points: t.List[Point[PointType]]
    _lock: threading.RLock

    def __init__(self, client: "Client", metric_type: str) -> None:
        super().__init__(client, metric_type)

        self._lock = threading.RLock()

        self._metric_labels = {}
        self._resource_labels = {}
        self._points = []

    def metric_label(self, key: str, value: MetricValue) -> t.Self:
        with self._lock:
            self._metric_labels[key] = value

        return self

    def metric_labels(
        self,
        labels: t.Dict[str, MetricValue],
    ) -> t.Self:
        with self._lock:
            self._metric_labels = labels

        return self

    def resource_label(self, key: ResourceKey, value: str) -> t.Self:
        with self._lock:
            self._resource_labels[key] = value

        return self

    def resource_labels(
        self,
        labels: t.Dict[ResourceKey, str],
    ) -> t.Self:
        with self._lock:
            self._resource_labels = labels.copy()

        return self

    def point(
        self,
        point: PointType,
        start: int | None = None,
        end: int | None = None,
    ) -> t.Self:
        """
        Add a point to this time series.
        """
        with self._lock:
            self._points.append(Point(point).interval(start, end))

        return self

    def points(self, points: t.List[monitoring_v3.Point]) -> t.Self:
        """
        Add multiple points to this time series.
        """
        with self._lock:
            self._points.extend(points)

        return self

    def build(self) -> monitoring_v3.TimeSeries | None:
        """
        Build a time series object.

        This will clear the points that have been added to this time series.
        """
        with self._lock:
            points, self._points = self._points, []

            if not points:
                return None

            ret = monitoring_v3.TimeSeries()

            ret.resource.type = self._resource_type

            if self._resource_labels:
                for r_key, r_value in self._resource_labels.items():
                    ret.resource.labels[r_key] = r_value

            ret.metric.type = self._metric_type

            for metric_key, metric_value in self._metric_labels.items():
                ret.metric.labels[metric_key] = metric_value

            ret.points = [p.build() for p in points]

            return ret

    def stop(self) -> None:
        """
        Stop this time series.
        """
        self._client.close(self)


class Counter(TimeSeries[int]):
    """
    A counter metric.
    """

    value: int

    def __init__(
        self,
        client: "Client",
        metric_type: str,
        initial_value: int = 0,
    ) -> None:
        super().__init__(client, metric_type)

        self.value = initial_value

    def inc(self, delta: int = 1) -> t.Self:
        """
        Increment this counter.
        """
        self.value += delta

        return self

    def dec(self, delta: int = 1) -> t.Self:
        """
        Decrement this counter.
        """
        self.value -= delta

        return self

    def build(self) -> monitoring_v3.TimeSeries | None:
        """
        Build a time series object.

        This will reset the counter to 0.
        """
        with self._lock:
            val, self.value = self.value, 0

            self.point(val, end=time.time_ns())

            return super().build()


class Gauge(TimeSeries[int]):
    """
    A gauge metric.
    """

    values: t.List[int]

    def __init__(
        self,
        client: "Client",
        metric_type: str,
    ) -> None:
        super().__init__(client, metric_type)

        self.values = []

    def set(self, value: int) -> t.Self:
        """
        Set the value of this gauge.
        """
        self.values.append(value)

        return self

    def build(self) -> monitoring_v3.TimeSeries | None:
        """
        Build a time series object.

        This will reset the gauge to 0.
        """
        with self._lock:
            values, self.values = self.values, []

            # take the average of the values
            value = 0

            if len(values) > 0:
                value = sum(values) // len(values)

            self.point(
                value,
                end=time.time_ns(),
            )

            return super().build()


class Client:
    _client: monitoring_v3.MetricServiceClient
    _project: str | None
    _time_series: "weakref.WeakSet[TimeSeries]"
    _metrics_prefix: str

    _lock: threading.Lock
    _thread: threading.Thread
    _stopping: threading.Event

    _task: t.Dict[ResourceKey, str] | None

    def __init__(
        self,
        stopping: threading.Event,
        client: monitoring_v3.MetricServiceClient,
        project: str | None,
        metrics_prefix: str,
    ) -> None:
        self._client = client

        self._project = (
            None
            if project is None
            else monitoring_v3.MetricServiceClient.common_project_path(project)
        )

        self._lock = threading.Lock()
        self._thread = threading.Thread(target=self._run)
        self._stopping = stopping
        self._metrics_prefix = metrics_prefix

        self._time_series = weakref.WeakSet()

        self._task = None

    def _report_all_metrics(self) -> None:
        with self._lock:
            all_ts = [ts.build() for ts in self._time_series]
            ts_with_points = [ts for ts in all_ts if ts is not None]

        if not self._project:
            return

        for i in range(0, len(ts_with_points), 200):
            batch = ts_with_points[i : i + 200]  # noqa: E203

            created = False
            count = 0

            while not created:
                try:
                    self._client.create_time_series(
                        {
                            "name": self._project,
                            "time_series": batch,
                        }
                    )
                    created = True
                except (exc.InternalServerError, exc.DeadlineExceeded):
                    count += 1

                    if count > 5:
                        raise

                    core.sleep(self._stopping, 1.0)

                    continue

    def _run(self) -> None:
        with core.error_boundary(
            "metrics.time-series.exporter",
            self._stopping,
            logger,
        ):
            while not self._stopping.is_set():
                start = time.time()

                self._report_all_metrics()

                # attempt to send every 30 seconds
                end = time.time()
                sleep_for = max(0.0, 30.0 - (end - start))

                core.sleep(self._stopping, sleep_for)

            # send any remaining metrics
            self._report_all_metrics()

    def task(
        self,
        project_id: str,
        location: str,
        namespace: str,
        job: str,
        task_id: str,
    ) -> t.Self:
        """
        Set the task for this client.
        """
        self._task = {
            "project_id": project_id,
            "location": location,
            "namespace": namespace,
            "job": job,
            "task_id": task_id,
        }

        return self

    @t.overload
    def time_series(
        self,
        metric_name: str,
        point_type: t.Type[str],
    ) -> TimeSeries[str]:
        ...

    @t.overload
    def time_series(  # type: ignore
        self,
        metric_name: str,
        point_type: t.Type[bool],
    ) -> TimeSeries[bool]:
        ...

    @t.overload
    def time_series(
        self,
        metric_name: str,
        point_type: t.Type[int],
    ) -> TimeSeries[int]:
        ...

    @t.overload
    def time_series(
        self,
        metric_name: str,
        point_type: t.Type[float],
    ) -> TimeSeries[float]:
        ...

    def time_series(
        self,
        metric_name: str,
        point_type: PointTypes,
    ):
        """
        Create a new time series.
        """
        metrics_name = self._metrics_prefix + "/" + metric_name.lstrip("/")

        with self._lock:
            ret: TimeSeries

            if point_type not in (str, bool, int, float):
                raise AssertionError(f"unknown point type: {point_type}")

            ret = TimeSeries(self, metrics_name)

            if self._task is not None:
                ret = ret.resource_labels(self._task)

            self._time_series.add(ret)

            return ret

    def counter(self, metric_name: str, initial_value: int = 0) -> Counter:
        """
        Create a new counter.
        """
        metric_name = self._metrics_prefix + "/" + metric_name.lstrip("/")

        with self._lock:
            ret = Counter(self, metric_name, initial_value)

            if self._task is not None:
                ret = ret.resource_labels(self._task)

            self._time_series.add(ret)

            return ret

    def gauge(self, metric_name: str) -> Gauge:
        """
        Create a new gauge.
        """
        metric_name = self._metrics_prefix + "/" + metric_name.lstrip("/")

        with self._lock:
            ret = Gauge(self, metric_name)

            if self._task is not None:
                ret = ret.resource_labels(self._task)

            self._time_series.add(ret)

        return ret

    def start(self) -> None:
        logger.debug("metrics.start")
        self._thread.start()

    def join(self) -> None:
        self._thread.join()

    def run(self):
        self.start()
        self.join()

    def stop(self) -> None:
        self._stopping.set()

        self.join()

    def __enter__(self) -> "Client":
        self.start()

        return self

    def __exit__(self, *_: t.Any) -> None:
        try:
            self.stop()
        except BaseException:
            logger.exception("metrics.stop")

    def close(self, ts: TimeSeries) -> None:
        with self._lock:
            if ts not in self._time_series:
                return

            self._time_series.remove(ts)


class Timestamp(t.TypedDict):
    seconds: int
    nanos: int


def to_timestamp(now: int | None = None) -> Timestamp:
    """
    :param now: The time in nanoseconds to convert to a timestamp. If not
        provided, the current time will be used. Use `time.time_ns()` to get
        the current time in nanoseconds.
    """
    if now is None:
        now = time.time_ns()

    seconds = now // (10**9)

    return {
        "seconds": seconds,
        "nanos": ((now // (10**9)) - seconds),
    }


def client(
    stopping: threading.Event,
    cfg: config.Config | None,
) -> Client:
    client = monitoring_v3.MetricServiceClient()

    metrics = Client(
        stopping,
        client,
        cfg.project_id if cfg is not None else None,
        METRIC_PREFIX + (cfg.prefix if cfg is not None else "").lstrip("/"),
    )

    if cfg and cfg.task:
        metrics = metrics.task(
            cfg.task.project_id,
            cfg.task.location,
            cfg.task.namespace,
            cfg.task.job,
            cfg.task.task_id,
        )

    return metrics
