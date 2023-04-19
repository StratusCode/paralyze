import typing as t

import orjson

__all__ = (
    "dumps",
    "loads",
)


def _default(obj: t.Any) -> t.Any:
    if hasattr(obj, "to_json"):
        return obj.to_json()

    raise TypeError


def dumps(obj: t.Any) -> bytes:
    return orjson.dumps(obj, default=_default)


def loads(obj: bytes) -> t.Any:
    return orjson.loads(obj)
