import typing as t

from msgspec import json

__all__ = (
    "dumps",
    "loads",
)


def enc_hook(obj: t.Any) -> t.Any:
    if hasattr(obj, "to_json"):
        return obj.to_json()

    raise NotImplementedError(f"Cannot serialize {obj!r}")


def dumps(obj: t.Any) -> bytes:
    return json.encode(obj, enc_hook=enc_hook)


def loads(obj: bytes) -> t.Any:
    return json.decode(obj)
