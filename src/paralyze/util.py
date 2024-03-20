import importlib
import inspect
import os.path
import pkgutil
import typing as t
import types


P = t.ParamSpec("P")


def previous_frame_globals(depth: int = 0) -> t.Dict[str, t.Any]:
    """
    Get the globals of the previous frame.
    """
    current = inspect.currentframe()

    for _ in range(depth + 2):
        if current is None:
            raise RuntimeError("Unable to find frame")

        current = current.f_back

    if current is None:
        raise RuntimeError("Unable to find frame")

    return current.f_globals


def find_all_child_modules(
    name: str | None = None,
    path: str | None = None,
) -> t.Iterable[types.ModuleType]:
    """
    Find all child modules of the given module.
    """
    if name is None:
        frame_globals = previous_frame_globals()

        path = os.path.dirname(frame_globals["__file__"])
        name = frame_globals["__name__"]

    if path is None:
        mod = importlib.import_module(name)

        mod_path: t.Union[str, t.List[str], None] = getattr(
            mod,
            "__path__",
            None,
        )

        match mod_path:
            case list():
                mod_path = mod_path[0]
            case str():
                pass
            case None:
                raise ValueError(f"{name} has no __path__")
            case _:
                raise TypeError(
                    f"Unexpected type for __path__: {type(mod_path)}"
                )

        assert isinstance(mod_path, str)

        path = mod_path

    assert path is not None
    ret: t.List[types.ModuleType] = []

    for pkg in pkgutil.walk_packages([path], onerror=lambda _: None):
        if "." in pkg.name:
            continue

        module_name = f"{name}.{pkg.name}"

        mod = importlib.import_module(module_name)
        ret.append(mod)

        if pkg.ispkg:
            for mod_path in mod.__path__:
                ret.extend(
                    find_all_child_modules(
                        mod.__name__,
                        mod_path,
                    )
                )

    return ret


def import_module(*modules: str, parent: str | None = None) -> None:
    """
    Import the given modules.
    """
    if parent is None:
        parent = previous_frame_globals()["__name__"]

    imports: t.List[str] = []

    for mod in modules:
        if mod.startswith("."):
            # relative module
            mod = "{parent}{mod}"

        imports.append(mod)

    for name in imports:
        importlib.import_module(name)
