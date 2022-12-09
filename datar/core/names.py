"""Name repairing"""
import inspect
import re
import keyword
import math
from numbers import Number
from typing import Any, Callable, List, Union, Iterable, Tuple

from .utils import logger


class NameNonUniqueError(ValueError):
    """Error for non-unique names"""


def _isnan(x: Any) -> bool:
    """Check if x is nan"""
    return isinstance(x, Number) and math.isnan(x)


def _is_scalar(x: Any) -> bool:
    """Check if x is scalar"""
    if isinstance(x, str):  # pragma: no cover
        return True
    try:
        iter(x)
    except TypeError:
        return True
    return False


def _log_changed_names(changed_names: List[Tuple[str, str]]) -> None:
    """Log the changed names"""
    if not changed_names:
        return

    logger.warning("New names:")
    for orig_name, new_name in changed_names:
        logger.warning("* %r -> %r", orig_name, new_name)


def _repair_names_minimal(names: Iterable[str]) -> List[str]:
    """Minimal repairing"""
    return ["" if name is None or _isnan(name) else str(name) for name in names]


def _repair_names_unique(
    names: Iterable[str],
    quiet: bool = False,
    sanitizer: Callable = None,
) -> List[str]:
    """Make sure names are unique"""
    min_names = _repair_names_minimal(names)
    neat_names = [
        re.sub(r"(?:(?<!_)_{1,2}\d+|(?<!_)__)+$", "", name)
        for name in min_names
    ]
    if callable(sanitizer):
        neat_names = [sanitizer(name) for name in neat_names]

    new_names = []
    changed_names = []
    for i, name in enumerate(names):
        neat_name = neat_names[i]
        if neat_names.count(neat_name) > 1 or neat_name == "":
            neat_name = f"{neat_name}__{i}"
        if neat_name != name:
            changed_names.append((name, neat_name))
        new_names.append(neat_name)
    if not quiet:
        _log_changed_names(changed_names)
    return new_names


def _repair_names_universal(
    names: Iterable[str],
    quiet: bool = False,
) -> List[str]:
    """Make sure names are safely to be used as variable or attribute"""
    min_names = _repair_names_minimal(names)
    neat_names = [re.sub(r"[^\w]", "_", name) for name in min_names]
    new_names = _repair_names_unique(
        neat_names,
        quiet=True,
        sanitizer=lambda name: (
            f"_{name}"
            if keyword.iskeyword(name) or (name and name[0].isdigit())
            else name
        ),
    )
    if not quiet:
        changed_names = [
            (orig_name, new_name)
            for orig_name, new_name in zip(names, new_names)
            if orig_name != new_name
        ]
        _log_changed_names(changed_names)
    return new_names


def _repair_names_check_unique(names: Iterable[str]) -> Iterable[str]:
    """Just check the uniqueness"""
    for name in names:
        if names.count(name) > 1:
            raise NameNonUniqueError(f"Names must be unique: {name}")
        if name == "" or _isnan(name):
            raise NameNonUniqueError(f"Names can't be empty: {name}")
        if re.search(r"(?:(?<!_)_{2}\d+|(?<!_)__)+$", str(name)):
            raise NameNonUniqueError(
                f"Names can't be of the form `__` or `_j`: {name}"
            )
    return names


BUILTIN_REPAIR_METHODS = dict(
    minimal=_repair_names_minimal,
    unique=_repair_names_unique,
    universal=_repair_names_universal,
    check_unique=_repair_names_check_unique,
)


def repair_names(
    names: Iterable[str],
    repair: Union[str, Callable],
) -> List[str]:
    """Repair names based on the method

    Args:
        names: The names to be repaired
        repair: The method to repair
            - `minimal`: Minimal names are never None or NA.
                When an element doesn't have a name, its minimal name
                is an empty string.
            - `unique`: Unique names are unique. A suffix is appended to
                duplicate names to make them unique.
            - `universal`: Universal names are unique and syntactic,
                meaning that you can safely use the names as variables without
                causing a syntax error (like `f.<name>`).
            - A function, accepts either a list of names or a single name.
                Function accepts a list of names must annotate the first
                argument with `typing.Iterable` or `typing.Sequence`.

    Examples:
        >>> repair_names([None]*3, repair="minimal")
        >>> # ["", "", ""]
        >>> repair_names(["x", NA], repair="minimal")
        >>> # ["x", ""]
        >>> repair_names(["", "x", "", "y", "x", "_2", "__"], repair="unique")
        >>> # ["__1", "x__2", "__3", "y", "x__5", "__6", "__7"]
        >>> repair_names(["", "x", NA, "x"], repair="universal")
        >>> # ["__1", "x__2", "__3", "x__4"]
        >>> repair_names(["(y)"  "_z"  ".2fa"  "False"], repair="universal")
        >>> # ["_y_", "_z", "_2fa", "_False"]

    Returns:
        The repaired names

    Raises:
        ValueError: when repair is not a string or callable
        NameNonUniqueError: when check_unique fails
    """
    if isinstance(repair, str):
        repair = BUILTIN_REPAIR_METHODS[repair]  # type: ignore
    elif (
        not _is_scalar(repair)
        and all(isinstance(elem, str) for elem in repair)
    ):
        return repair  # type: ignore
    elif not callable(repair):
        raise ValueError("Expect a function for name repairing.")

    parameters = inspect.signature(repair).parameters  # type: ignore
    annotation = list(parameters.values())[0].annotation
    if annotation is inspect._empty or annotation._name not in (
        "Iterable",
        "Sequence",
    ):  # scalar input
        return [repair(name) for name in names]

    return repair(names)
