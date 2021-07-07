"""Name repairing"""
import inspect
import re
import keyword
from typing import Callable, List, Sequence, Union, Iterable, Tuple

import numpy

from .exceptions import NameNonUniqueError
from .types import is_iterable


def _log_changed_names(changed_names: Iterable[Tuple[str, str]]) -> None:
    """Log the changed names"""
    if not changed_names:
        return
    from .utils import logger

    logger.warning("New names:")
    for orig_name, new_name in changed_names:
        logger.warning("* %r -> %r", orig_name, new_name)


def _repair_names_minimal(names: Iterable[str]) -> List[str]:
    """Minimal repairing"""
    return ["" if name in (None, numpy.nan) else str(name) for name in names]


def _repair_names_unique(
    names: Sequence[str],
    quiet: bool = False,
    sanitizer: Callable = None,
    base0_: bool = None,
) -> List[str]:
    """Make sure names are unique"""
    base = int(not base0_)
    min_names = _repair_names_minimal(names)
    neat_names = [
        re.sub(r"(?:(?<!_)_{1,2}\d+|(?<!_)__)+$", "", name)
        for name in min_names
    ]
    if callable(sanitizer):
        neat_names = [sanitizer(name) for name in neat_names]

    new_names = []
    changed_names = []
    for i, name in enumerate(neat_names):
        if neat_names.count(name) > 1 or name == "":
            name = f"{name}__{i + base}"
        if name != names[i]:
            changed_names.append((names[i], name))
        new_names.append(name)
    if not quiet:
        _log_changed_names(changed_names)
    return new_names


def _repair_names_universal(
    names: Iterable[str], quiet: bool = False, base0_: bool = None
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
        base0_=base0_,
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
        if name == "" or name is numpy.nan:
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
    base0_: bool = None,
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
        base0_: Whether the numeric suffix starts from 0 or not.
            If not specified, will use `datar.base.get_option('index.base.0')`.

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
    from .utils import get_option

    base0_ = get_option("index.base.0", base0_)
    if isinstance(repair, str):
        repair = BUILTIN_REPAIR_METHODS[repair] # type: ignore
    elif is_iterable(repair) and all(isinstance(elem, str) for elem in repair):
        return repair # type: ignore
    elif not callable(repair):
        raise ValueError("Expect a function for name repairing.")

    parameters = inspect.signature(repair).parameters # type: ignore
    annotation = list(parameters.values())[0].annotation
    if annotation is inspect._empty or annotation._name not in (
        "Iterable",
        "Sequence",
    ):  # scalar input
        return [
            repair(name, base0_=base0_) # type: ignore[operator]
            if "base0_" in parameters
            else repair(name) # type: ignore[operator]
            for name in names
        ]

    names = list(names)
    return (
        repair(names, base0_=base0_) # type: ignore[operator]
        if "base0_" in parameters
        else repair(names) # type: ignore[operator]
    )
