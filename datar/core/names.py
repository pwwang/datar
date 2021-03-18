"""Name repairing"""
import inspect
import re
import keyword
from typing import Callable, List, Optional, Union, Iterable

import numpy

class NameNonUniqueError(Exception):
    """When check_unique fails"""

def _log_changed_names(changed_names: Iterable[str]) -> None:
    if not changed_names:
        return
    from .utils import logger
    logger.warning('New names:')
    for orig_name, new_name in changed_names:
        logger.warning('* %r -> %r', orig_name, new_name)

def _repair_names_minimal(names: Iterable[str]) -> List[str]:
    return ["" if name in (None, numpy.nan) else str(name) for name in names]

def _repair_names_unique(
        names: Iterable[str],
        quiet: bool = False,
        sanitizer: Optional[Callable[[str], str]] = None
) -> List[str]:
    min_names = _repair_names_minimal(names)
    neat_names = [
        re.sub(r'(?:(?<!_)_{1,2}\d+|(?<!_)__)+$', '', name)
        for name in min_names
    ]
    if callable(sanitizer):
        neat_names = [sanitizer(name) for name in neat_names]

    new_names = []
    changed_names = []
    for i, name in enumerate(neat_names):
        if neat_names.count(name) > 1 or name == '':
            name = f'{name}__{i}'
        if name != names[i]:
            changed_names.append((names[i], name))
        new_names.append(name)
    if not quiet:
        _log_changed_names(changed_names)
    return new_names

def _repair_names_universal(
        names: Iterable[str],
        quiet: bool = False
) -> List[str]:
    min_names = _repair_names_minimal(names)
    neat_names = [re.sub(r'[^\w]', '_', name) for name in min_names]
    new_names = _repair_names_unique(
        neat_names,
        quiet=True,
        sanitizer=lambda name: (
            f'_{name}'
            if keyword.iskeyword(name) or (name and name[0].isdigit())
            else name
        )
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
    for name in names:
        if names.count(name) > 1:
            raise NameNonUniqueError(f"Names must be unique: {name}")
        if name == "" or name is numpy.nan:
            raise NameNonUniqueError(f"Names can't be empty: {name}")
        if re.search(r'(?:(?<!_)_{1,2}\d+|(?<!_)__)+$', name):
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

def repair_names(names: str, repair: Union[str, Callable]) -> List[str]:
    """Repair names based on the method"""
    if isinstance(repair, str):
        repair = BUILTIN_REPAIR_METHODS[repair]
    elif not callable(repair):
        raise ValueError('Expect a function for name repairing.')

    parameters = inspect.signature(repair).parameters
    annotation = list(parameters.values())[0].annotation
    if (
            annotation is inspect._empty or
            annotation._name not in ('Iterable', 'Sequence')
    ): # scalar input
        return [repair(name) for name in names]

    return repair(names)
