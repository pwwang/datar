"""Provide options"""

from typing import Any, Mapping, Union
from contextlib import contextmanager

from diot import Diot

# pylint: disable=invalid-name
_key_transform = lambda key: key.replace('_', '.')
_dict_transform_back = lambda dic: {
    key.replace('.', '_'): val
    for key, val in dic.items()
}

OPTIONS = Diot(
    # Whether use 0-based numbers when index is involved, acts similar like R
    # Otherwise, like python
    index_base_0=False,
    dplyr_summarise_inform=True,
    # allow 'a.b' to access 'a_b'
    diot_transform=_key_transform
)

def options(*args: Union[str, Mapping[str, Any]], **kwargs: Any) -> None:
    """Allow the user to set and examine a variety of global options

    Args:
        *args: Names of options to return
        **kwargs: name-value pair to create/set an option
    """
    if not args and not kwargs:
        return OPTIONS.copy()

    names = [arg.replace('.', '_') for arg in args if isinstance(arg, str)]
    pairs = {}
    for arg in args:
        if isinstance(arg, dict):
            pairs.update(_dict_transform_back(arg))
    pairs.update(_dict_transform_back(kwargs))

    out = Diot({
        name: value for name, value in OPTIONS.items()
        if name in names or name in pairs
    }, diot_transform=_key_transform)
    OPTIONS.update(pairs)
    return out

@contextmanager
def options_context(**kwargs: Any) -> None:
    """A context manager to execute code with temporary options

    Note that this is not thread-safe.
    """
    opts = options()
    options(**kwargs)
    yield
    options(opts)

# pylint: disable=invalid-name
def getOption(x: str, default: Any = None) -> Any:
    """Get the current value set for option ‘x’,
    or ‘default’ (which defaults to ‘NULL’) if the option is unset.

    Args:
        x: The name of the option
        default: The default value if `x` is unset
    """
    return OPTIONS.get(x, default)
