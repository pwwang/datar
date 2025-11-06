from typing import Any as _Any, Callable as _Callable

from pipda import register_verb as _register_verb
from .core.verb_env import get_verb_ast_fallback as _get_verb_ast_fallback

from .core.load_plugins import plugin as _plugin

locals().update(_plugin.hooks.misc_api())


@_register_verb(object, ast_fallback=_get_verb_ast_fallback("pipe"))
def pipe(data: _Any, func: _Callable, *args, **kwargs) -> _Any:
    """Apply a function to the data

    This function is similar to pandas.DataFrame.pipe() and allows you to
    apply custom functions in a piping workflow. Works with any data type.

    Args:
        data: The data object (can be any type)
        func: Function to apply to the data. ``args`` and ``kwargs`` are
            passed into ``func``.
        *args: Positional arguments passed into ``func``
        **kwargs: Keyword arguments passed into ``func``

    Returns:
        The return value of ``func``

    Examples:
        >>> import datar.all as dr
        >>> # Works with lists
        >>> [1, 2, 3] >> dr.pipe(lambda x: [i * 2 for i in x])
        [2, 4, 6]

        >>> # Works with dicts
        >>> data = {'a': 1, 'b': 2}
        >>> data >> dr.pipe(lambda x: {k: v * 2 for k, v in x.items()})
        {'a': 2, 'b': 4}

        >>> # With additional arguments
        >>> def add_value(data, value):
        ...     return [x + value for x in data]
        >>> [1, 2, 3] >> dr.pipe(add_value, 10)
        [11, 12, 13]

        >>> # Chain multiple operations
        >>> [1, 2, 3] >> dr.pipe(lambda x: [i * 2 for i in x]) >> dr.pipe(sum)
        12
    """
    return func(data, *args, **kwargs)
