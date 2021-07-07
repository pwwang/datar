"""Constants and functions about NULL (none)"""

from typing import Any

from pipda import register_func

from ..core.contexts import Context

# pylint: disable=unused-argument

NULL = None


@register_func(None, context=Context.EVAL)
def as_null(*args: Any, **kwargs: Any) -> None:
    """Ignores arguments and returns NULL (None). R's `as.null()`

    Args:
        *args: and
        **kwargs: arguments to be ignored

    Returns:
        `NULL`
    """
    return NULL


@register_func(None, context=Context.EVAL)
def is_null(x: Any) -> bool:
    """Check if x is exactly NULL (None), same as R's `is.null()`

    Args:
        x: The value to check

    Returns:
        True if x is NULL (None) else False
    """
    return x is NULL
