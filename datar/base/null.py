"""Constants and functions about NULL (none)"""
from pipda import register_func

from ..core.contexts import Context


NULL = None


@register_func(context=Context.EVAL)
def as_null(*args, **kwargs):
    """Ignores arguments and returns NULL (None). R's `as.null()`

    Args:
        *args: and
        **kwargs: arguments to be ignored

    Returns:
        `NULL`
    """
    return NULL


@register_func(context=Context.EVAL)
def is_null(x):
    """Check if x is exactly NULL (None), same as R's `is.null()`

    Args:
        x: The value to check

    Returns:
        True if x is NULL (None) else False
    """
    return x is NULL
