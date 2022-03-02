"""Functions from tidyr"""
from pipda import register_func

from ..core.contexts import Context
from ..base import seq


@register_func(None, context=Context.EVAL)
def full_seq(x, period, tol=1e-6):
    """Create the full sequence of values in a vector

    Args:
        x: A numeric vector.
        period: Gap between each observation. The existing data will be
            checked to ensure that it is actually of this periodicity.
        tol: Numerical tolerance for checking periodicity.

    Returns:
        The full sequence
    """
    minx = min(x)  # na not counted
    maxx = max(x)

    if any(
        (elem - minx) % period > tol
        and period - ((elem - minx) % period) > tol
        for elem in x
    ):
        raise ValueError("`x` is not a regular sequence.")

    if period - ((maxx - minx) % period) <= tol:
        maxx += tol

    return seq(minx, maxx, by=period)
