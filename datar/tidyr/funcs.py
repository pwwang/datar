"""Functions from tidyr"""

from ..core.factory import func_factory
from ..base import seq


@func_factory(kind="apply")
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

    minx = x.min()  # na not counted
    maxx = x.max()

    if (
        ((x - minx) % period > tol) & (period - ((x - minx) % period) > tol)
    ).any():
        raise ValueError("`x` is not a regular sequence.")

    if period - ((maxx - minx) % period) <= tol:
        maxx += tol

    return seq(minx, maxx, by=period)
