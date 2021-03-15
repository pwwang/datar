"""Functions from tidyr"""

from datar.dplyr.funcs import last
from typing import Iterable

import numpy
from pipda import register_func, Context

from ..core.types import NumericType

@register_func(None, context=Context.EVAL)
def full_seq(
        x: Iterable[NumericType],
        period: NumericType,
        tol: float = 1e-6
) -> Iterable[NumericType]:
    """Create the full sequence of values in a vector

    Args:
        x: A numeric vector.
        period: Gap between each observation. The existing data will be
            checked to ensure that it is actually of this periodicity.
        tol: Numerical tolerance for checking periodicity.

    Returns:
        The full sequence
    """
    x = sorted(x)
    ret = []
    for elem in x:
        if not ret:
            ret.append(elem)
            continue
        last_elem = ret[-1]
        n = float(elem - last_elem) / float(period)
        if not numpy.isclose(n, float(round(n)), atol=tol):
            raise ValueError('`x` is not a regular sequence.')
        ret.extend((i+1) * period + last_elem for i in range(int(round(n))))
    return ret
