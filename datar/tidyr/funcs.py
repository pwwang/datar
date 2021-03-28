"""Functions from tidyr"""

from pandas.core.series import Series
from datar.core.middlewares import Nesting
from datar.dplyr.funcs import last
from typing import Any, Iterable

import numpy
from pipda import register_func

from ..core.types import NumericType
from ..core.contexts import Context

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
    data = sorted(x)
    ret = []
    for elem in data:
        if not ret:
            ret.append(elem)
            continue
        last_elem = ret[-1]
        n = float(elem - last_elem) / float(period)
        if not numpy.isclose(n, float(round(n)), atol=tol):
            raise ValueError('`x` is not a regular sequence.')
        ret.extend((i+1) * period + last_elem for i in range(int(round(n))))
    if isinstance(x, Series):
        return Series(x, name=x.name)
    return ret

@register_func(None, context=None)
def nesting(*cols: Any, **kwargs: Any) -> Nesting:
    return Nesting(*cols, **kwargs)
