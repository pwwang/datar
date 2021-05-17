"""Basic functions"""
from typing import Any, Iterable

import numpy
from pandas import Series
from pipda import register_func

from ..core.contexts import Context
from ..core.collections import Collection

@register_func(None, context=Context.EVAL)
def itemgetter(data: Iterable[Any], *subscripts) -> numpy.ndarray:
    """Itemgetter as a function for verb

    In datar expression, we can do:
    >>> arr = [1,2,3]
    >>> tibble(x=2) >> mutate(y=arr[f.x])

    Since `arr[f.x]` won't compile. We need to use the `itemgetter` operator:
    >>> tibble(x=2) >> mutate(y=itemgetter(arr, f.x))

    Args:
        data: The data to be get items from
        *subscripts: The subscripts
    """
    flattened = Collection(
        subs.values if isinstance(subs, Series) else subs
        for subs in  subscripts
    )

    return numpy.array([data[sub] for sub in flattened])
