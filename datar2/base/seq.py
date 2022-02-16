from typing import Any, Sequence, Union

import numpy as np
from pipda import register_func
from pandas.api.types import is_scalar, is_array_like, is_integer
from pandas.core.groupby import SeriesGroupBy
from pandas._typing import AnyArrayLike

from ..core.utils import logger
from ..core.contexts import Context
from ..core.collections import Collection


@register_func(None, context=Context.EVAL)
def seq_along(along_with: Union[AnyArrayLike, Sequence]) -> np.ndarray:
    """Generate sequences along an iterable

    Args:
        along_with: An iterable to seq along with

    Returns:
        The generated sequence.
    """
    return np.array(range(len(along_with)), dtype=int)


@register_func(None, context=Context.EVAL)
def seq_len(length_out: Union[int, AnyArrayLike, Sequence]) -> np.ndarray:
    """Generate sequences with the length"""
    if isinstance(length_out, SeriesGroupBy):
        return length_out.apply(seq_len).explode().astype(int)

    if is_scalar(length_out):
        return np.array(range(int(length_out)), dtype=int)

    if len(length_out) > 1:
        logger.warning(
            "In seq_len(...) : first element used of 'length_out' argument"
        )
    length_out = int(list(length_out)[0])
    return np.array(range(length_out), dtype=int)


@register_func(None, context=Context.EVAL)
def seq(
    from_: int = None,
    to: int = None,
    by: int = None,
    length_out: Union[int, AnyArrayLike, Sequence[int]] = None,
    along_with: int = None,
) -> AnyArrayLike:
    """Generate a sequence

    https://rdrr.io/r/base/seq.html

    Note that this API is consistent with r-base's seq. 1-based and inclusive.
    """
    if along_with is not None:
        return seq_along(along_with)

    if is_array_like(from_):
        return seq_along(from_)

    if length_out is not None and from_ is None and to is None:
        return seq_len(length_out)

    if from_ is None:
        from_ = 0
    elif to is None:
        from_, to = 0, from_

    if length_out is not None:
        by = (float(to) - float(from_)) / float(length_out)

    elif by is None:
        by = 1 if to > from_ else -1
        length_out = to - from_ if to > from_ else from_ - to
    else:
        length_out = (to - from_ + 0.1 * by) // by
    return np.array([from_ + n * by for n in range(int(length_out))], dtype=int)


@register_func(None, context=Context.UNSET)
def c(*elems: Any) -> AnyArrayLike:
    """Mimic R's concatenation. Named one is not supported yet
    All elements passed in will be flattened.

    Args:
        *elems: The elements

    Returns:
        A collection of elements
    """
    return Collection(*elems)


@register_func(None, context=Context.EVAL)
def rep(
    x: Any,
    times: Union[int, Sequence[int], AnyArrayLike] = 1,
    length: int = None,
    each: int = 1,
) -> np.ndarray:
    """replicates the values in x

    Args:
        x: a vector or scaler
        times: number of times to repeat each element if of length len(x),
            or to repeat the whole vector if of length 1
        length: non-negative integer. The desired length of the output vector
        each: non-negative integer. Each element of x is repeated each times.

    Returns:
        A list of repeated elements in x.
    """
    if is_scalar(x):
        x = [x]
    if not is_scalar(times):
        if len(times) != len(x):
            raise ValueError(
                "Invalid times argument, expect length "
                f"{len(times)}, got {len(x)}"
            )
        if each != 1:
            raise ValueError(
                "Unexpected each argument when times is an iterable."
            )

    if is_integer(times):
        x = np.array(
            [elem for elem in x for _ in range(each)] * int(times),
            dtype=int,
        )
    else:
        x = np.array(
            [elem for n, elem in zip(times, x) for _ in range(n)],
            dtype=int
        )
    if length is None:
        return x

    repeats = length // len(x) + 1
    x = np.tile(x, repeats)
    return x[:length]
