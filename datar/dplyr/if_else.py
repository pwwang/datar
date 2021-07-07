"""Vectorised if and multiple if-else

https://github.com/tidyverse/dplyr/blob/master/R/if_else.R
https://github.com/tidyverse/dplyr/blob/master/R/case_when.R
"""
from typing import Any, Iterable, Union

import numpy
from pandas import Series
from pipda import register_func

from ..core.contexts import Context
from ..core.types import is_iterable, is_scalar, is_null
from ..core.utils import Array
from ..base import NA


@register_func(None, context=Context.EVAL)
def if_else(
    condition: Union[bool, Iterable[bool]],
    true: Any,
    false: Any,
    missing: Any = None,
) -> numpy.ndarray:
    """Where condition is TRUE, the matching value from true, where it's FALSE,
    the matching value from false, otherwise missing.

    Args:
        condition: the conditions
        true: and
        false: Values to use for TRUE and FALSE values of condition.
            They must be either the same length as condition, or length 1.
        missing: If not None, will be used to replace missing values

    Returns:
        A series with values replaced.
    """
    condition = Array(condition)
    na_indexes = is_null(condition)
    condition[na_indexes] = False
    condition = condition.astype(bool)
    return case_when(
        na_indexes,
        missing,
        numpy.invert(condition),
        false,
        condition,
        true,
        True,
        missing,
    )


@register_func(None, context=Context.EVAL)
def case_when(*when_cases: Any) -> Series:
    """Vectorise multiple `if_else()` statements.

    Args:
        *when_cases: A even-size sequence, with 2n-th element values to match,
            and 2(n+1)-th element the values to replace.
            When matching value is True, then next value will be default to
            replace

    Returns:
        A series with values replaced
    """
    if not when_cases or len(when_cases) % 2 != 0:
        raise ValueError("Number of arguments of case_when should be even.")

    out_len = 1
    if is_iterable(when_cases[0]):
        out_len = len(when_cases[0])
    elif is_iterable(when_cases[1]):
        out_len = len(when_cases[1])

    out = Array([NA] * out_len, dtype=object)
    when_cases = reversed(list(zip(when_cases[0::2], when_cases[1::2])))
    for case, rep in when_cases:
        if case is True:
            out[:] = rep
        elif (
            case is not False and case is not NA
        ):  # skip atmoic False condition
            case = Array(case)
            case[is_null(case)] = False
            case = case.astype(bool)
            index = numpy.where(case)
            if is_scalar(rep) or len(rep) == 1 or len(rep) == len(index):
                out[index] = rep
            elif len(rep) == len(out):
                out[index] = Array(rep)[index]
            else:
                raise ValueError(
                    f"Values to replace must be length {out_len} "
                    f"(length of `condition`) or one, not {len(rep)}."
                )

    return Array(out.tolist())  # shrink the dtype
