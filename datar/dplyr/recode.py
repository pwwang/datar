"""Recode values

https://github.com/tidyverse/dplyr/blob/master/R/recode.R
"""
from typing import Any, Iterable, Mapping

import numpy
import pandas
from pandas import Categorical, Series
from pipda import register_func

from ..core.contexts import Context
from ..core.utils import logger, get_option, Array
from ..core.types import is_not_null, is_scalar, is_null, is_categorical
from ..base import NA, unique, c, intersect
from ..base.na import NA_integer_, NA_character_


def _get_first(x: Iterable[Any]) -> Any:
    """Get first raw item from an iterable"""
    x = x[0]
    try:
        return x.item()
    except AttributeError:
        return x


def _args_to_recodings(
    *args: Any, _force_index: bool = False, **kwargs: Any
) -> Mapping[Any, Any]:
    """Convert arguments to replaceable"""
    base0_ = get_option("index.base.0")
    values = {}
    for i, arg in enumerate(args):
        if isinstance(arg, dict):
            values.update(arg)
        else:
            values[i + int(not base0_)] = arg

    values.update(kwargs)
    if _force_index:
        for key in list(values):
            if isinstance(key, str) and key.isdigit():
                values[int(key)] = values.pop(key)
    return values


def _check_length(val: numpy.ndarray, x: numpy.ndarray, name: str):
    """Check the length of the values to recode"""
    length_x = len(val)
    n = len(x)
    if length_x in (1, n):
        return

    if n == 1:
        raise ValueError(f"{name} must be length 1, not {length_x}.")
    raise ValueError(f"{name} must be length {n}, not {length_x}.")


def _check_type(val: numpy.ndarray, out_type: type, name: str):
    """Check the type of the values to recode"""
    if val.dtype is numpy.dtype(object):
        if out_type and not all(isinstance(elem, out_type) for elem in val):
            raise TypeError(
                f"{name} must be {out_type.__name__}, not {type(val[0])}."
            )
    elif out_type and not isinstance(_get_first(val), out_type):
        raise TypeError(
            f"{name} must be {out_type.__name__}, not {val.dtype.name}."
        )


def _replace_with(
    # pylint: disable=invalid-name
    x: numpy.ndarray,
    out_type: type,
    i: numpy.ndarray,
    val: Any,
    name: str,
) -> numpy.ndarray:
    """Replace with given value at index"""
    # https://github.com/tidyverse/dplyr/blob/HEAD/R/utils-replace-with.R
    if val is None:
        return x

    if is_scalar(val):
        val = numpy.array([val])
    else:
        val = numpy.array(val)

    _check_length(val, x, name)
    if not is_null(val).any():
        _check_type(val, out_type, name)
    # check_class(val, x, name)

    i[is_null(i)] = False

    if len(val) == 1:
        x[i] = val[0]
    else:
        x[i] = val[i]

    return x


def _validate_recode_default(
    default: Any,
    x: numpy.ndarray,
    out: numpy.ndarray,
    out_type: type,
    replaced: numpy.ndarray,
) -> numpy.ndarray:
    """Validate default for recoding"""
    default = _recode_default(x, default, out_type)
    if default is None and sum(replaced & is_not_null(x)) < len(
        out[is_not_null(x)]
    ):
        logger.warning(
            "Unreplaced values treated as NA as `_x` is not compatible. "
            "Please specify replacements exhaustively or supply `_default`",
        )

    return default


def _recode_default(x: numpy.ndarray, default: Any, out_type: type) -> Any:
    """Get right default for recoding"""
    if default is None and (
        out_type is None or isinstance(_get_first(x), out_type)
    ):
        return x
    return default


def _recode_numeric(
    _x: numpy.ndarray,
    *args: Any,
    _default: Any = None,
    _missing: Any = None,
    **kwargs: Any,
) -> numpy.ndarray:
    """Recode numeric vectors"""

    values = _args_to_recodings(*args, **kwargs, _force_index=True)
    _check_args(values, _default, _missing)
    if any(not isinstance(val, int) for val in values):
        raise ValueError("All values must be unnamed (or named with integers).")

    n = len(_x)
    out = numpy.array([NA] * n, dtype=object)
    replaced = numpy.array([False] * n)
    out_type = None

    for val in values:
        if out_type is None:
            out_type = type(values[val])
        out = _replace_with(
            out, out_type, _x == val, values[val], f"Element {val}"
        )
        replaced[_x == val] = True

    _default = _validate_recode_default(_default, _x, out, out_type, replaced)
    out = _replace_with(
        out, out_type, ~replaced & is_not_null(_x), _default, "`_default`"
    )
    out = _replace_with(
        out, out_type, is_null(_x) | (_x == NA_integer_), _missing, "`_missing`"
    )
    if out_type and not is_null(out).any():
        out = out.astype(out_type)
    return out


def _recode_character(
    _x: Iterable[Any],
    *args: Any,
    _default: Any = None,
    _missing: Any = None,
    **kwargs: Any,
) -> numpy.ndarray:
    """Recode character vectors"""
    values = _args_to_recodings(*args, **kwargs)
    _check_args(values, _default, _missing)
    if not all(isinstance(val, str) for val in values):
        raise ValueError("All values must be named.")

    n = len(_x)
    out = numpy.array([NA] * n, dtype=object)
    replaced = numpy.array([False] * n)
    out_type = None

    for val in values:
        if out_type is None:
            out_type = type(values[val])
        out = _replace_with(out, out_type, _x == val, values[val], f"`{val}`")
        replaced[_x == val] = True

    _default = _validate_recode_default(_default, _x, out, out_type, replaced)
    out = _replace_with(
        out, out_type, ~replaced & is_not_null(_x), _default, "`_default`"
    )
    out = _replace_with(
        out,
        out_type,
        is_null(_x) | (_x == NA_character_),
        _missing,
        "`_missing`",
    )
    if out_type and not is_null(out).any():
        out = out.astype(out_type)
    return out


def _check_args(values: Mapping[Any, Any], default: Any, missing: Any) -> None:
    """Check if any replacement specified"""
    if not values and default is None and missing is None:
        raise ValueError("No replacements provided.")


@register_func(context=Context.EVAL)
def recode(
    _x: Iterable,
    *args: Any,
    _default: Any = None,
    _missing: Any = None,
    **kwargs: Any,
) -> Iterable[Any]:
    """Recode a vector, replacing elements in it

    Args:
        x: A vector to modify
        *args: and
        **kwargs: replacements
        _default: If supplied, all values not otherwise matched will be
            given this value. If not supplied and if the replacements are
            the same type as the original values in series, unmatched values
            are not changed. If not supplied and if the replacements are
            not compatible, unmatched values are replaced with NA.
        _missing: If supplied, any missing values in .x will be replaced
            by this value.

    Returns:
        The vector with values replaced
    """
    if is_scalar(_x):
        _x = [_x]

    if not isinstance(_x, numpy.ndarray):
        _x_obj = numpy.array(_x, dtype=object)  # Keep NAs
        _x = numpy.array(_x)
        if numpy.issubdtype(_x.dtype, numpy.str_):
            na_len = len(NA_character_)
            if (_x.dtype.itemsize >> 2) < na_len:  # length not enough
                _x = _x.astype(f"<U{na_len}")
            _x[is_null(_x_obj)] = NA_character_
        elif numpy.issubdtype(_x.dtype, numpy.integer):
            _x[is_null(_x_obj)] = NA_integer_

    if numpy.issubdtype(_x.dtype, numpy.number) or numpy.issubdtype(
        Array(_x[is_not_null(_x)].tolist()).dtype, numpy.number
    ):
        return _recode_numeric(
            _x, *args, _default=_default, _missing=_missing, **kwargs
        )

    return _recode_character(
        _x, *args, _default=_default, _missing=_missing, **kwargs
    )


@recode.register((Categorical, Series), context=Context.EVAL)
def _(
    _x: Iterable,
    *args: Any,
    _default: Any = None,
    _missing: Any = None,
    **kwargs: Any,
) -> Categorical:
    """Recode factors"""
    if not is_categorical(_x):  # non-categorical Series
        return recode(
            _x.values, *args, _default=_default, _missing=_missing, **kwargs
        )
    if isinstance(_x, Series):
        _x = _x.values  # get the Categorical object

    values = _args_to_recodings(*args, **kwargs)
    if not values:
        raise ValueError("No replacements provided.")

    if not all(isinstance(key, str) for key in values):
        raise ValueError(
            "Named values required for recoding factors/categoricals."
        )

    if _missing is not None:
        raise ValueError("`_missing` is not supported for factors.")

    n = len(_x)
    _check_args(values, _default, _missing)
    out = numpy.array([NA] * n, dtype=object)
    replaced = numpy.array([False] * n)
    out_type = None

    for val in values:
        if out_type is None:
            out_type = type(values[val])
        out = _replace_with(
            out, out_type, _x.categories == val, values[val], f"`{val}`"
        )
        replaced[_x.categories == val] = True

    _default = _validate_recode_default(_default, _x, out, out_type, replaced)
    out = _replace_with(out, out_type, ~replaced, _default, "`_default`")

    if out_type is str:
        return Categorical(out)
    return out[_x.codes]


@register_func(None, context=Context.EVAL)
def recode_factor(
    _x: Iterable[Any],
    *args: Any,
    _default: Any = None,
    _missing: Any = None,
    _ordered: bool = False,
    **kwargs: Any,
) -> Iterable[Any]:
    """Recode a factor

    see recode().
    """
    values = _args_to_recodings(*args, **kwargs)
    recoded = recode(_x, values, _default=_default, _missing=_missing)

    out_type = type(_get_first(recoded))
    _default = _recode_default(_x, _default, out_type)
    all_levels = unique(
        c(
            list(values.values()),
            [] if _default is None else _default,
            [] if _missing is None else _missing,
        )
    )

    recoded_levels = (
        recoded.categories
        if isinstance(recoded, Categorical)
        else unique(recoded[pandas.notna(recoded)])
    )
    levels = intersect(all_levels, recoded_levels)

    return Categorical(recoded, categories=levels, ordered=_ordered)


recode_categorical = recode_factor  # pylint: disable=invalid-name
