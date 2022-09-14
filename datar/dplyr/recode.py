"""Recode values

https://github.com/tidyverse/dplyr/blob/master/R/recode.R
"""

import numpy as np
from ..core.backends import pandas as pd
from ..core.backends.pandas import Categorical, Series
from ..core.backends.pandas.api.types import (
    is_scalar,
    is_categorical_dtype,
    is_numeric_dtype,
)

from ..core.utils import logger
from ..core.factory import func_factory
from ..core.tibble import SeriesCategorical

from ..base import c, intersect
from ..base.na import NA_integer_, NA_character_


def _get_first(x):
    """Get first raw item from an iterable"""
    try:
        x = x.iloc[0]
    except (AttributeError, IndexError):
        x = x[0]
    try:
        return x.item()
    except AttributeError:
        return x


def _args_to_recodings(*args, _force_index=False, **kwargs):
    """Convert arguments to replaceable"""
    values = {}
    for i, arg in enumerate(args):
        if isinstance(arg, dict):
            values.update(arg)
        else:
            values[i] = arg

    values.update(kwargs)
    if _force_index:
        for key in list(values):
            if isinstance(key, str) and key.isdigit():
                values[int(key)] = values.pop(key)
    return values


def _check_length(val, x, name):
    """Check the length of the values to recode"""
    length_x = len(val)
    n = len(x)
    if length_x in (1, n):
        return

    if n == 1:
        raise ValueError(f"{name} must be length 1, not {length_x}.")
    raise ValueError(f"{name} must be length {n}, not {length_x}.")


def _check_type(val, out_type, name):
    """Check the type of the values to recode"""
    if isinstance(out_type, type) and isinstance(None, out_type):
        return  # pragma: no cover

    if val.dtype is np.dtype(object):
        if out_type and not all(isinstance(elem, out_type) for elem in val):
            raise TypeError(
                f"{name} must be {out_type.__name__}, not {type(val[0])}."
            )
    elif out_type and not isinstance(_get_first(val), out_type):
        raise TypeError(
            f"{name} must be {out_type.__name__}, not {val.dtype.name}."
        )


def _replace_with(
    x,
    out_type,
    i,
    val,
    name,
):
    """Replace with given value at index"""
    # https://github.com/tidyverse/dplyr/blob/HEAD/R/utils-replace-with.R
    if val is None:
        return x

    if is_scalar(val):
        val = np.array([val])
    else:
        val = np.array(val)

    _check_length(val, x, name)
    if not pd.isnull(val).any():
        _check_type(val, out_type, name)
    # check_class(val, x, name)

    i[pd.isnull(i)] = False

    if len(val) == 1:
        x[i] = val[0]
    else:
        x[i] = val[i]

    return x


def _validate_recode_default(
    default,
    x,
    out,
    out_type,
    replaced,
):
    """Validate default for recoding"""
    default = _recode_default(x, default, out_type)
    if default is None and sum(replaced & ~pd.isnull(x)) < len(
        out[~pd.isnull(x)]
    ):
        logger.warning(
            "Unreplaced values treated as NA as `_x` is not compatible. "
            "Please specify replacements exhaustively or supply `_default`",
        )

    return default


def _recode_default(x, default, out_type):
    """Get right default for recoding"""
    if default is None and (
        out_type is None or isinstance(_get_first(x), out_type)
    ):
        return x
    return default


def _recode_numeric(
    _x,
    *args,
    _default=None,
    _missing=None,
    **kwargs,
):
    """Recode numeric vectors"""
    values = _args_to_recodings(*args, **kwargs, _force_index=True)
    _check_args(values, _default, _missing)
    if any(not isinstance(val, int) for val in values):
        raise ValueError(
            "All values must be unnamed (or named with integers)."
        )

    n = len(_x)
    out = np.array([np.nan] * n, dtype=object)
    replaced = np.array([False] * n)
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
        out, out_type, ~replaced & ~pd.isnull(_x), _default, "`_default`"
    )
    out = _replace_with(
        out,
        out_type,
        pd.isnull(_x) | (_x == NA_integer_),
        _missing,
        "`_missing`",
    )
    if out_type and not pd.isnull(out).any():
        out = out.astype(out_type)
    return out


def _recode_character(
    _x,
    *args,
    _default=None,
    _missing=None,
    **kwargs,
):
    """Recode character vectors"""
    values = _args_to_recodings(*args, **kwargs)
    _check_args(values, _default, _missing)
    if not all(isinstance(val, str) for val in values):
        raise ValueError("All values must be named.")

    n = len(_x)
    out = np.array([np.nan] * n, dtype=object)
    replaced = np.array([False] * n)
    out_type = None

    for val in values:
        if out_type is None:
            out_type = type(values[val])
        out = _replace_with(out, out_type, _x == val, values[val], f"`{val}`")
        replaced[_x == val] = True

    _default = _validate_recode_default(_default, _x, out, out_type, replaced)
    out = _replace_with(
        out, out_type, ~replaced & ~pd.isnull(_x), _default, "`_default`"
    )
    out = _replace_with(
        out,
        out_type,
        pd.isnull(_x) | (_x == NA_character_),
        _missing,
        "`_missing`",
    )
    if out_type and not pd.isnull(out).any():
        out = out.astype(out_type)
    return out


def _check_args(values, default, missing):
    """Check if any replacement specified"""
    if not values and default is None and missing is None:
        raise ValueError("No replacements provided.")


@func_factory(kind="transform")
def recode(
    _x,
    *args,
    _default=None,
    _missing=None,
    **kwargs,
):
    """Recode a vector, replacing elements in it

    Args:
        x: A vector to modify
        *args: and
        **kwargs: replacements
        _default: If supplied, all values not otherwise matched will be
            given this value. If not supplied and if the replacements are
            the same type as the original values in series, unmatched values
            are not changed. If not supplied and if the replacements are
            not compatible, unmatched values are replaced with np.nan.
        _missing: If supplied, any missing values in .x will be replaced
            by this value.

    Returns:
        The vector with values replaced
    """
    if is_categorical_dtype(_x):  # Categorical
        return recode.dispatch(SeriesCategorical)(
            _x,
            *args,
            _default=_default,
            _missing=_missing,
            **kwargs,
        )

    if is_numeric_dtype(_x):
        return _recode_numeric(
            _x, *args, _default=_default, _missing=_missing, **kwargs
        )

    return _recode_character(
        _x, *args, _default=_default, _missing=_missing, **kwargs
    )


@recode.register_dispatchee(SeriesCategorical)
def _(
    _x,
    *args,
    _default=None,
    _missing=None,
    **kwargs,
):
    """Recode factors"""
    x = _x.values  # get the Categorical object

    values = _args_to_recodings(*args, **kwargs)
    if not values:
        raise ValueError("No replacements provided.")

    if not all(isinstance(key, str) for key in values):
        raise ValueError(
            "Named values required for recoding factors/categoricals."
        )

    if _missing is not None:
        raise ValueError("`_missing` is not supported for factors.")

    n = len(x)
    _check_args(values, _default, _missing)
    out = np.array([np.nan] * n, dtype=object)
    replaced = np.array([False] * n)
    out_type = None

    for val in values:
        if out_type is None:
            out_type = type(_get_first([values[val]]))
        out = _replace_with(
            out,
            out_type,
            # _x.categories == val,
            x == val,
            values[val],
            f"`{val}`",
        )
        # _x may have duplicated values
        # replaced[_x.categories == val] = True
        replaced[x == val] = True

    _default = _validate_recode_default(_default, x, out, out_type, replaced)
    out = _replace_with(out, out_type, ~replaced, _default, "`_default`")

    if out_type is str:
        return Series(Categorical(out), index=_x.index, name=_x.name)
    return Series(out[x.codes], index=_x.index, name=_x.name)


@func_factory(kind="transform")
def recode_factor(
    _x,
    *args,
    _default=None,
    _missing=None,
    _ordered=False,
    **kwargs,
):
    """Recode a factor

    see recode().
    """
    values = _args_to_recodings(*args, **kwargs)

    recoded = recode.dispatch(Series)(
        _x,
        values,
        _default=_default,
        _missing=_missing,
    )

    out_type = type(_get_first(recoded))
    _default = _recode_default(_x, _default, out_type)
    all_levels = pd.unique(
        c(
            list(values.values()),
            [] if _default is None else _default,
            [] if _missing is None else _missing,
        )
    )

    recoded_levels = (
        recoded.categories
        if isinstance(recoded, Categorical)
        else pd.unique(recoded[pd.notnull(recoded)])
    )
    levels = intersect(all_levels, recoded_levels, __ast_fallback="normal")

    return Series(
        Categorical(recoded, categories=levels, ordered=_ordered),
        index=_x.index,
    )


recode_categorical = recode_factor
