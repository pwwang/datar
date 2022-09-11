"""Expand data frame to include all possible combinations of values

https://github.com/tidyverse/tidyr/blob/HEAD/R/expand.R
"""

from typing import Any, Callable, Iterable, Mapping, Union

import numpy as np
from numpy import product
from pipda import register_func, register_verb

from ..core.backends import pandas as pd
from ..core.backends.pandas import DataFrame, Series, Categorical
from ..core.backends.pandas.api.types import is_scalar, is_categorical_dtype

from ..core.contexts import Context
from ..core.defaults import DEFAULT_COLUMN_PREFIX
from ..core.tibble import TibbleGrouped, TibbleRowwise, reconstruct_tibble
from ..core.names import repair_names

from ..base import NA, NULL, factor, levels
from ..base.factor import _ensure_categorical
from ..tibble import tibble
from ..dplyr import arrange, distinct, pull, ungroup


@register_func(context=Context.EVAL)
def expand_grid(
    *args: Iterable[Any],
    _name_repair: Union[str, Callable] = "check_unique",
    **kwargs: Iterable[Any],
) -> DataFrame:
    """Create a tibble from all combinations of inputs

    Args:
        *args: and
        **kwargs: name-value pairs.
            For `*args`, names will be inferred from the values and if failed,
            `_Var0`, `_Var1`, etc will be used.
        _name_repair: treatment of problematic column names:
            - "minimal": No name repair or checks, beyond basic existence,
            - "unique": Make sure names are unique and not empty,
            - "check_unique": (default value), no name repair,
                but check they are unique,
            - "universal": Make the names unique and syntactic
            - a function: apply custom name repair

    Returns:
        A data frame with one column for each input in `*args` and `**kwargs`.
        The output will have one row for each combination of the inputs,
        i.e. the size be equal to the product of the sizes of the inputs.
        This implies that if any input has length 0, the output will have
        zero rows.
    """
    dots = _dots_cols(*args, **kwargs)
    named = dots.pop("__named__")
    ns = {key: len(val) for key, val in dots.items()}
    n = product(list(ns.values()))

    if n == 0:
        out = {
            key: (val.iloc[[], :] if isinstance(val, DataFrame) else [])
            for key, val in dots.items()
        }
    else:
        n = np.array([n], dtype=float)
        ns_np = np.array(list(ns.values()), dtype=float)

        each = n / np.cumprod(ns_np)
        times = n / each / ns_np

        each = dict(zip(dots, each.astype(int)))
        times = dict(zip(dots, times.astype(int)))
        out = {
            key: _vec_repeat(val, each[key], times[key])
            for key, val in dots.items()
        }

    # # tibble will somehow flatten the nested dataframes into fake nested df.
    # # do it inside _flatten_nested
    # out = tibble(out, _name_repair=_name_repair)
    return _flatten_nested(out, named, _name_repair)


@register_verb(DataFrame, context=Context.EVAL)
def expand(
    data: DataFrame,
    *args: Union[Series, DataFrame],
    _name_repair: Union[str, Callable] = "check_unique",
    **kwargs: Union[Series, DataFrame],
) -> DataFrame:
    """Generates all combination of variables found in a dataset.

    Args:
        data: A data frame
        *args: and,
        **kwargs: columns to expand. Columns can be atomic lists.
            - To find all unique combinations of x, y and z, including
              those not present in the data, supply each variable as a
              separate argument: `expand(df, x, y, z)`.
            - To find only the combinations that occur in the data, use
              nesting: `expand(df, nesting(x, y, z))`.
            - You can combine the two forms. For example,
              `expand(df, nesting(school_id, student_id), date)` would
              produce a row for each present school-student combination
              for all possible dates.
        _name_repair: treatment of problematic column names:
            - "minimal": No name repair or checks, beyond basic existence,
            - "unique": Make sure names are unique and not empty,
            - "check_unique": (default value), no name repair,
                but check they are unique,
            - "universal": Make the names unique and syntactic
            - a function: apply custom name repair

    Returns:
        A data frame with all combination of variables.
    """
    cols = _dots_cols(*args, **kwargs)
    named = cols.pop("__named__")
    cols = {key: _sorted_unique(val) for key, val in cols.items()}

    out = expand_grid(**cols, _name_repair=_name_repair)
    out = _flatten_nested(out, named, _name_repair)

    return out


@expand.register(TibbleGrouped, context=Context.PENDING)
def _(
    data: TibbleGrouped,
    *args: Union[Series, DataFrame],
    _name_repair: Union[str, Callable] = "check_unique",
    **kwargs: Union[Series, DataFrame],
) -> TibbleGrouped:
    """Expand on grouped data frame"""

    def apply_func(df):
        return expand(
            df,
            *args,
            _name_repair=_name_repair,
            **kwargs,
            __ast_fallback="normal",
        )

    out = data._datar["grouped"].apply(apply_func).droplevel(-1).reset_index()
    return reconstruct_tibble(data, out)


@expand.register(TibbleRowwise, context=Context.PENDING)
def _(
    data: TibbleRowwise,
    *args: Union[Series, DataFrame],
    _name_repair: Union[str, Callable] = "check_unique",
    **kwargs: Union[Series, DataFrame],
) -> DataFrame:
    """Expand on rowwise dataframe"""
    return expand(
        ungroup(data, __ast_fallback="normal"),
        *args,
        _name_repair=_name_repair,
        **kwargs,
        __ast_fallback="normal",
    )


@register_func(context=Context.EVAL)
def nesting(
    *args: Any,
    _name_repair: Union[str, Callable] = "check_unique",
    **kwargs: Any,
) -> DataFrame:
    """A helper that only finds combinations already present in the data.

    Args:
        *args: and,
        **kwargs: columns to expand. Columns can be atomic lists.
            - To find all unique combinations of x, y and z, including
              those not present in the data, supply each variable as a
              separate argument: `expand(df, x, y, z)`.
            - To find only the combinations that occur in the data, use
              nesting: `expand(df, nesting(x, y, z))`.
            - You can combine the two forms. For example,
              `expand(df, nesting(school_id, student_id), date)` would
              produce a row for each present school-student combination
              for all possible dates.
        _name_repair: treatment of problematic column names:
            - "minimal": No name repair or checks, beyond basic existence,
            - "unique": Make sure names are unique and not empty,
            - "check_unique": (default value), no name repair,
                but check they are unique,
            - "universal": Make the names unique and syntactic
            - a function: apply custom name repair

    Returns:
        A data frame with all combinations in data.
    """
    cols = _dots_cols(*args, **kwargs)
    named = cols.pop("__named__")
    out = _sorted_unique(tibble(**cols, _name_repair=_name_repair))
    return _flatten_nested(out, named, _name_repair)


@register_func(context=Context.EVAL)
def crossing(
    *args: Any,
    _name_repair: Union[str, Callable] = "check_unique",
    **kwargs: Any,
) -> DataFrame:
    """A wrapper around `expand_grid()` that de-duplicates and sorts its inputs

    When values are not specified by literal `list`, they will be sorted.

    Args:
        *args: and,
        **kwargs: columns to expand. Columns can be atomic lists.
            - To find all unique combinations of x, y and z, including
              those not present in the data, supply each variable as a
              separate argument: `expand(df, x, y, z)`.
            - To find only the combinations that occur in the data, use
              nesting: `expand(df, nesting(x, y, z))`.
            - You can combine the two forms. For example,
              `expand(df, nesting(school_id, student_id), date)` would
              produce a row for each present school-student combination
              for all possible dates.
        _name_repair: treatment of problematic column names:
            - "minimal": No name repair or checks, beyond basic existence,
            - "unique": Make sure names are unique and not empty,
            - "check_unique": (default value), no name repair,
                but check they are unique,
            - "universal": Make the names unique and syntactic
            - a function: apply custom name repair

    Returns:
        A data frame with values deduplicated and sorted.
    """
    cols = _dots_cols(*args, **kwargs)
    named = cols.pop("__named__")
    out = {key: _sorted_unique(val) for key, val in cols.items()}

    out = expand_grid(**out, _name_repair=_name_repair)
    return _flatten_nested(out, named, _name_repair)


# Helpers --------------------------------
def _dots_cols(
    *args: Iterable[Any], **kwargs: Iterable[Any]
) -> Mapping[str, Iterable[Any]]:
    """Mimic tidyr:::dots_cols to clean up the dot (args, kwargs) arugments"""
    out = {"__named__": {}}
    for i, arg in enumerate(args):
        if arg is None:
            continue

        name = getattr(arg, "name", getattr(arg, "__name__", None))
        if not isinstance(name, str):
            # name is a Series
            name = None
        name = name or f"{DEFAULT_COLUMN_PREFIX}{i}"
        out["__named__"][name] = False
        out[name] = [arg] if is_scalar(arg) else arg

    for name, arg in kwargs.items():
        if arg is None:
            continue

        out[name] = [arg] if is_scalar(arg) else arg
        out["__named__"][name] = True

    return out


def _vec_repeat(
    vec: Iterable[Any], each: Iterable[int], times: Iterable[int]
) -> Iterable[Any]:
    """Repeat a vector or a dataframe by rows"""
    if isinstance(vec, DataFrame):
        indexes = _vec_repeat(vec.index, each=each, times=times)
        return vec.loc[indexes, :].reset_index(drop=True)

    vec = _ensure_categorical(vec)
    # np.repeat() turn [np.nan, 'A'] to ['nan', 'A']
    vec_to_rep = vec
    if any(isinstance(elem, str) for elem in vec) and any(
        pd.isnull(elem) for elem in vec
    ):
        vec_to_rep = np.array(vec, dtype=object)
    out = np.tile(np.repeat(vec_to_rep, each), times)
    if is_categorical_dtype(vec):
        return factor(out, levels(vec), ordered=vec.ordered)
    return out


def _flatten_nested(
    x: Union[DataFrame, Mapping[str, Iterable[Any]]],
    named: Mapping[str, bool],
    name_repair: Union[str, Callable],
) -> DataFrame:
    """Mimic `tidyr:::flatten_nested`"""
    if isinstance(x, DataFrame):
        names = repair_names(list(named), name_repair)
        named = dict(zip(names, named.values()))
        x = {name: pull(x, name, __ast_fallback="normal",) for name in named}

    to_flatten = {
        key: isinstance(val, DataFrame) and not named[key]
        for key, val in x.items()
    }
    out = _flatten_at(x, to_flatten)
    return tibble(**out, _name_repair=name_repair)


def _flatten_at(
    x: Mapping[str, Iterable[Any]], to_flatten: Mapping[str, bool]
) -> Mapping[str, Iterable[Any]]:
    """Flatten data at `to_flatten`"""
    if not any(to_flatten.values()):
        return x

    out = {}
    for name, val in x.items():
        if len(val) == 0:
            continue

        if to_flatten[name]:
            for col in val:
                out[col] = val[col]
        else:
            out[name] = val
    return out


def _sorted_unique(x: Iterable[Any]) -> Union[Categorical, np.ndarray]:
    """Sort and deduplicate the values"""
    x = _ensure_categorical(x)
    if is_categorical_dtype(x):
        lvls = levels(x)
        return factor(lvls, lvls, exclude=NULL, ordered=x.ordered)

    # don't sort on bare list?
    # if isinstance(x, list):
    #     return pd.unique(x)

    if isinstance(x, DataFrame):
        return arrange(
            distinct(x, __ast_fallback="normal"),
            __ast_fallback="normal",
        )

    # return np.sort(np.unique(x))
    # np.unique() will turn ['A', 'B', np.nan] to ['A', 'B', 'nan']
    try:
        out = pd.unique(x)
    except TypeError:
        # unhashable type: 'list'
        # workaround for unhashable elements
        # using its stringified form as key, which has side-effects
        maps = {str(elem): elem for elem in x}
        out = pd.unique(list(maps.keys()))
        out = np.array([maps[elem] for elem in out], dtype=object)

    has_na = pd.isnull(out).any()
    if has_na:
        out = np.sort(out[~pd.isnull(out)])
        return np.concatenate([out, [NA]])
    # np.sort() cannot do comparisons between string and NA
    return np.sort(out)
