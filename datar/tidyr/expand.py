"""Expand data frame to include all possible combinations of values

https://github.com/tidyverse/tidyr/blob/HEAD/R/expand.R
"""

from typing import Any, Callable, Iterable, Mapping, Union

import numpy
import pandas
from numpy import product
from pandas import DataFrame, Series, Categorical
from pandas.core.dtypes.common import is_categorical_dtype
from pipda import register_func, register_verb

from ..core.contexts import Context
from ..core.defaults import DEFAULT_COLUMN_PREFIX
from ..core.types import is_scalar, is_null, is_not_null
from ..core.utils import categorized, copy_attrs, reconstruct_tibble
from ..core.grouped import DataFrameGroupBy, DataFrameRowwise
from ..core.names import repair_names

from ..base import NA, NULL, factor, levels
from ..tibble import tibble
from ..dplyr import arrange, distinct, pull


@register_func(None, context=Context.EVAL)
def expand_grid(
    *args: Iterable[Any],
    _name_repair: Union[str, Callable] = "check_unique",
    base0_: bool = None,
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
        base0_: Whether the suffixes of repaired names should be 0-based.
            If not provided, will use `datar.base.get_option('index.base.0')`.

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
        n = numpy.array([n], dtype=float)
        ns_np = numpy.array(list(ns.values()), dtype=float)

        each = n / numpy.cumprod(ns_np)
        times = n / each / ns_np

        each = dict(zip(dots, each.astype(int)))
        times = dict(zip(dots, times.astype(int)))
        out = {
            key: _vec_repeat(val, each[key], times[key])
            for key, val in dots.items()
        }

    ## tibble will somehow flatten the nested dataframes into fake nested df.
    ## do it inside _flatten_nested
    # out = tibble(out, _name_repair=_name_repair, base0_=base0_)
    return _flatten_nested(out, named, _name_repair, base0_)


@register_verb(DataFrame, context=Context.EVAL)
def expand(
    data: DataFrame,
    *args: Union[Series, DataFrame],
    _name_repair: Union[str, Callable] = "check_unique",
    base0_: bool = None,
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
        base0_: Whether the suffixes of repaired names should be 0-based.
            If not provided, will use `datar.base.get_option('index.base.0')`.

    Returns:
        A data frame with all combination of variables.
    """
    cols = _dots_cols(*args, **kwargs)
    named = cols.pop("__named__")
    cols = {key: _sorted_unique(val) for key, val in cols.items()}

    out = expand_grid(**cols, _name_repair=_name_repair, base0_=base0_)
    out = _flatten_nested(out, named, _name_repair, base0_)

    copy_attrs(out, data)
    return out


@expand.register(DataFrameGroupBy, context=Context.PENDING)
def _(
    data: DataFrameGroupBy,
    *args: Union[Series, DataFrame],
    _name_repair: Union[str, Callable] = "check_unique",
    base0_: bool = None,
    **kwargs: Union[Series, DataFrame],
) -> DataFrameGroupBy:
    """Expand on grouped data frame"""

    def apply_func(df):
        return expand(
            df, *args, _name_repair=_name_repair, base0_=base0_, **kwargs
        )

    out = data.datar_apply(apply_func)
    return reconstruct_tibble(data, out)


@expand.register(DataFrameRowwise, context=Context.EVAL)
def _(
    data: DataFrameRowwise,
    *args: Union[Series, DataFrame],
    _name_repair: Union[str, Callable] = "check_unique",
    base0_: bool = None,
    **kwargs: Union[Series, DataFrame],
) -> DataFrame:
    """Expand on rowwise dataframe"""
    return expand.dispatch(DataFrame)(
        data, *args, _name_repair=_name_repair, base0_=base0_, **kwargs
    )


@register_func(None, context=Context.EVAL)
def nesting(
    *args: Any,
    _name_repair: Union[str, Callable] = "check_unique",
    base0_: bool = None,
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
        base0_: Whether the suffixes of repaired names should be 0-based.
            If not provided, will use `datar.base.get_option('index.base.0')`.

    Returns:
        A data frame with all combinations in data.
    """
    cols = _dots_cols(*args, **kwargs)
    named = cols.pop("__named__")
    out = _sorted_unique(
        tibble(**cols, _name_repair=_name_repair, base0_=base0_)
    )
    return _flatten_nested(out, named, _name_repair, base0_)


@register_func(None, context=Context.EVAL)
def crossing(
    *args: Any,
    _name_repair: Union[str, Callable] = "check_unique",
    base0_: bool = None,
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
        base0_: Whether the suffixes of repaired names should be 0-based.
            If not provided, will use `datar.base.get_option('index.base.0')`.

    Returns:
        A data frame with values deduplicated and sorted.
    """
    cols = _dots_cols(*args, **kwargs)
    named = cols.pop("__named__")
    out = {key: _sorted_unique(val) for key, val in cols.items()}

    out = expand_grid(**out, _name_repair=_name_repair, base0_=base0_)
    return _flatten_nested(out, named, _name_repair, base0_)


# Helpers --------------------------------
def _dots_cols(
    *args: Iterable[Any], **kwargs: Iterable[Any]
) -> Mapping[str, Iterable[Any]]:
    """Mimic tidyr:::dots_cols to clean up the dot (args, kwargs) arugments"""
    out = {"__named__": {}}
    for i, arg in enumerate(args):
        if arg is None:
            continue

        name = getattr(
            arg,
            "__dfname__",
            getattr(arg, "name", getattr(arg, "__name__", None)),
        )
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

    vec = categorized(vec)
    # numpy.repeat() turn [numpy.nan, 'A'] to ['nan', 'A']
    vec_to_rep = vec
    if any(isinstance(elem, str) for elem in vec) and any(
        pandas.isnull(elem) for elem in vec
    ):
        vec_to_rep = numpy.array(vec, dtype=object)
    out = numpy.tile(numpy.repeat(vec_to_rep, each), times)
    if is_categorical_dtype(vec):
        return factor(out, levels(vec), ordered=vec.ordered)
    return out


def _flatten_nested(
    x: Union[DataFrame, Mapping[str, Iterable[Any]]],
    named: Mapping[str, bool],
    name_repair: Union[str, Callable],
    base0: bool = None,
) -> DataFrame:
    """Mimic `tidyr:::flatten_nested`"""
    if isinstance(x, DataFrame):
        names = repair_names(list(named), name_repair, base0)
        named = dict(zip(names, named.values()))
        x = {name: pull(x, name) for name in named}

    to_flatten = {
        key: isinstance(val, DataFrame) and not named[key]
        for key, val in x.items()
    }
    out = _flatten_at(x, to_flatten)
    return tibble(**out, _name_repair=name_repair, base0_=base0)


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


def _sorted_unique(x: Iterable[Any]) -> Union[Categorical, numpy.ndarray]:
    """Sort and deduplicate the values"""
    x = categorized(x)
    if is_categorical_dtype(x):
        lvls = levels(x)
        return factor(lvls, lvls, exclude=NULL, ordered=x.ordered)

    # don't sort on bare list?
    # if isinstance(x, list):
    #     return pandas.unique(x)

    if isinstance(x, DataFrame):
        return arrange(distinct(x))

    # return numpy.sort(numpy.unique(x))
    # numpy.unique() will turn ['A', 'B', numpy.nan] to ['A', 'B', 'nan']
    try:
        out = pandas.unique(x)
    except TypeError:
        # unhashable type: 'list'
        # workaround for unhashable elements
        # using its stringified form as key, which has side-effects
        maps = {str(elem): elem for elem in x}
        out = pandas.unique(list(maps.keys()))
        out = numpy.array([maps[elem] for elem in out], dtype=object)

    has_na = is_null(out).any()
    if has_na:
        out = numpy.sort(out[is_not_null(out)])
        return numpy.concatenate([out, [NA]])
    # numpy.sort() cannot do comparisons between string and NA
    return numpy.sort(out)
