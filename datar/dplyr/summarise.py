"""Summarise each group to fewer rows"""

from itertools import chain
from typing import Any, Union

from pandas import DataFrame, Index, Series
from pandas.core.groupby import SeriesGroupBy
from pipda import register_verb, evaluate_expr

from ..core.types import is_scalar
from ..core.defaults import DEFAULT_COLUMN_PREFIX
from ..core.contexts import Context
from ..core.utils import (
    arg_match,
    check_column_uniqueness,
    logger,
    get_option,
    regcall,
)
from ..core.exceptions import ColumnNotExistingError
from ..core.grouped import DatarGroupBy, DatarRowwise

from .group_data import group_vars, group_keys
from .group_by import group_by_drop_default


@register_verb(DataFrame, context=Context.PENDING)
def summarise(
    _data: DataFrame,
    *args: Any,
    _groups: str = None,
    **kwargs: Any,
) -> DataFrame:
    """Summarise each group to fewer rows

    See https://dplyr.tidyverse.org/reference/summarise.html

    Both input and the summarised data can be recycled, but separately.

    Aliases - `summarize`

    Examples:
        >>> df = tibble(x=[1,2,3,4])
        >>> df >> summarise(y=sum(f.x), z=f.y*2)
        >>> #   y  z
        >>> # 0 10 20
        >>> df >> summarise(y=sum(f.x), z=f.x+f.y) # fail

        But they should not be mixed in later argument. For example:
        >>> df = tibble(x=[1,2,3,4], g=list('aabb')) >> group_by(f.g)
        >>> df >> summarise(n=n() + f.x)
        >>> # as expected:
        >>>      g  n
        >>> # 0  a  3
        >>> # 1  a  4
        >>> # 2  b  5
        >>> # 3  b  6
        >>> # [Groups: ['g'] (n=2)]
        >>> # However:
        >>> df >> summarise(y=1, n=n() + f.y)
        >>> # n() will be recycling output instead of input
        >>> #    g  y  n
        >>> # 0  a  1  2
        >>> # 1  b  1  2

    Args:
        _groups: Grouping structure of the result.
            - "drop_last": dropping the last level of grouping.
            - "drop": All levels of grouping are dropped.
            - "keep": Same grouping structure as _data.
            - "rowwise": Each row is its own group.
        *args: and
        **kwargs: Name-value pairs, where value is the summarized
            data for each group

    Returns:
        The summary dataframe.
    """
    check_column_uniqueness(
        _data, "Can't transform a data frame with duplicate names"
    )
    _groups = arg_match(
        _groups, "_groups", ["drop", "drop_last", "keep", "rowwise", None]
    )

    out = _summarise_build(_data, *args, **kwargs)
    gvars = regcall(group_vars, _data)
    if _groups is None:
        if out.shape[0] == 1 and not isinstance(_data, DatarRowwise):
            _groups = "drop_last"
        else:
            _groups = "keep"

    if _groups == "drop_last":
        if len(gvars) > 1:
            if get_option("dplyr_summarise_inform"):
                logger.info(
                    "`summarise()` has grouped output by "
                    "%s (override with `_groups` argument)",
                    gvars[:-1],
                )
            out = DatarGroupBy.from_grouped(
                out.obj.groupby(
                    gvars[:-1],
                    observed=_data.attrs["_grouped"].observed,
                    sort=_data.attrs["_sort"].sort,
                )
            )

    elif _groups == "keep" and gvars:
        if get_option("dplyr_summarise_inform"):
            logger.info(
                "`summarise()` has grouped output by "
                "%s (override with `_groups` argument)",
                gvars,
            )
        out = DatarGroupBy.from_grouped(
            out.groupby(
                gvars,
                observed=_data.attrs["_grouped"].observed,
                sort=_data.attrs["_grouped"].sort,
            )
        )

    elif _groups == "rowwise":
        out = DatarRowwise.from_grouped(
            out.groupby(
                list(range(_data.shape[0])),
                observed=group_by_drop_default(_data),
                sort=False,
            )
        )

    elif isinstance(_data, DatarRowwise) and get_option(
        "dplyr_summarise_inform"
    ):
        logger.info(
            "`summarise()` has ungrouped output. "
            "You can override using the `_groups` argument."
        )

    # else: # drop
    return out


summarize = summarise


def _summarise_build(_data: DataFrame, *args: Any, **kwargs: Any) -> DataFrame:
    """Build summarise result"""
    if isinstance(_data, DatarGroupBy):
        result_index = _data.attrs["_grouped"].grouper.result_index
    else:
        result_index = Index([0])

    outframe = DataFrame(index=result_index)
    for key, val in chain(enumerate(args), kwargs.items()):
        outframe = _summarise_single_argument(
            _data,
            outframe,
            key,
            val,
            result_index,
        )

    if isinstance(_data, DatarGroupBy):
        return outframe.reset_index()
    return outframe.reset_index(drop=True)


def _is_series_compatible(
    ser: Union[Series, SeriesGroupBy, DataFrame], index: Index
) -> bool:
    """Check if given series compatible with given index"""
    if not isinstance(ser, SeriesGroupBy):
        ser = ser.groupby(ser.index)

    return ser.grouper.result_index.equals(index)


def _summarise_single_argument(
    data: DataFrame,
    outframe: DataFrame,
    key: Union[int, str],
    value: Any,
    result_index: Index,
) -> DataFrame:
    """Do summarise for a single argument"""
    try:
        value = evaluate_expr(value, outframe, Context.EVAL)
    except (KeyError, ColumnNotExistingError):
        # also recycle input
        value = evaluate_expr(value, data, Context.EVAL)

    if isinstance(value, (Series, SeriesGroupBy)):
        obj = getattr(value, "obj", value)
        key = obj.name if isinstance(key, int) else key

        # check if index matches
        if not _is_series_compatible(value, result_index):
            logger.warning("Incompatible Series, ignoring index.")
            obj = obj.values

        outframe = _summarise_single_col(outframe, key, obj, result_index)

    elif isinstance(value, DataFrame):
        to_dict_type = "series"

        if not _is_series_compatible(value, result_index):
            logger.warning("Incompatible DataFrame, ignoring index.")
            to_dict_type = "list"

        for col, val in value.to_dict(to_dict_type).items():
            outframe = _summarise_single_col(
                outframe,
                col if isinstance(key, int) else f"{key}${col}",
                value,
                result_index,
            )

    elif isinstance(value, dict):
        for col, val in value.items():
            if isinstance(val, (Series, SeriesGroupBy)):
                obj = getattr(val, "obj", val)
                if not _is_series_compatible(val, result_index):
                    obj = obj.values
            else:
                obj = val

            outframe = _summarise_single_col(
                outframe,
                col if isinstance(key, int) else f"{key}${col}",
                obj,
                result_index,
            )

    else:
        if isinstance(key, int):
            key = (
                value
                if isinstance(value, str)
                else f"{DEFAULT_COLUMN_PREFIX}{key}"
            )

        outframe = _summarise_single_col(outframe, key, value, result_index)

    return outframe


def _summarise_single_col(
    outframe: DataFrame,
    key: str,
    value: Any,
    result_index: Index,
) -> DataFrame:
    """Mutate a single column to data, return the new column name"""
    if value is None:
        return

    if is_scalar(value):
        outframe[key] = value

    elif isinstance(value, Series):
        if len(value) == outframe.shape[0]:
            outframe[key] = value
        else:
            outframe = outframe.loc[value.index, :]
            outframe[key] = value

    else:
        index = result_index.repeat(len(value))
        value = Series(value * len(result_index), index=index)
        outframe = outframe.loc[index, :]
        outframe[key] = value

    return outframe
