"""Summarise each group to fewer rows"""

from itertools import chain
from typing import Any, Union

import numpy
from pandas import DataFrame, Index, Series, concat
from pandas.core.groupby import SeriesGroupBy
from pipda import register_verb, evaluate_expr
from pipda.function import Function

from ..core.types import is_scalar
from ..core.defaults import DEFAULT_COLUMN_PREFIX
from ..core.contexts import Context, ContextEval, ContextBase
from ..core.utils import (
    arg_match,
    check_column_uniqueness,
    logger,
    get_option,
    regcall,
)
from ..core.exceptions import ColumnNotExistingError
from ..core.grouped import DatarGroupBy, DatarRowwise

from ..base import setdiff
from .group_data import group_vars
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
        allones = (
            not isinstance(_data, DatarGroupBy) and out.shape[0] == 1
        ) or (
            isinstance(_data, DatarGroupBy)
            and out.shape[0]
            == len(_data.attrs["_grouped"].grouper.result_index)
        )
        if allones and not isinstance(_data, DatarRowwise):
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
            out = DatarGroupBy.from_groupby(
                out.groupby(
                    gvars[:-1],
                    observed=_data.attrs["_grouped"].observed,
                    sort=_data.attrs["_grouped"].sort,
                )
            )

    elif _groups == "keep" and gvars:
        if get_option("dplyr_summarise_inform"):
            logger.info(
                "`summarise()` has grouped output by "
                "%s (override with `_groups` argument)",
                gvars,
            )
        grouped = _data.attrs["_grouped"]
        out = DatarGroupBy.from_groupby(
            out.groupby(
                gvars,
                observed=grouped.observed,
                sort=grouped.sort,
            )
        )

    elif _groups == "rowwise":
        out = DatarRowwise.from_groupby(
            out.groupby(
                list(range(_data.shape[0])),
                observed=group_by_drop_default(_data),
                sort=False,
            ),
            group_vars=gvars,
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
    context = ContextEval()
    for key, val in chain(enumerate(args), kwargs.items()):
        outframe = _summarise_single_argument(
            _data,
            outframe,
            key,
            val,
            result_index,
            context,
        )

    gvars = regcall(group_vars, _data)
    tmp_cols = [
        mcol
        for mcol in outframe.columns
        if mcol.startswith("_")
        and mcol in context.used_refs
        and mcol not in gvars
    ]
    outframe = outframe[outframe.columns.difference(tmp_cols)]

    if isinstance(_data, DatarRowwise):
        return concat(
            (_data.loc[outframe.index, gvars], outframe),
            axis=1,
        )

    if isinstance(_data, DatarGroupBy):
        # in case group variables are replaced
        reset_cols = regcall(setdiff, gvars, outframe.columns)
        outframe = outframe.reset_index(reset_cols)

    return outframe.reset_index(drop=True)


def _summarise_single_argument(
    data: DataFrame,
    outframe: DataFrame,
    key: Union[int, str],
    value: Any,
    result_index: Index,
    context: ContextBase,
) -> DataFrame:
    """Do summarise for a single argument"""
    envdata = outframe
    if outframe.shape[1] == 0 or (
        isinstance(value, Function)
        and getattr(value._pipda_func, "summarise_prefers_input", False)
    ):
        envdata = data

    try:
        value = evaluate_expr(value, envdata, context)
    except (KeyError, ColumnNotExistingError):
        # also recycle input
        value = evaluate_expr(value, data, context)

    if isinstance(value, Series):
        # x = f.x.mean()
        # Requires the index matches result_index
        # Note that it is not necessarily all-ones series
        # (only one record in each group)
        if not value.index.unique().equals(result_index):
            logger.warning("Incompatible Series, ignoring index.")
            value = value.values

        outframe = _summarise_single_col(
            outframe,
            key,
            value,
            result_index,
        )

    elif isinstance(value, SeriesGroupBy):
        # x = f.x + 1
        if not value.grouper.result_index.equals(result_index):
            raise ValueError("Incompatible SeriesGroupBy object.")

        outframe = _summarise_single_col(outframe, key, value, result_index)

    elif isinstance(value, DataFrame):
        if value.shape[0] > 0 and not value.index.equals(result_index):
            logger.warning("Incompatible DataFrame, ignoring index.")
            index = result_index.repeat(value.shape[0])
            value = value.loc[numpy.tile(value.index, len(result_index)), :]
            value.index = index
            outframe = outframe.loc[index, :]
            outframe[
                [
                    col if isinstance(key, int) else f"{key}${col}"
                    for col in value.columns
                ]
            ] = value

        else:
            for col, val in value.to_dict("series").items():
                outframe = _summarise_single_col(
                    outframe,
                    col if isinstance(key, int) else f"{key}${col}",
                    val,
                    result_index,
                )

    elif isinstance(value, dict):
        for col, val in value.items():
            colname = col if isinstance(key, int) else f"{key}${col}"
            outframe = _summarise_single_argument(
                data,
                outframe,
                colname,
                val,
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
        return outframe

    if is_scalar(value):
        outframe[key] = value

    elif isinstance(value, Series):
        # y = f.x.mean()
        outframe = outframe.loc[value.index, :]
        outframe[key] = value

    elif isinstance(value, SeriesGroupBy):
        # y = f.x + 1
        size = value.grouper.size()
        index = size.index.repeat(size)
        outframe = outframe.loc[index, :]
        value.obj.index = index
        outframe[key] = value.obj

    else:
        index = result_index.repeat(len(value))
        value = Series(value * len(result_index), index=index)
        outframe = outframe.loc[index, :]
        outframe[key] = value

    return outframe
