"""Arrange rows by column values

See source https://github.com/tidyverse/dplyr/blob/master/R/arrange.R
"""
from typing import Any, Iterable, Mapping, Tuple
from pandas import DataFrame
from pipda import register_verb
from pipda.symbolic import DirectRefAttr, DirectRefItem
from pipda.function import FastEvalFunction
from pipda.utils import CallingEnvs

from ..core.contexts import Context
from ..core.utils import check_column_uniqueness, reconstruct_tibble
from ..base import union
from .group_data import group_vars
from .group_by import ungroup
from .mutate import mutate
from .desc import desc


@register_verb(DataFrame, context=Context.PENDING)
def arrange(
    _data: DataFrame, *args: Any, _by_group: bool = False, **kwargs: Any
) -> DataFrame:
    """orders the rows of a data frame by the values of selected columns.

    The original API:
    https://dplyr.tidyverse.org/reference/arrange.html

    Args:
        _data: A data frame
        *series: Variables, or functions of variables.
            Use desc() to sort a variable in descending order.
        _by_group: If TRUE, will sort first by grouping variable.
            Applies to grouped data frames only.
        **kwargs: Name-value pairs that apply with mutate

    Returns:
        An object of the same type as _data.
        The output has the following properties:
            All rows appear in the output, but (usually) in a different place.
            Columns are not modified.
            Groups are not modified.
            Data frame attributes are preserved.
    """
    if not args and not kwargs:
        return _data

    check_column_uniqueness(
        _data, "Cannot arrange a data frame with duplicate names"
    )

    # See if we don't need to mutate
    # If all series are the ones from the _data itself
    if not kwargs:
        by = _series_cols(args, _data.columns)
        if by is not None:
            if _by_group:
                gvars = group_vars(_data, __calling_env=CallingEnvs.REGULAR)
                gby = dict(zip(gvars, [True] * len(gvars)))
                gby.update(by)
                by = gby

            out = _data.sort_values(
                list(by), ascending=list(by.values())
            ).reset_index(drop=True)
            return reconstruct_tibble(_data, out, keep_rowwise=True)

    if not _by_group:
        sorting_df = mutate(
            ungroup(_data, __calling_env=CallingEnvs.REGULAR),
            *args,
            **kwargs,
            _keep="none",
            __calling_env=CallingEnvs.REGULAR,
        )
        sorting_df = sorting_df.sort_values(by=sorting_df.columns.tolist())
    else:
        gvars = group_vars(_data, __calling_env=CallingEnvs.REGULAR)
        sorting_df = ungroup(
            mutate(
                _data,
                *args,
                **kwargs,
                _keep="none",
                __calling_env=CallingEnvs.REGULAR,
            ),
            __calling_env=CallingEnvs.REGULAR,
        )
        by = union(gvars, sorting_df.columns, __calling_env=CallingEnvs.REGULAR)
        sorting_df = sorting_df.sort_values(by=by)

    out = _data.loc[sorting_df.index, :].reset_index(drop=True)

    return reconstruct_tibble(_data, out, keep_rowwise=True)


def _series_col(arg: Any, columns: Iterable[str]) -> str:
    """Turn a single arg into name and desc if possible"""
    if (
        isinstance(arg, (DirectRefAttr, DirectRefItem))
        and arg._pipda_ref in columns
    ):
        return arg._pipda_ref

    return None


def _series_cols(
    args: Tuple,
    columns: Iterable[str],
) -> Mapping[str, bool]:
    """Check if one of the args is a series column or columns in original df"""
    out = {}
    for arg in args:
        sercol = _series_col(arg, columns)
        if sercol:
            out[sercol] = True

        elif (
            isinstance(arg, FastEvalFunction)
            and arg._pipda_func is desc.__origfunc__
        ):
            for col in arg._pipda_args:
                sercol = _series_col(col, columns)
                if sercol is None:
                    return None
                out[sercol] = False

        else:
            return None

    return out
