"""Subset columns using their names and types

See source https://github.com/tidyverse/dplyr/blob/master/R/select.R
"""
from typing import Any, Iterable, Mapping, Sequence, Tuple, Union

from pandas import DataFrame, Index
from pipda import register_verb

from ..core.contexts import Context
from ..core.tibble import Tibble, TibbleGrouped
from ..core.utils import vars_select, logger, regcall
from ..core.collections import Inverted
from ..base import setdiff, union, intersect
from .group_data import group_vars


@register_verb(DataFrame, context=Context.SELECT)
def select(
    _data: DataFrame,
    *args: Union[str, Iterable, Inverted],
    **kwargs: Mapping[str, str],
) -> Tibble:
    """Select (and optionally rename) variables in a data frame

    See original API
    https://dplyr.tidyverse.org/reference/select.html

    To exclude columns use `~` instead of `-`. For example, to exclude last
    column: `select(df, ~c(-1))`.

    To use column name in slice: `f[f.col1:f.col2]`. If you don't want `col2`
    to be included: `f[f.col1:f.col2:0]`

    Args:
        *columns: The columns to select
        **renamings: The columns to rename and select in new => old column way.
        base0_: Whether the columns are 0-based if given by indexes
            If not provided, will use `datar.base.get_option('index.base.0')`

    Returns:
        The dataframe with select columns
    """
    all_columns = _data.columns
    gvars = regcall(group_vars, _data)
    selected_idx, new_names = _eval_select(
        all_columns,
        *args,
        **kwargs,
        _group_vars=gvars,
    )
    out = _data.copy()
    # nested dfs?
    if new_names:
        out.rename(columns=new_names, inplace=True)
        if (
            isinstance(out, TibbleGrouped)
            and len(regcall(intersect, gvars, new_names)) > 0
        ):
            out._datar["group_vars"] = [
                new_names.get(gvar, gvar) for gvar in gvars
            ]

    return out.iloc[:, selected_idx]


def _eval_select(
    _all_columns: Index,
    *args: Any,
    _group_vars: Sequence[str],
    **kwargs: Any,
) -> Tuple[Sequence[int], Mapping[str, str]]:
    """Evaluate selections to get locations

    Returns:
        A tuple of (selected columns, dict of old-to-new renaming columns)
    """
    selected_idx = vars_select(
        _all_columns,
        *args,
        *kwargs.values(),
    )
    missing = regcall(setdiff, _group_vars, _all_columns[selected_idx])
    if len(missing) > 0:
        logger.info("Adding missing grouping variables: %s", missing)

    selected_idx = regcall(
        union,
        _all_columns.get_indexer_for(_group_vars),
        selected_idx
    )

    if not kwargs:
        return selected_idx, None

    rename_idx = vars_select(
        _all_columns,
        *kwargs.values(),
    )

    # check uniqueness
    # for ridx in rename_idx:
    #     dupidx = _all_columns.get_indexer_for([_all_columns[ridx]])
    #     if dupidx.size > 1:
    #         raise ValueError(
    #             "Names must be unique. Name "
    #             f'"{_all_columns[ridx]}" found at locations {list(dupidx)}.'
    #         )

    new_names = dict(zip(_all_columns[rename_idx], kwargs))
    return selected_idx, new_names