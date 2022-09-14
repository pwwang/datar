"""Subset columns using their names and types

See source https://github.com/tidyverse/dplyr/blob/master/R/select.R
"""
from typing import Any, Iterable, Mapping, Sequence, Tuple, Union

from pipda import register_verb

from ..core.backends.pandas import DataFrame, Index

from ..core.contexts import Context
from ..core.tibble import Tibble, TibbleGrouped
from ..core.utils import vars_select, logger
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
        *args: The columns to select
        **kwargs: The columns to rename and select in new => old column way.

    Returns:
        The dataframe with select columns
    """
    all_columns = _data.columns
    gvars = group_vars(_data, __ast_fallback="normal")
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
            and len(intersect(gvars, new_names, __ast_fallback="normal")) > 0
        ):
            out._datar["group_vars"] = [
                new_names.get(gvar, gvar) for gvar in gvars
            ]

    return out.iloc[:, selected_idx]


def _eval_select(
    _all_columns: Index,
    *args: Any,
    _group_vars: Sequence[str],
    _missing_gvars_inform: bool = True,
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

    if _missing_gvars_inform:
        missing = setdiff(
            _group_vars,
            _all_columns[selected_idx],
            __ast_fallback="normal",
        )
        if len(missing) > 0:
            logger.info("Adding missing grouping variables: %s", missing)

    selected_idx = union(
        _all_columns.get_indexer_for(_group_vars),
        selected_idx,
        __ast_fallback="normal",
    )

    if not kwargs:
        return selected_idx, None

    rename_idx = vars_select(_all_columns, *kwargs.values())
    new_names = dict(zip(_all_columns[rename_idx], kwargs))
    return selected_idx, new_names
