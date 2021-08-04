"""Subset columns using their names and types

See source https://github.com/tidyverse/dplyr/blob/master/R/select.R
"""
from typing import Any, Iterable, List, Mapping, Tuple, Union

from pandas import DataFrame, Index, Series
from pipda import register_verb
from pipda.utils import CallingEnvs

from ..core.contexts import Context
from ..core.types import StringOrIter
from ..core.utils import position_at, vars_select, logger
from ..core.collections import Inverted
from ..core.grouped import DataFrameGroupBy
from ..base import setdiff, union
from .group_by import group_by_drop_default
from .group_data import group_data, group_vars


@register_verb(DataFrame, context=Context.SELECT)
def select(
    _data: DataFrame,
    *args: Union[StringOrIter, Iterable[Iterable], Inverted],
    base0_: bool = None,
    **kwargs: Mapping[str, str],
) -> DataFrame:
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
    gvars = group_vars(_data, __calling_env=CallingEnvs.REGULAR)
    selected, new_names = _eval_select(
        all_columns,
        *args,
        **kwargs,
        _group_vars=gvars,
        base0_=base0_,
    )
    out = _data.iloc[:, selected].copy()

    if new_names:
        out.rename(columns=new_names, inplace=True)

    if isinstance(_data, DataFrameGroupBy):
        gvars = [new_names.get(gvar, gvar) for gvar in gvars]
        gdata = group_data(_data, __calling_env=CallingEnvs.REGULAR)
        gdata.columns = gvars + ["_rows"]
        return _data.__class__(
            out,
            _group_vars=gvars,
            _group_drop=group_by_drop_default(_data),
            _group_data=gdata,
        )
    return out


def _eval_select(
    _all_columns: Index,
    *args: Any,
    _group_vars: Iterable[str],
    base0_: bool = None,
    **kwargs: Any,
) -> Tuple[List[int], Mapping[str, str]]:
    """Evaluate selections to get locations

    Returns:
        A tuple of (selected columns, dict of old-to-new renaming columns)
    """
    selected = vars_select(
        _all_columns,
        *args,
        *kwargs.values(),
        base0=base0_,
    )
    missing = setdiff(
        _group_vars, _all_columns[selected], __calling_env=CallingEnvs.REGULAR
    )
    if missing:
        logger.info("Adding missing grouping variables: %s", missing)

    selected = union(
        _all_columns.get_indexer_for(_group_vars),
        selected,
        __calling_env=CallingEnvs.REGULAR,
    )

    # dplyr takes new -> old
    # we transform it to old -> new for better access
    new_names = {}
    for key, val in kwargs.items():
        # key: new name
        # val: old name
        if isinstance(val, Series):
            val = val.name
        if isinstance(val, str):
            idx = _all_columns.get_indexer_for([val])
            if len(idx) > 1:
                raise ValueError(
                    "Names must be unique. "
                    f'Name "{val}" found at locations {list(idx)}.'
                )
        else:  # int
            # try:
            #   If out of bounds, it should be raised at getting missing
            val = _all_columns[
                position_at(val, len(_all_columns), base0=base0_)
            ]
            # except IndexError:
            #     raise ColumnNotExistingError(
            #         f'Location {val} does not exist.'
            #     ) from None
        new_names[val] = key
    return selected, new_names
