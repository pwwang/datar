"""Group by verbs and functions
See source https://github.com/tidyverse/dplyr/blob/master/R/group-by.r
"""

from typing import Any, Mapping, Optional

from pandas import DataFrame
from pipda import register_verb
from pipda.symbolic import DirectRefAttr, DirectRefItem
from pipda.utils import Expression

from ..core.grouped import DataFrameGroupBy
from ..core.contexts import Context
from ..core.utils import name_mutatable_args, vars_select
from ..core.exceptions import ColumnNotExistingError
from ..base.funcs import setdiff, union

from .group_data import group_vars


@register_verb(DataFrame, context=Context.PENDING)
def group_by(
        _data: DataFrame,
        *args: Any,
        _add: bool = False, # not working, since _data is not grouped
        _drop: Optional[bool] = None,
        **kwargs: Any
) -> DataFrameGroupBy:
    """Takes an existing tbl and converts it into a grouped tbl where
    operations are performed "by group"

    Args:
        _data: The dataframe
        *args: variables or computations to group by.
        **kwargs: Extra variables to group the dataframe

    Return:
        A DataFrameGroupBy object
    """
    if _drop is None:
        _drop = group_by_drop_default(_data)

    groups = group_by_prepare(_data, *args, **kwargs, _add=_add)
    return DataFrameGroupBy(
        groups['data'],
        _group_vars=groups['group_names'],
        _drop=_drop
    )

@register_verb(DataFrame, context=Context.SELECT)
def ungroup(x: DataFrame, *cols: str) -> DataFrame:
    if cols:
        raise ValueError(f'`*cols` is not empty.')
    return x

@ungroup.register(DataFrameGroupBy, context=Context.SELECT)
def _(x: DataFrameGroupBy, *cols: str) -> DataFrame:
    if not cols:
        return DataFrame(x, index=x.index)
    old_groups = group_vars(x)
    to_remove = vars_select(x.columns, *cols)
    new_groups = setdiff(old_groups, x.columns[to_remove])

    return group_by(x, *new_groups)

# @ungroup.register(RowwiseDataFrame, context=Context.SELECT)
# def _(x: RowwiseDataFrame, *cols: str) -> DataFrame:
#     if cols:
#         raise ValueError(f'`*cols` is not empty.')
#     return DataFrame(x)

def group_by_prepare(
        _data: DataFrame,
        *args: Any,
        _add: bool = False,
        **kwargs: Any
) -> Mapping[str, Any]:
    computed_columns = add_computed_columns(
        _data,
        *args,
        _fn="group_by",
        **kwargs
    )

    out = computed_columns['data']
    group_names = computed_columns['added_names']
    if _add:
        group_names = union(group_vars(_data), group_names)

    unknown = setdiff(group_names, out.columns)
    if unknown:
        raise ValueError(
            "Must group by variables found in `_data`.",
            f"Column `{unknown}` not found."
        )

    return {'data': out, 'group_names': group_names}

def add_computed_columns(
        _data: DataFrame,
        *args: Any,
        _fn: str = "group_by",
        **kwargs: Any
) -> Mapping[str, Any]:
    """Add mutated columns if necessary"""
    from .mutate import mutate_cols
    named = name_mutatable_args(*args, **kwargs)

    if any(
            isinstance(val, Expression) and
            not isinstance(val, (DirectRefAttr, DirectRefItem))
            for val in named.values()
    ):
        context = Context.EVAL.value
        try:
            cols, nonexists = mutate_cols(
                ungroup(_data),
                context,
                *args,
                **kwargs
            )
        except Exception as exc:
            raise ValueError(
                f"Problem adding computed columns in `{_fn}()`."
            ) from exc

        out = _data.copy()
        col_names = cols.columns.tolist()
        out[col_names] = cols
    else:
        out = _data
        col_names = list(named)
        nonexists = setdiff(col_names, out.columns)

    if nonexists:
        raise ColumnNotExistingError(
            'Must group by variables found in `_data`. '
            f'Columns {nonexists} are not found.'
        )

    return {'data': out, 'added_names': col_names}

def group_by_drop_default(_tbl) -> bool:
    """Get the groupby _drop attribute of dataframe"""
    return _tbl.attrs.get('groupby_drop', True)
