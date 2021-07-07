"""Create, modify, and delete columns

See source https://github.com/tidyverse/dplyr/blob/master/R/mutate.R
"""

from typing import Any, Tuple, List, Union
from pandas import DataFrame
from pipda import register_verb, evaluate_expr, ContextBase

from ..core.contexts import Context, ContextEval
from ..core.utils import (
    dedup_name,
    recycle_value,
    arg_match,
    df_setitem,
    name_mutatable_args,
    reconstruct_tibble,
)
from ..core.defaults import DEFAULT_COLUMN_PREFIX
from ..core.grouped import DataFrameGroupBy
from ..base import setdiff, union, intersect, c, NA
from .group_by import group_by_drop_default
from .group_data import group_vars, group_data
from .relocate import relocate


@register_verb(
    DataFrame,
    context=Context.PENDING,
    extra_contexts={"_before": Context.SELECT, "_after": Context.SELECT},
)
def mutate(
    _data: DataFrame,
    *args: Any,
    _keep: str = "all",
    _before: Union[int, str] = None,
    _after: Union[int, str] = None,
    base0_: bool = None,
    **kwargs: Any,
) -> DataFrame:
    # pylint: disable=too-many-branches
    """Adds new variables and preserves existing ones

    The original API:
    https://dplyr.tidyverse.org/reference/mutate.html

    Args:
        _data: A data frame
        _keep: allows you to control which columns from _data are retained
            in the output:
            - "all", the default, retains all variables.
            - "used" keeps any variables used to make new variables;
              it's useful for checking your work as it displays inputs and
              outputs side-by-side.
            - "unused" keeps only existing variables not used to make new
                variables.
            - "none", only keeps grouping keys (like transmute()).
        _before: and
        _after: Optionally, control where new columns should appear
            (the default is to add to the right hand side).
            See relocate() for more details.
        base0_: Whether `_before` and `_after` are 0-based if given by indexes.
            If not provided, will use `datar.base.get_option('index.base.0')`
        *args: and
        **kwargs: Name-value pairs. The name gives the name of the column
            in the output. The value can be:
            - A vector of length 1, which will be recycled to the correct
                length.
            - A vector the same length as the current group (or the whole
                data frame if ungrouped).
            - None to remove the column

    Returns:
        An object of the same type as _data. The output has the following
        properties:
        - Rows are not affected.
        - Existing columns will be preserved according to the _keep
            argument. New columns will be placed according to the
            _before and _after arguments. If _keep = "none"
            (as in transmute()), the output order is determined only
            by ..., not the order of existing columns.
        - Columns given value None will be removed
        - Groups will be recomputed if a grouping variable is mutated.
        - Data frame attributes are preserved.
    """
    keep = arg_match(_keep, "_keep", ["all", "unused", "used", "none"])

    context = ContextEval()
    cols, removed = _mutate_cols(_data, context, *args, **kwargs)
    if cols is None:
        cols = DataFrame(index=_data.index)

    out = _data.copy()
    # order is the same as _data
    out[cols.columns.tolist()] = cols
    # out.columns.difference(removed)
    # changes column order when removed == []
    out = out[setdiff(out.columns, removed)]
    if _before is not None or _after is not None:
        new = setdiff(cols.columns, _data.columns)
        out = relocate(out, *new, _before=_before, _after=_after, base0_=base0_)

    if keep == "all":
        keep = out.columns
    elif keep == "unused":
        used = context.used_refs.keys()
        unused = setdiff(_data.columns, used)
        keep = intersect(
            out.columns, c(group_vars(_data), unused, cols.columns)
        )
    elif keep == "used":
        used = context.used_refs.keys()
        keep = intersect(out.columns, c(group_vars(_data), used, cols.columns))
    else:  # keep == 'none':
        keep = union(
            setdiff(group_vars(_data), cols.columns),
            intersect(cols.columns, out.columns),
        )

    out = out[keep]
    return out.loc[[], :] if len(_data) == 0 else out


@mutate.register(DataFrameGroupBy, context=Context.PENDING)
def _(
    _data: DataFrameGroupBy,
    *args: Any,
    _keep: str = "all",
    _before: str = None,
    _after: str = None,
    base0_: bool = None,
    **kwargs: Any,
) -> DataFrameGroupBy:
    """Mutate on DataFrameGroupBy object"""

    def apply_func(df):
        index = df.attrs["_group_index"]
        rows = df.attrs["_group_data"].loc[index, "_rows"]
        ret = df.reset_index(drop=True) >> mutate(
            *args,
            _keep=_keep,
            _before=_before,
            _after=_after,
            base0_=base0_,
            **kwargs,
        )
        ret.index = rows
        return ret

    out = _data.datar_apply(apply_func, _drop_index=False)
    if out is not None:
        # keep the original row order
        out.sort_index(inplace=True)
        # not only DataFrameGroupBy but also DataFrameRowwise
        return reconstruct_tibble(_data, out, keep_rowwise=True)

    # 0-row
    named = name_mutatable_args(*args, **kwargs)
    df = DataFrame({key: [] for key in named})
    out = _data.copy()
    out[df.columns.tolist()] = df
    return _data.__class__(
        out,
        _group_vars=group_vars(_data),
        _group_drop=group_by_drop_default(_data),
        _group_data=group_data(_data),
    )


@register_verb(DataFrame, context=Context.PENDING)
def transmute(
    _data: DataFrame,
    *args: Any,
    _before: Union[int, str] = None,
    _after: Union[int, str] = None,
    base0_: bool = None,
    **kwargs: Any,
) -> DataFrame:
    """Mutate with _keep='none'

    See Also:
        [`mutate()`](datar.dplyr.mutate.mutate).
    """
    return _data >> mutate(
        *args,
        _keep="none",
        _before=_before,
        _after=_after,
        base0_=base0_,
        **kwargs,
    )


def _mutate_cols(
    data: DataFrame,  # TODO: data and context could be kwargs
    context: ContextBase,
    *args: Any,
    **kwargs: Any,
) -> Tuple[DataFrame, List[str]]:
    """Mutate columns"""
    if not args and not kwargs:
        return None, []
    data = data.copy()
    named_mutatables = name_mutatable_args(*args, **kwargs)
    new_columns = []
    removed = []
    add_new_name = True
    for name, mutatable in named_mutatables.items():
        ddp_name = dedup_name(name, list(named_mutatables))
        # if not a dedup name, it's a new name
        add_new_name = ddp_name == name
        mutatable = evaluate_expr(mutatable, data, context)
        if mutatable is None:
            if ddp_name in data:
                removed.append(ddp_name)
                data.drop(columns=[ddp_name], inplace=True)
            # be silent if name doesn't exist
            continue

        if isinstance(mutatable, DataFrame):
            if mutatable.shape[1] == 0 and not ddp_name.startswith(
                DEFAULT_COLUMN_PREFIX
            ):
                data = df_setitem(
                    data, ddp_name, [NA] * max(mutatable.shape[0], 1)
                )
                if add_new_name:
                    new_columns.append(ddp_name)
            else:
                for col in mutatable.columns:
                    new_name = (
                        col
                        if ddp_name.startswith(DEFAULT_COLUMN_PREFIX)
                        else f"{ddp_name}${col}"
                    )
                    coldata = recycle_value(
                        mutatable[col], data.shape[0], ddp_name
                    )
                    data = df_setitem(data, new_name, coldata)

                    if add_new_name:
                        new_columns.append(new_name)
        else:
            mutatable = recycle_value(mutatable, data.shape[0], ddp_name)
            data = df_setitem(data, ddp_name, mutatable)

            if add_new_name:
                new_columns.append(ddp_name)

    # keep column order
    return data[new_columns], removed
