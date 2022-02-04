"""Create, modify, and delete columns

See source https://github.com/tidyverse/dplyr/blob/master/R/mutate.R
"""

from typing import Any, List, Union
from itertools import chain

from pandas import DataFrame, Series
from pandas.core.groupby import SeriesGroupBy
from pipda import register_verb, evaluate_expr, ContextBase


from ..core.grouped import DatarGroupBy, DatarRowwise
from ..core.contexts import Context, ContextEval
from ..core.utils import arg_match, regcall, broadcast
from ..core.defaults import DEFAULT_COLUMN_PREFIX
from ..base import setdiff, union, intersect, c
from ..tibble import tibble
from .group_data import group_vars
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

    gvars = regcall(group_vars, _data)
    data = _data.copy()
    all_columns = data.columns

    mutated_cols = []
    context = ContextEval()
    for key, val in chain(enumerate(args), kwargs.items()):
        mut_cols = _mutate_single_argument(data, key, val, context)
        mutated_cols.extend(mut_cols)
    # names end with "_" are temporary names
    tmp_cols = [
        mcol
        for mcol in mutated_cols
        if mcol.endswith("_") and mcol in context.used_refs
    ]
    data = data.loc[:, data.columns.difference(tmp_cols)]
    mutated_cols = regcall(setdiff, mutated_cols, tmp_cols)

    if _before is not None or _after is not None:
        new = regcall(setdiff, mutated_cols, all_columns)
        data = regcall(
            relocate,
            data,
            *new,
            _before=_before,
            _after=_after,
            base0_=base0_,
        )

    if keep == "all":
        keep = data.columns
    elif keep == "unused":
        used = context.used_refs.keys()
        unused = regcall(setdiff, all_columns, used)
        keep = regcall(intersect, data.columns, c(gvars, unused, mutated_cols))
    elif keep == "used":
        used = context.used_refs.keys()
        keep = regcall(intersect, data.columns, c(gvars, used, mutated_cols))
    else:  # keep == 'none':
        keep = regcall(
            union,
            regcall(setdiff, gvars, mutated_cols),
            regcall(intersect, mutated_cols, data.columns),
        )

    data = data[keep]
    data = data.loc[[], :] if len(data) == 0 else data
    data.attrs["_mutated_cols"] = mutated_cols
    # redo grouping if group vars are mutated
    # rowwise data frame doesn't need it
    if not isinstance(data, DatarRowwise) and intersect(gvars, mutated_cols):
        grouped = data.attrs["_grouped"]
        return DatarGroupBy.from_grouped(
            grouped.obj.copy().groupby(
                gvars,
                observed=grouped.observed,
                sort=grouped.sort,
            )
        )

    return data


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
    return regcall(
        mutate,
        _data,
        *args,
        _keep="none",
        _before=_before,
        _after=_after,
        base0_=base0_,
        **kwargs,
    )


# multiple dispatch?
def _mutate_single_argument(
    data: DataFrame,
    key: Union[int, str],
    value: Any,
    context: ContextBase,
) -> List[str]:
    """Mutate a single column/argument to data and return the new column name"""
    out = []
    value = evaluate_expr(value, data, context)
    grouped = data.attrs.get("_grouped", None)
    grouper = getattr(grouped, "grouper", None)
    if isinstance(key, int):
        if isinstance(value, Series):
            _mutate_single_col(data, value.name, value)
            out.append(value.name)
        elif isinstance(value, SeriesGroupBy):
            _mutate_single_col(data, value.obj.name, value)
            out.append(value.obj.name)
        elif isinstance(value, dict):
            for key, val in value.items():
                _mutate_single_col(data, key, val)
                if val is not None:
                    out.append(key)
        elif isinstance(value, DataFrame):
            for key, val in value.to_dict("series").items():
                if grouper is not None:
                    val = val.groupby(grouper)
                _mutate_single_col(data, key, val)
                out.append(key)
        elif isinstance(value, str):
            _mutate_single_col(data, value, value)
            out.append(value)
        else:
            key = f"{DEFAULT_COLUMN_PREFIX}{key}"
            _mutate_single_col(data, key, value)
            if value is not None:
                out.append(key)

    elif isinstance(value, (Series, SeriesGroupBy)):
        _mutate_single_col(data, key, value)
        out.append(key)

    elif isinstance(value, dict):
        value = tibble(value)
        for col, val in value.to_dict("series").items():
            _mutate_single_col(data, f"{key}${col}", val)
            out.append(f"{key}${col}")

    elif isinstance(value, DataFrame):
        for col, val in value.to_dict("series").items():
            if grouper is not None:
                val = val.groupby(grouper)
            _mutate_single_col(data, f"{key}${col}", val)
            out.append(f"{key}${col}")

    else:
        _mutate_single_col(data, key, value)
        if value is not None:
            out.append(key)

    return out


def _mutate_single_col(data: DataFrame, key: str, value: Any) -> None:
    """Mutate a single column to data, return the new column name"""
    if value is None:
        try:
            del data[key]
        except KeyError:
            ...

    elif data.shape[0] == 0:
        data[key] = []

    elif data.shape[1] == 0:
        data[key] = value

    else:
        # use data[col] instead of
        # data.iloc[:, 0] to trigger DatarGroupBy.__getitem__()
        data[key] = broadcast(value, data[data.columns[0]])[0]
