"""Create, modify, and delete columns

See source https://github.com/tidyverse/dplyr/blob/master/R/mutate.R
"""

from contextlib import suppress

from pipda import register_verb, evaluate_expr, ReferenceAttr, ReferenceItem

from ..core.backends.pandas import DataFrame

from ..core.contexts import Context, ContextEvalRefCounts
from ..core.utils import arg_match, name_of, regcall
from ..core.broadcast import add_to_tibble
from ..core.tibble import reconstruct_tibble
from ..base import setdiff, union, intersect, c
from ..tibble import as_tibble
from .group_data import group_vars
from .relocate import relocate


@register_verb(
    DataFrame,
    context=Context.PENDING,
    extra_contexts={"_before": Context.SELECT, "_after": Context.SELECT},
)
def mutate(
    _data,
    *args,
    _keep="all",
    _before=None,
    _after=None,
    **kwargs,
):
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
    data = regcall(as_tibble, _data.copy())
    all_columns = data.columns

    mutated_cols = []
    context = ContextEvalRefCounts()
    for val in args:
        if (
            isinstance(val, (ReferenceItem, ReferenceAttr))
            and val._pipda_level == 1
            and val._pipda_ref in data
        ):
            mutated_cols.append(val._pipda_ref)
            continue

        bkup_name = name_of(val)
        val = evaluate_expr(val, data, context)
        if val is None:
            continue

        if isinstance(val, DataFrame):
            mutated_cols.extend(val.columns)
            data = add_to_tibble(data, None, val, broadcast_tbl=False)
        else:
            key = name_of(val) or bkup_name
            mutated_cols.append(key)
            data = add_to_tibble(data, key, val, broadcast_tbl=False)

    for key, val in kwargs.items():
        val = evaluate_expr(val, data, context)
        if val is None:
            with suppress(KeyError):
                data.drop(columns=[key], inplace=True)
        else:
            data = add_to_tibble(data, key, val, broadcast_tbl=False)
            if isinstance(val, DataFrame):
                mutated_cols.extend({f"{key}${col}" for col in val.columns})
            else:
                mutated_cols.append(key)

    # names start with "_" are temporary names if they are used
    tmp_cols = [
        mcol
        for mcol in mutated_cols
        if mcol.startswith("_")
        and mcol in context.used_refs
        and mcol not in _data.columns
    ]
    # columns can be removed later
    # df >> mutate(Series(1, name="z"), z=None)
    mutated_cols = regcall(intersect, mutated_cols, data.columns)
    mutated_cols = regcall(setdiff, mutated_cols, tmp_cols)
    # new cols always at last
    # data.columns.difference() does not keep order

    data = data.loc[:, regcall(setdiff, data.columns, tmp_cols)]

    if _before is not None or _after is not None:
        new_cols = regcall(setdiff, mutated_cols, _data.columns)
        data = regcall(
            relocate,
            data,
            *new_cols,
            _before=_before,
            _after=_after,
        )

    if keep == "all":
        keep = data.columns
    elif keep == "unused":
        used = list(context.used_refs)
        unused = regcall(setdiff, all_columns, used)
        keep = regcall(intersect, data.columns, c(gvars, unused, mutated_cols))
    elif keep == "used":
        used = list(context.used_refs)
        keep = regcall(intersect, data.columns, c(gvars, used, mutated_cols))
    else:  # keep == 'none':
        keep = regcall(
            union,
            regcall(setdiff, gvars, mutated_cols),
            regcall(intersect, mutated_cols, data.columns),
        )

    data = data[keep]
    # redo grouping if original columns changed
    # so we don't have discripency on
    # df.x.obj when df is grouped
    if intersect(_data.columns, mutated_cols).size > 0:
        data = reconstruct_tibble(_data, data)

    # used for group_by
    data._datar["mutated_cols"] = mutated_cols
    return data


@register_verb(DataFrame, context=Context.PENDING)
def transmute(
    _data,
    *args,
    _before=None,
    _after=None,
    **kwargs,
):
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
        **kwargs,
    )
