"""Arrange rows by column values

See source https://github.com/tidyverse/dplyr/blob/master/R/arrange.R
"""
from pipda import register_verb

from ..core.backends.pandas import DataFrame

from ..core.contexts import Context
from ..core.tibble import TibbleGrouped
from ..core.exceptions import NameNonUniqueError
from ..base import union
from .mutate import mutate


@register_verb(DataFrame, context=Context.PENDING)
def arrange(_data, *args, _by_group=False, **kwargs):
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
    if not args and not kwargs and not _by_group:
        return _data.copy()

    if not _data.columns.is_unique:
        raise NameNonUniqueError(
            "Cannot arrange a data frame with duplicate names."
        )

    gvars = getattr(_data, "group_vars", [])

    sorting_df = mutate(_data, *args, __ast_fallback="normal", **kwargs)
    if _by_group:
        sorting_cols = union(
            gvars,
            sorting_df._datar["mutated_cols"],
            __ast_fallback="normal",
        )
    else:
        sorting_cols = sorting_df._datar["mutated_cols"]

    sorting_df = DataFrame(sorting_df, copy=False).sort_values(
        list(sorting_cols), na_position="last"
    )
    out = _data.reindex(sorting_df.index)
    if isinstance(_data, TibbleGrouped):
        out.reset_index(drop=True, inplace=True)

    return out
