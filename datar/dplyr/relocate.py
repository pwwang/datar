"""Relocate columns"""
from typing import Any, Union

from pandas import DataFrame
from pipda import register_verb

from ..core.contexts import Context
from ..base import setdiff, union
from .group_data import group_vars
from .select import _eval_select


@register_verb(DataFrame, context=Context.SELECT)
def relocate(
    _data: DataFrame,
    *args: Any,
    _before: Union[int, str] = None,
    _after: Union[int, str] = None,
    base0_: bool = None,
    **kwargs: Any,
) -> DataFrame:
    """change column positions

    See original API
    https://dplyr.tidyverse.org/reference/relocate.html

    Args:
        _data: A data frame
        *args: and
        **kwargs: Columns to rename and move
        _before: and
        _after: Destination. Supplying neither will move columns to
            the left-hand side; specifying both is an error.
        base0_: Whether `_before` and `_after` are 0-based if given by indexes.
            If not provided, will use `datar.base.get_option('index.base.0')`

    Returns:
        An object of the same type as .data. The output has the following
        properties:
        - Rows are not affected.
        - The same columns appear in the output, but (usually) in a
            different place.
        - Data frame attributes are preserved.
        - Groups are not affected
    """
    gvars = group_vars(_data)
    all_columns = _data.columns
    to_move, new_names = _eval_select(
        all_columns, *args, **kwargs, base0_=base0_, _group_vars=gvars
    )
    if _before is not None and _after is not None:
        raise ValueError("Must supply only one of `_before` and `_after`.")

    # length = len(all_columns)
    if _before is not None:
        where = min(
            _eval_select(all_columns, _before, _group_vars=[], base0_=base0_)[0]
        )
        if where not in to_move:
            to_move.append(where)

    elif _after is not None:
        where = max(
            _eval_select(all_columns, _after, _group_vars=[], base0_=base0_)[0]
        )
        if where not in to_move:
            to_move.insert(0, where)
    else:
        where = 0
        if where not in to_move:
            to_move.append(where)

    lhs = setdiff(range(where), to_move)
    rhs = setdiff(range(where + 1, _data.shape[1]), to_move)
    pos = union(lhs, union(to_move, rhs))
    out = _data.iloc[:, pos].copy()
    if new_names:
        out.rename(columns=new_names, inplace=True)

    return out
