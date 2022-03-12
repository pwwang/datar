"""Relocate columns"""
from typing import Any, Union

from pandas import DataFrame
from pipda import register_verb

from ..core.contexts import Context
from ..core.utils import regcall
from ..core.tibble import Tibble, TibbleGrouped
from ..base import setdiff, union, intersect
from ..tibble import as_tibble
from .group_data import group_vars
from .select import _eval_select


@register_verb(DataFrame, context=Context.SELECT)
def relocate(
    _data: DataFrame,
    *args: Any,
    _before: Union[int, str] = None,
    _after: Union[int, str] = None,
    **kwargs: Any,
) -> Tibble:
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

    Returns:
        An object of the same type as .data. The output has the following
        properties:
        - Rows are not affected.
        - The same columns appear in the output, but (usually) in a
            different place.
        - Data frame attributes are preserved.
        - Groups are not affected
    """
    gvars = regcall(group_vars, _data)
    _data = regcall(as_tibble, _data.copy())

    all_columns = _data.columns
    to_move, new_names = _eval_select(
        all_columns,
        *args,
        **kwargs,
        _group_vars=gvars,
        _missing_gvars_inform=False,
    )

    to_move = list(to_move)
    if _before is not None and _after is not None:
        raise ValueError("Must supply only one of `_before` and `_after`.")

    # length = len(all_columns)
    if _before is not None:
        where = min(
            _eval_select(
                all_columns,
                _before,
                _group_vars=[],
                _missing_gvars_inform=False,
            )[0]
        )
        if where not in to_move:
            to_move.append(where)

    elif _after is not None:
        where = max(
            _eval_select(
                all_columns,
                _after,
                _group_vars=[],
                _missing_gvars_inform=False,
            )[0]
        )
        if where not in to_move:
            to_move.insert(0, where)
    else:
        where = 0
        if where not in to_move:
            to_move.append(where)

    lhs = regcall(setdiff, range(where), to_move)
    rhs = regcall(setdiff, range(where + 1, len(all_columns)), to_move)
    pos = regcall(union, lhs, regcall(union, to_move, rhs))

    out = _data.iloc[:, pos]
    # out = out.copy()
    if new_names:
        out.rename(columns=new_names, inplace=True)
        if (
            isinstance(out, TibbleGrouped)
            and len(regcall(intersect, gvars, new_names)) > 0
        ):
            out._datar["group_vars"] = [
                new_names.get(gvar, gvar) for gvar in gvars
            ]

    return out
