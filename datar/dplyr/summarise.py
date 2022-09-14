"""Summarise each group to fewer rows"""

from itertools import chain
from typing import Any, Tuple

from pipda import register_verb, evaluate_expr

from ..core.backends.pandas import DataFrame, Series

from ..core.exceptions import NameNonUniqueError
from ..core.contexts import Context
from ..core.options import get_option
from ..core.broadcast import add_to_tibble
from ..core.utils import arg_match, logger, name_of
from ..core.tibble import Tibble, TibbleGrouped, TibbleRowwise

from ..base import setdiff
from .group_by import ungroup
from .group_data import group_vars, group_keys


@register_verb(DataFrame, context=Context.PENDING)
def summarise(
    _data: DataFrame,
    *args: Any,
    _groups: str = None,
    **kwargs: Any,
) -> Tibble:
    """Summarise each group to fewer rows

    See https://dplyr.tidyverse.org/reference/summarise.html

    Both input and the summarised data can be recycled, but separately.

    Aliases - `summarize`

    Examples:
        >>> df = tibble(x=[1,2,3,4])
        >>> df >> summarise(y=sum(f.x), z=f.y*2)
        >>> #   y  z
        >>> # 0 10 20
        >>> df >> summarise(y=sum(f.x), z=f.x+f.y) # fail

        But they should not be mixed in later argument. For example:
        >>> df = tibble(x=[1,2,3,4], g=list('aabb')) >> group_by(f.g)
        >>> df >> summarise(n=n() + f.x)
        >>> # as expected:
        >>>      g  n
        >>> # 0  a  3
        >>> # 1  a  4
        >>> # 2  b  5
        >>> # 3  b  6
        >>> # [Groups: ['g'] (n=2)]
        >>> # However:
        >>> df >> summarise(y=1, n=n() + f.y)
        >>> # n() will be recycling output instead of input
        >>> #    g  y  n
        >>> # 0  a  1  2
        >>> # 1  b  1  2

    Args:
        _groups: Grouping structure of the result.
            - "drop_last": dropping the last level of grouping.
            - "drop": All levels of grouping are dropped.
            - "keep": Same grouping structure as _data.
            - "rowwise": Each row is its own group.
        *args: and
        **kwargs: Name-value pairs, where value is the summarized
            data for each group

    Returns:
        The summary dataframe.
    """
    if not _data.columns.is_unique:
        raise NameNonUniqueError(
            "Can't transform a data frame with duplicate names."
        )

    _groups = arg_match(
        _groups, "_groups", ["drop", "drop_last", "keep", "rowwise", None]
    )

    gvars = group_vars(_data, __ast_fallback="normal")
    out, all_ones = _summarise_build(_data, *args, **kwargs)
    if _groups is None:
        if not isinstance(_data, TibbleRowwise) and all_ones:
            _groups = "drop_last"
        else:
            _groups = "keep"

    if _groups == "drop_last":
        if len(gvars) > 1:
            if get_option("dplyr_summarise_inform"):
                logger.info(
                    "`summarise()` has grouped output by "
                    "%s (override with `_groups` argument)",
                    gvars[:-1],
                )
            out = TibbleGrouped.from_groupby(
                out.groupby(
                    list(gvars[:-1]),
                    observed=_data._datar["grouped"].observed,
                    sort=_data._datar["grouped"].sort,
                    dropna=_data._datar["grouped"].dropna,
                )
            )

    elif _groups == "keep" and len(gvars) > 0:
        if get_option("dplyr_summarise_inform"):
            logger.info(
                "`summarise()` has grouped output by "
                "%s (override with `_groups` argument)",
                gvars,
            )
        grouped = _data._datar["grouped"]
        out = out.group_by(
            gvars,
            sort=grouped.sort,
            drop=grouped.observed,
            dropna=grouped.dropna,
        )

    elif _groups == "rowwise":
        out = out.rowwise(gvars)

    elif isinstance(_data, TibbleRowwise) and get_option(
        "dplyr_summarise_inform"
    ):
        logger.info(
            "`summarise()` has ungrouped output. "
            "You can override using the `_groups` argument."
        )

    # else: # drop
    return out


summarize = summarise


def _summarise_build(
    _data: DataFrame,
    *args: Any,
    **kwargs: Any,
) -> Tuple[Tibble, bool]:
    """Build summarise result"""
    if isinstance(_data, TibbleRowwise):
        outframe = _data.loc[:, _data.group_vars]
    else:
        outframe = group_keys(_data, __ast_fallback="normal")
        if isinstance(_data, TibbleGrouped):
            grouped = _data._datar["grouped"]
            outframe = outframe.group_by(
                grouped.grouper.names,
                drop=grouped.observed,
                dropna=grouped.dropna,
                sort=grouped.sort,
            )

    if isinstance(_data, Tibble):
        # So we have _data._datar["used_ref"]
        _data = _data.copy(False)
    else:
        _data = Tibble(_data, copy=False)

    _data._datar["used_refs"] = set()
    outframe._datar["summarise_source"] = _data
    all_ones = True
    for key, val in chain(enumerate(args), kwargs.items()):
        try:
            val = evaluate_expr(val, outframe, Context.EVAL)
        except KeyError:
            val = evaluate_expr(val, _data, Context.EVAL)

        if val is None:
            continue

        if isinstance(key, int):
            if isinstance(val, (DataFrame, Series)) and len(val) == 0:
                continue
            key = name_of(val)

        newframe = add_to_tibble(outframe, key, val, broadcast_tbl=True)
        if newframe is not outframe:
            # if it is broadcasted, then it should not be all ones.
            # since all ones don't need to broadcast
            all_ones = False

        outframe = newframe

    gvars = group_vars(_data, __ast_fallback="normal")
    tmp_cols = [
        mcol
        for mcol in outframe.columns
        if mcol.startswith("_")
        and mcol not in _data._datar["used_refs"]
        and mcol not in gvars
    ]
    outframe = ungroup(outframe, __ast_fallback="normal")
    outframe = outframe[
        setdiff(outframe.columns, tmp_cols, __ast_fallback="normal")
    ]
    return outframe.reset_index(drop=True), all_ones
