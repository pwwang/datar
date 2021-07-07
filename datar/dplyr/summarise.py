"""Summarise each group to fewer rows"""

from typing import Any, Iterable, Mapping, Union
from pandas import DataFrame
from pipda import register_verb, evaluate_expr
from pipda.function import Function

from ..core.defaults import DEFAULT_COLUMN_PREFIX
from ..core.contexts import Context
from ..core.utils import (
    length_of,
    recycle_df,
    arg_match,
    check_column_uniqueness,
    df_setitem,
    name_mutatable_args,
    logger,
    get_option,
    reconstruct_tibble,
    dedup_name,
)
from ..core.exceptions import ColumnNotExistingError
from ..core.grouped import DataFrameGroupBy, DataFrameRowwise

from .group_data import group_keys, group_vars, group_data
from .group_by import group_by_drop_default


@register_verb(DataFrame, context=Context.PENDING)
def summarise(
    _data: DataFrame,
    *args: Union[DataFrame, Mapping[str, Iterable[Any]]],
    _groups: str = None,
    **kwargs: Any,
) -> DataFrame:
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
        *args, **kwargs: Name-value pairs, where value is the summarized
            data for each group

    Returns:
        The summary dataframe.
    """
    check_column_uniqueness(
        _data, "Can't transform a data frame with duplicate names"
    )
    _groups = arg_match(
        _groups, "_groups", ["drop", "drop_last", "keep", "rowwise", None]
    )
    out = _summarise_build(_data, *args, **kwargs)
    if _groups == "rowwise":
        return DataFrameRowwise(out, _group_drop=group_by_drop_default(_data))
    return out


@summarise.register(DataFrameGroupBy, context=Context.PENDING)
def _(
    _data: DataFrameGroupBy,
    *args: Union[DataFrame, Mapping[str, Iterable[Any]]],
    _groups: str = None,
    **kwargs: Any,
) -> DataFrame:
    # empty
    _groups = arg_match(
        _groups, "_groups", ["drop", "drop_last", "keep", "rowwise", None]
    )

    allone = True
    if group_data(_data).shape[0] == 0:
        out = _summarise_build(_data, *args, **kwargs).iloc[[], :]
    else:

        def apply_func(df):
            nonlocal allone
            out = df >> summarise(*args, **kwargs)
            if out.shape[0] != 1:
                allone = False
            return out

        out = _data.datar_apply(apply_func)

    g_keys = group_vars(_data)
    if _groups is None:
        if allone and not isinstance(_data, DataFrameRowwise):
            _groups = "drop_last"
        else:
            _groups = "keep"

    if _groups == "drop_last":
        if len(g_keys) > 1:
            if get_option("dplyr_summarise_inform"):
                logger.info(
                    "`summarise()` has grouped output by "
                    "%s (override with `_groups` argument)",
                    g_keys[:-1],
                )
            out = reconstruct_tibble(_data, out, [g_keys[-1]])
    elif _groups == "keep" and g_keys:
        if get_option("dplyr_summarise_inform"):
            logger.info(
                "`summarise()` has grouped output by "
                "%s (override with `_groups` argument)",
                g_keys,
            )
        out = DataFrameGroupBy(
            out, _group_vars=g_keys, _group_drop=group_by_drop_default(_data)
        )
    elif _groups == "rowwise":
        out = reconstruct_tibble(_data, out, keep_rowwise=True)
    elif isinstance(_data, DataFrameRowwise) and get_option(
        "dplyr_summarise_inform"
    ):
        logger.info(
            "`summarise()` has ungrouped output. "
            "You can override using the `_groups` argument."
        )
    # else: # drop
    return out


summarize = summarise  # pylint: disable=invalid-name


def _summarise_build(_data: DataFrame, *args: Any, **kwargs: Any) -> DataFrame:
    """Build summarise result"""
    context = Context.EVAL.value
    named = name_mutatable_args(*args, **kwargs)

    out = group_keys(_data)
    for key, val in named.items():
        # support: df %>% muate(a=1, a=a+1)
        key = dedup_name(key, list(named))

        envdata = out
        if out.shape[1] == 0 or (
            isinstance(val, Function)
            and getattr(val.func, "summarise_prefers_input", False)
        ):
            envdata = _data

        try:
            val = evaluate_expr(val, envdata, context)
        except (KeyError, ColumnNotExistingError):
            # also recycle input
            val = evaluate_expr(val, _data, context)

        if val is None:
            continue

        if key.startswith(DEFAULT_COLUMN_PREFIX) and isinstance(val, DataFrame):
            # ignore key
            for name, ser in val.to_dict("series").items():
                if length_of(out) == 1:
                    out, ser = recycle_df(out, ser, None, key)
                out = df_setitem(out, name, ser)
        elif isinstance(val, DataFrame):
            for name, ser in val.to_dict("series").items():
                if length_of(out) == 1:
                    out, ser = recycle_df(out, ser, None, key)
                out = df_setitem(out, f"{key}${name}", ser)
        elif length_of(out) == 1:
            out, val = recycle_df(out, val, None, key)
            out = df_setitem(out, key, val)
        else:
            out = df_setitem(out, key, val)

    return out
