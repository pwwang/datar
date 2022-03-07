"""Subset distinct/unique rows

See source https://github.com/tidyverse/dplyr/blob/master/R/distinct.R
"""
from pandas import DataFrame
from pandas.core.groupby import GroupBy
from pipda import register_verb

from ..core.contexts import Context
from ..core.factory import func_factory
from ..core.utils import regcall
from ..core.tibble import Tibble, TibbleGrouped, reconstruct_tibble
from ..base import union, setdiff, intersect
from .mutate import mutate


@register_verb(DataFrame, context=Context.PENDING)
def distinct(_data, *args, _keep_all=False, **kwargs):
    """Select only unique/distinct rows from a data frame.

    The original API:
    https://dplyr.tidyverse.org/reference/distinct.html

    Args:
        _data: The dataframe
        *args: and
        **kwargs: Optional variables to use when determining
            uniqueness.
        _keep_all: If TRUE, keep all variables in _data

    Returns:
        A dataframe without duplicated rows in _data
    """
    if not args and not kwargs:
        uniq = _data.drop_duplicates()
    else:
        # keep_none_prefers_new_order
        uniq = (
            regcall(
                mutate,
                _data,
                *args,
                **kwargs,
                _keep="none",
            )
        ).drop_duplicates()

    if not _keep_all:
        # keep original order
        out = uniq[
            regcall(
                union,
                regcall(intersect, _data.columns, uniq.columns),
                regcall(setdiff, uniq.columns, _data.columns),
            )
        ]
    else:
        out = _data.loc[uniq.index, :].copy()
        out[uniq.columns.tolist()] = uniq

    return reconstruct_tibble(_data, Tibble(out, copy=False))


@func_factory("agg", "x")
def n_distinct(x, na_rm=True):
    """Get the length of distinct elements"""
    return x.nunique(dropna=na_rm)


n_distinct.register(
    (TibbleGrouped, GroupBy),
    "nunique",
    pre=lambda x, na_rm=True: (x, (), {"dropna": na_rm}),
)
