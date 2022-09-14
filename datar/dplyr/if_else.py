"""Vectorised if and multiple if-else

https://github.com/tidyverse/dplyr/blob/master/R/if_else.R
https://github.com/tidyverse/dplyr/blob/master/R/case_when.R
"""
import numpy as np
from pipda import register_func

from ..core.backends import pandas as pd
from ..core.backends.pandas import Series
from ..core.backends.pandas.core.groupby import SeriesGroupBy

from ..core.contexts import Context
from ..core.tibble import reconstruct_tibble
from ..tibble import tibble
from .group_by import ungroup


@register_func(context=Context.EVAL)
def if_else(condition, true, false, missing=None):
    """Where condition is TRUE, the matching value from true, where it's FALSE,
    the matching value from false, otherwise missing.

    Note that NAs will be False in condition if missing is not specified

    Args:
        condition: the conditions
        true: and
        false: Values to use for TRUE and FALSE values of condition.
            They must be either the same length as condition, or length 1.
        missing: If not None, will be used to replace missing values

    Returns:
        A series with values replaced.
    """
    if isinstance(condition, SeriesGroupBy):
        return _if_else_sgb(condition, true, false, missing)

    if missing is None:
        missing = np.nan
        na_conds = False
    else:
        na_conds = pd.isnull(condition)

    newcond = condition
    if isinstance(condition, Series):
        newcond = condition.fillna(False)
    elif isinstance(condition, np.ndarray):
        newcond = np.nan_to_num(condition)
    else:
        newcond = np.nan_to_num(condition, 0.0)

    newcond = newcond.astype(bool)

    out = case_when(
        na_conds,  # 0
        missing,  # 1
        ~newcond,  # 2
        false,  # 3
        newcond,  # 4
        true,  # 5
        True,  # 6
        missing,  # 7
    )

    if isinstance(condition, Series):
        out.index = condition.index
        out.name = condition.name

    return out


# SeriesGroupBy
def _if_else_sgb(condition, true, false, missing=None):
    if missing is None:
        missing = np.nan
    df = tibble(condition, true, false, missing, _name_repair="minimal")
    # use obj so df.x won't get a SeriesGroupBy
    grouped = df._datar["grouped"]
    out = if_else(
        grouped.obj.iloc[:, 0],
        grouped.obj.iloc[:, 1],
        grouped.obj.iloc[:, 2],
        grouped.obj.iloc[:, 3],
    )
    return out.groupby(
        condition.grouper,
        observed=condition.observed,
        sort=condition.sort,
        dropna=condition.dropna,
    )


@register_func(context=Context.EVAL)
def case_when(*when_cases):
    """Vectorise multiple `if_else()` statements.

    Args:
        *when_cases: A even-size sequence, with 2n-th element values to match,
            and 2(n+1)-th element the values to replace.
            When matching value is True, then next value will be default to
            replace

    Returns:
        A series with values replaced
    """
    if not when_cases or len(when_cases) % 2 != 0:
        raise ValueError("No cases provided or case-value not paired.")

    is_series = any(
        isinstance(wc, (Series, SeriesGroupBy)) for wc in when_cases
    )
    df = tibble(*when_cases, _name_repair="minimal")

    ungrouped = ungroup(df, __ast_fallback="normal")

    value = Series(np.nan, index=ungrouped.index)
    for i in range(ungrouped.shape[1] - 1, 0, -2):
        condition = ungrouped.iloc[:, i - 1].fillna(False).values.astype(bool)
        value[condition] = ungrouped.iloc[:, i][condition]

    value = value.to_frame(name="when_case_result")
    value = reconstruct_tibble(df, value)
    value = value["when_case_result"]
    return value if is_series else value.values
