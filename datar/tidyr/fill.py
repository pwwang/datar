"""Fill in missing values with previous or next value

https://github.com/tidyverse/tidyr/blob/HEAD/R/fill.R
"""

from typing import Optional, Union

from pandas import DataFrame
from pipda import register_verb

from ..core.contexts import Context
from ..core.utils import vars_select, copy_attrs
from ..core.grouped import DataFrameGroupBy

from ..dplyr import group_vars, group_by_drop_default

@register_verb(
    DataFrame,
    context=Context.SELECT
)
def fill(
        _data: DataFrame,
        *columns: Union[str, int],
        _direction: str = "down",
        _base0: Optional[bool] = None
) -> DataFrame:
    """Fills missing values in selected columns using the next or
    previous entry.

    See https://tidyr.tidyverse.org/reference/fill.html

    Args:
        _data: A dataframe
        *columns: Columns to fill
        _direction: Direction in which to fill missing values.
            Currently either "down" (the default), "up",
            "downup" (i.e. first down and then up) or
            "updown" (first up and then down).
        _base0: Whether `*columns` are 0-based if given by indexes
            If not provided, will use `datar.base.getOption('index.base.0')`

    Returns:
        The dataframe with NAs being replaced.
    """
    data = _data.copy()
    if not columns:
        data = data.fillna(
            method='ffill' if _direction.startswith('down') else 'bfill',
        )
        if _direction in ('updown', 'downup'):
            data = data.fillna(
                method='ffill' if _direction.endswith('down') else 'bfill',
            )
    else:
        colidx = vars_select(data.columns, *columns, base0=_base0)
        data.iloc[:, colidx] = fill(data.iloc[:, colidx], _direction=_direction)
    return data

@fill.register(DataFrameGroupBy, context=Context.SELECT)
def _(
        _data: DataFrameGroupBy,
        *columns: str,
        _direction: str = "down"
) -> DataFrameGroupBy:
    # DataFrameGroupBy
    out = _data.group_apply(
        lambda df: fill(df, *columns, _direction=_direction)
    )
    out = _data.__class__(
        out,
        _group_vars=group_vars(_data),
        _drop=group_by_drop_default(_data)
    )
    copy_attrs(out, _data)
    return out
