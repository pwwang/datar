"""Drop rows containing missing values

https://github.com/tidyverse/tidyr/blob/HEAD/R/drop-na.R
"""

from typing import Optional
from pandas import DataFrame
from pipda import register_verb

from ..core.contexts import Context
from ..core.utils import arg_match, vars_select, copy_attrs
from ..core.grouped import DataFrameGroupBy

from ..dplyr import group_vars, group_by_drop_default

@register_verb(DataFrame, context=Context.SELECT)
def drop_na(
        _data: DataFrame,
        *columns: str,
        how: str = 'any',
        _base0: Optional[bool] = None
) -> DataFrame:
    """Drop rows containing missing values

    See https://tidyr.tidyverse.org/reference/drop_na.html

    Args:
        data: A data frame.
        *columns: Columns to inspect for missing values.
        how: How to select the rows to drop
            - all: All columns of `columns` to be `NA`s
            - any: Any columns of `columns` to be `NA`s
            (tidyr doesn't support this argument)
        _base0: Whether `*columns` are 0-based if given by indexes
            If not provided, will use `datar.base.getOption('index.base.0')`

    Returns:
        Dataframe with rows with NAs dropped and indexes dropped
    """
    arg_match(how, ['any', 'all'])
    all_columns = _data.columns
    if columns:
        columns = vars_select(all_columns, *columns, base0=_base0)
        columns = all_columns[columns]
        out = _data.dropna(subset=columns, how=how).reset_index(drop=True)
    else:
        out = _data.dropna(how=how).reset_index(drop=True)

    if isinstance(_data, DataFrameGroupBy):
        out = _data.__class__(
            out,
            _group_vars=group_vars(_data),
            _drop=group_by_drop_default(_data)
        )
    copy_attrs(out, _data)
    return out
