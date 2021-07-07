"""Drop rows containing missing values

https://github.com/tidyverse/tidyr/blob/HEAD/R/drop-na.R
"""

from pandas import DataFrame
from pipda import register_verb

from ..core.contexts import Context
from ..core.utils import arg_match, vars_select, reconstruct_tibble


@register_verb(DataFrame, context=Context.SELECT)
def drop_na(
    _data: DataFrame,
    *columns: str,
    how_: str = "any",
    base0_: bool = None,
) -> DataFrame:
    """Drop rows containing missing values

    See https://tidyr.tidyverse.org/reference/drop_na.html

    Args:
        data: A data frame.
        *columns: Columns to inspect for missing values.
        how_: How to select the rows to drop
            - all: All columns of `columns` to be `NA`s
            - any: Any columns of `columns` to be `NA`s
            (tidyr doesn't support this argument)
        base0_: Whether `*columns` are 0-based if given by indexes
            If not provided, will use `datar.base.get_option('index.base.0')`

    Returns:
        Dataframe with rows with NAs dropped and indexes dropped
    """
    arg_match(how_, "how_", ["any", "all"])
    all_columns = _data.columns
    if columns:
        columns = vars_select(all_columns, *columns, base0=base0_)
        columns = all_columns[columns]
        out = _data.dropna(subset=columns, how=how_).reset_index(drop=True)
    else:
        out = _data.dropna(how=how_).reset_index(drop=True)

    return reconstruct_tibble(_data, out, keep_rowwise=True)
