"""Unite multiple columns into one by pasting strings together"""

from typing import Union

import pandas
from pandas import DataFrame
from pipda import register_verb

from ..core.contexts import Context
from ..core.utils import vars_select, reconstruct_tibble

from ..base import setdiff


@register_verb(DataFrame, context=Context.SELECT)
def unite(
    data: DataFrame,
    col: str,
    *columns: Union[str, int],
    sep: str = "_",
    remove: bool = True,
    na_rm: bool = False,
    base0_: bool = None,
) -> DataFrame:
    """Unite multiple columns into one by pasting strings together

    Args:
        data: A data frame.
        col: The name of the new column, as a string or symbol.
        *columns: Columns to unite
        sep: Separator to use between values.
        remove: If True, remove input columns from output data frame.
        na_rm: If True, missing values will be remove prior to uniting
            each value.
        base0_: Whether `columns` is 0-based when given by index
            If not provided, will use `datar.base.get_option('index.base.0')`

    Returns:
        The dataframe with selected columns united
    """
    all_columns = data.columns
    if not columns:
        columns = all_columns
    else:
        columns = all_columns[vars_select(all_columns, *columns, base0=base0_)]

    out = data.copy()

    def unite_cols(row):
        if na_rm:
            row = [elem for elem in row if not pandas.isnull(elem)]
        return sep.join(str(elem) for elem in row)

    out[col] = out[columns].agg(unite_cols, axis=1)
    # get indexes to relocate
    insert_at = min(data.columns.get_indexer_for(columns))
    relocated_cols = (
        data.columns[:insert_at]
        .difference([col])
        .union([col], sort=False)
        .union(data.columns[insert_at:].difference([col]), sort=False)
    )
    out = out[relocated_cols]

    if remove:
        cols_to_remove = setdiff(columns, [col])
        if len(cols_to_remove) > 0:
            out.drop(columns=cols_to_remove, inplace=True)

    return reconstruct_tibble(data, out)
