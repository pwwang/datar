"""Unite multiple columns into one by pasting strings together"""

from typing import Union

from ..core.backends import pandas as pd
from ..core.backends.pandas import DataFrame, Series
from pipda import register_verb

from ..core.contexts import Context
from ..core.utils import vars_select, regcall
from ..core.tibble import reconstruct_tibble

from ..dplyr import ungroup


@register_verb(DataFrame, context=Context.SELECT)
def unite(
    data: DataFrame,
    col: str,
    *columns: Union[str, int],
    sep: str = "_",
    remove: bool = True,
    na_rm: bool = True,
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

    Returns:
        The dataframe with selected columns united
    """
    all_columns = data.columns
    if not columns:
        columns = all_columns
    else:
        columns = all_columns[vars_select(all_columns, *columns)]

    out = regcall(ungroup, data).copy()

    united = Series(out[columns].values.tolist(), index=out.index)
    if sep is not None:
        if na_rm:
            united = united.transform(
                lambda lst: [elem for elem in lst if pd.notnull(elem)]
            )

        united = united.transform(lambda x: sep.join(str(elem) for elem in x))

    # get indexes to relocate
    unite_cols = out.columns.get_indexer_for(columns)
    insert_at = int(unite_cols.min())
    out.insert(insert_at, col, united, allow_duplicates=True)

    if remove:
        out_cols = [
            i for i in range(out.shape[1])
            if i <= insert_at and i - 1 not in unite_cols
        ]
        out = out.iloc[:, out_cols]

    return reconstruct_tibble(data, out)
