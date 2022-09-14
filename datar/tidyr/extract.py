"""Extract a character column into multiple columns using regular
expression groups

https://github.com/tidyverse/tidyr/blob/HEAD/R/extract.R
"""
import re
from typing import Union

from ..core.backends import pandas as pd
from ..core.backends.pandas import DataFrame
from ..core.backends.pandas.api.types import is_scalar
from pipda import register_verb

from ..core.contexts import Context
from ..core.utils import apply_dtypes, vars_select
from ..core.tibble import reconstruct_tibble

from ..dplyr import ungroup


@register_verb(DataFrame, context=Context.SELECT)
def extract(
    data: DataFrame,
    col: Union[str, int],
    into,
    regex: str = r"(\w+)",
    remove: bool = True,
    convert=False,
) -> DataFrame:
    """Given a regular expression with capturing groups, extract() turns each
    group into a new column. If the groups don't match, or the input is NA,
    the output will be NA.

    See https://tidyr.tidyverse.org/reference/extract.html

    Args:
        data: The dataframe
        col: Column name or position.
        into: Names of new variables to create as character vector.
            Use None to omit the variable in the output.
        regex: a regular expression used to extract the desired values.
            There should be one group (defined by ()) for each element of into.
        remove: If TRUE, remove input column from output data frame.
        convert: The universal type for the extracted columns or a dict for
            individual ones

    Returns:
        Dataframe with extracted columns.
    """
    if is_scalar(into):
        into = [into]  # type: ignore

    all_columns = data.columns
    col = vars_select(all_columns, col)
    col = all_columns[col[0]]

    outcols = {}
    # merge columns with same name
    # all columns are already strs
    # # 'col' => i, j, k
    # # i, j, k are indexes that have same name 'col'
    mergedcols = {}
    for i, outcol in enumerate(into):
        if is_scalar(outcol) and pd.isnull(outcol):
            continue
        if not isinstance(outcol, str):
            raise ValueError(
                "`into` must be a string or an iterable of strings."
            )
        outcols[i] = outcol
        mergedcols.setdefault(outcol, []).append(i)

    regex = re.compile(regex)
    if regex.groups != len(into):
        raise ValueError(
            f"`regex` should define {len(into)} groups; "
            f"found {regex.groups}."
        )

    undata = ungroup(data, __ast_fallback="normal")
    out = undata[col].str.extract(regex)
    out = {
        outcol: (
            out.iloc[:, indexes[0]]
            if len(indexes) == 1
            else out.iloc[:, indexes].astype(str).agg("".join, axis=1)
        )
        for outcol, indexes in mergedcols.items()
    }

    out = DataFrame(out)
    apply_dtypes(out, convert)

    base = undata[all_columns.difference([col])] if remove else undata
    out = pd.concat([base, out], axis=1)
    return reconstruct_tibble(data, out)
