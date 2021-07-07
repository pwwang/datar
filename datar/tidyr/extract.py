"""Extract a character column into multiple columns using regular
expression groups

https://github.com/tidyverse/tidyr/blob/HEAD/R/extract.R
"""
import re
from typing import Union, Mapping

import pandas
from pandas import DataFrame
from pipda import register_verb

from ..core.types import StringOrIter, Dtype, is_scalar
from ..core.contexts import Context
from ..core.utils import apply_dtypes, vars_select, reconstruct_tibble


@register_verb(DataFrame, context=Context.SELECT)
def extract(
    data: DataFrame,
    col: Union[str, int],
    into: StringOrIter,
    regex: str = r"(\w+)",
    remove: bool = True,
    convert: Union[bool, Dtype, Mapping[str, Dtype]] = False,
    base0_: bool = None,
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
        base0_: Whether `col` is 0-based when given by index
            If not provided, will use `datar.base.get_option('index.base.0')`

    Returns:
        Dataframe with extracted columns.
    """
    if is_scalar(into):
        into = [into] # type: ignore

    all_columns = data.columns
    col = vars_select(all_columns, col, base0=base0_)
    col = all_columns[col[0]]

    outcols = {}
    # merge columns with same name
    # all columns are already strs
    ## 'col' => i, j, k
    ## i, j, k are indexes that have same name 'col'
    mergedcols = {}
    for i, outcol in enumerate(into):
        if is_scalar(outcol) and pandas.isnull(outcol):
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
    out = data[col].str.extract(regex)
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

    base = data[all_columns.difference([col])] if remove else data
    out = pandas.concat([base, out], axis=1)
    return reconstruct_tibble(data, out, keep_rowwise=True)
