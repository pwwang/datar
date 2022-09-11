"""Extract a single colum

https://github.com/tidyverse/dplyr/blob/master/R/pull.R
"""
from pipda import register_verb

from ..core.backends.pandas import DataFrame, Series
from ..core.backends.pandas.api.types import is_scalar

from ..core.tibble import Tibble, TibbleGrouped
from ..core.contexts import Context
from ..core.utils import arg_match
from ..tibble import as_tibble


@register_verb(
    DataFrame,
    context=Context.SELECT,
    extra_contexts={"name": Context.EVAL},
)
def pull(_data, var=-1, name=None, to=None):

    """Pull a series or a dataframe from a dataframe

    Args:
        _data: The dataframe
        var: The column to pull, either the name or the index
        name: The name of the pulled value
            - If `to` is frame, or the value pulled is data frame, it will be
              the column names
            - If `to` is series, it will be the series name. If multiple names
              are given, only the first name will be used.
            - If `to` is series, but value pulled is a data frame, then a
              dictionary of series with the series names as keys or given `name`
              as keys.
        to: Type of data to return.
            Only works when pulling `a` for name `a$b`
            - series: Return a pandas Series object
              Group information will be lost
              If pulled value is a dataframe, it will return a dict of series,
              with the series names or the `name` provided.
            - array: Return a numpy.ndarray object
            - frame: Return a DataFrame with that column
            - list: Return a python list
            - dict: Return a dict with `name` as keys and pulled value as values
              Only a single column is allowed to pull
            - If not provided: `series` when pulled data has only one columns.
                `dict` if `name` provided and has the same length as the pulled
                single column. Otherwise `frame`.

    Returns:
        The data according to `to`
    """
    # make sure pull(df, 'x') pulls a dataframe for columns
    # x$a, x$b in df

    to = arg_match(
        to, "to", ["list", "array", "frame", "series", "dict", None]
    )
    if name is not None and is_scalar(name):
        name = [name]

    _data = as_tibble(_data, __ast_fallback="normal")
    if isinstance(var, int):
        var = _data.columns[var]
        var = var.split("$", 1)[0]

    pulled = _data[var]
    pulled = getattr(pulled, "obj", pulled)
    # if var in _data.columns and isinstance(pulled, DataFrame):
    #     pulled = pulled.iloc[:, 0]

    if to is None:
        if name is not None and len(name) == len(pulled):
            to = "dict"
        else:
            to = "frame" if isinstance(pulled, DataFrame) else "series"

    if to == "dict":
        if name is None or len(name) != len(pulled):
            raise ValueError(
                "No `name` provided or length mismatches with the values."
            )
        return dict(zip(name, pulled))
    if to == "list":
        return pulled.values.tolist()
    if to == "array":
        return pulled.values
    if to == "frame":
        value = pulled if isinstance(pulled, DataFrame) else pulled.to_frame()
        if name and len(name) != value.shape[1]:
            raise ValueError(
                f"Expect {value.shape[1]} names but got {len(name)}."
            )
        if name:
            value.columns = name
        return value
    # if to == 'series':
    if isinstance(pulled, DataFrame) and pulled.shape[1] == 1:
        pulled = pulled.iloc[:, -1]
    if isinstance(pulled, Series):
        if name:
            pulled.name = name[0]
        return pulled
    # df
    if name and len(name) != pulled.shape[1]:
        raise ValueError(
            f"Expect {pulled.shape[1]} names but got {len(name)}."
        )

    out = pulled.to_dict("series")
    if not name:
        return out

    for newname, oldname in zip(name, out):
        out[newname] = out.pop(oldname)
    return out


@pull.register(
    TibbleGrouped,
    context=Context.PENDING,
)
def _(_data, var=-1, name=None, to=None):
    """Pull a column from a grouped data frame"""
    return pull(
        Tibble(_data, copy=False),
        var=var,
        name=name,
        to=to,
        __ast_fallback="normal",
    )
