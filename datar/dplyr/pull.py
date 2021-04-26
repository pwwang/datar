"""Extract a single colum

https://github.com/tidyverse/dplyr/blob/master/R/pull.R
"""

from typing import Mapping, Optional, Union

from pandas import DataFrame, Series
from pipda import register_verb, evaluate_expr

from ..core.defaults import f
from ..core.contexts import Context
from ..core.utils import arg_match
from ..core.types import StringOrIter, SeriesLikeType, is_scalar

@register_verb(DataFrame, context=Context.SELECT)
def pull(
        _data: DataFrame,
        var: Union[int, str] = -1,
        name: Optional[StringOrIter] = None,
        to: Optional[str] = None
) -> Union[DataFrame, SeriesLikeType, Mapping[str, SeriesLikeType]]:
    """Pull a series or a dataframe from a dataframe

    Args:
        _data: The dataframe
        var: The column to pull
        name: If specified, a zip object will be return with the name-value
            pairs. It can be a column name or a list of strs with the same
            length as the series
            Only works when pulling `a` for name `a$b`
        to: Type of data to return.
            Only works when pulling `a` for name `a$b`
            - series: Return a pandas Series object
              Group information will be lost
            - array: Return a numpy.ndarray object
            - frame: Return a DataFrame with that column
            - list: Return a python list
            - If not provided: `series` when pulled data has only one columns,
                otherwise `frame`.

    Returns:
        The data according to `to`
    """
    # make sure pull(df, 'x') pulls a dataframe for columns
    # x$a, x$b in df

    to = arg_match(to, ['list', 'array', 'frame', 'series', None])
    if name and is_scalar(name):
        name = [name]

    if isinstance(var, int):
        var = _data.columns[var]
        var = var.split('$', 1)[0]

    pulled = evaluate_expr(f[var], _data, Context.EVAL)
    # if var in _data.columns and isinstance(pulled, DataFrame):
    #     pulled = pulled.iloc[:, 0]

    if to is None:
        to = 'frame' if isinstance(pulled, DataFrame) else 'series'

    if to == 'list':
        return pulled.values.tolist()
    if to == 'array':
        return pulled.values
    if to == 'frame':
        value = pulled if isinstance(pulled, DataFrame) else pulled.to_frame()
        if name and len(name) != value.shape[1]:
            raise ValueError(
                f'Expect {value.shape[1]} names but got {len(name)}.'
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
            f'Expect {pulled.shape[1]} names but got {len(name)}.'
        )

    out = pulled.to_dict('series')
    if not name:
        return out

    for newname, oldname in zip(name, out):
        out[newname] = out.pop(oldname)
    return out
