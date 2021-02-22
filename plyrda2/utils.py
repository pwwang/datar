
from typing import Iterable
import warnings
from numpy.core.numeric import NaN
from pandas.core.frame import DataFrame
from pandas.core.groupby.generic import SeriesGroupBy

from pandas.core.series import Series
from .exceptions import PlyrdaColumnNameInvalidException

def is_neg(val):
    from .common import Inverted
    if isinstance(val, Inverted):
        return True

    if not isinstance(val, int):
        return False

    return val < 0

def check_column(column):
    from .common import Inverted
    if not isinstance(column, (
            (int, str, list, tuple, Inverted)
    )):
        raise PlyrdaColumnNameInvalidException(
            'Invalid column, expected int, str, list, tuple, c(), '
            f'X.column, -c() or -X.column, got {type(column)}'
        )

def select_columns(all_columns, *columns, raise_nonexist=True):
    negs = [is_neg(column) for column in columns]
    if any(negs) and not all(negs):
        raise PlyrdaColumnNameInvalidException(
            'Either none or all of the columns are negative.'
        )

    selected = []
    has_negs = any(negs)
    for column in columns:
        check_column(column)
        if isinstance(column, int): # 1, -1
            selected.append(all_columns[-column if has_negs else column])
        elif isinstance(column, str): # X.price, 'price'
            selected.append(column)
        elif isinstance(column, (list, tuple)): # ['x', 'y']
            selected.extend(column)
        elif isinstance(column, Series):
            selected.append(column.name)
        elif isinstance(column.operand, (list, tuple)): # -starts_with('c')
            selected.extend(column.operand)
        elif isinstance(column.operand, Series):
            selected.append(column.operand.name)
        else: # -X.cut
            selected.append(column.operand)

    if raise_nonexist:
        for sel in selected:
            if sel not in all_columns:
                raise PlyrdaColumnNameInvalidException(
                    f"Column `{sel}` doesn't exist."
                )

    if has_negs:
        selected = list_diff(all_columns, selected)
    return selected

def filter_columns(all_columns, match, ignore_case, func):
    if not isinstance(match, (tuple, list)):
        match = (match, )

    # return [colname for colname in all_columns
    #         if any(func(mat.lower() if ignore_case else mat,
    #                     colname.lower() if ignore_case else colname)
    #                for mat in match)]

    # The match order matters
    ret = []
    for mat in match:
        for column in all_columns:
            if column in ret:
                continue
            if (func(mat.lower() if ignore_case else mat,
                     column.lower() if ignore_case else column)):
                ret.append(column)
    return ret

def expand_collections(collections):
    if not isinstance(collections, (list, tuple)):
        return [collections]
    ret = []
    for collection in collections:
        ret.extend(expand_collections(collection))
    return ret

def nrows_or_nelems(obj):
    try:
        return obj.shape[0]
    except AttributeError:
        return len(obj)

def series_expandable(df_or_series, series_or_df):
    if (not isinstance(df_or_series, (Series, DataFrame)) or
            not isinstance(series_or_df, (Series, DataFrame))):
        return False
    if type(df_or_series) is type(series_or_df):
        return False

    series = df_or_series if isinstance(df_or_series, Series) else series_or_df
    df = df_or_series if isinstance(df_or_series, DataFrame) else series_or_df
    if series.index.name not in df.columns:
        return False
    return True

def series_expand(series, df):
    holder = DataFrame({'x': [NaN] * df.shape[0]})
    holder.loc[df[series.index.name].notna().values, 'x'] = series[
        df[series.index.name].dropna()
    ].values
    holder.index = df[series.index.name]
    return holder['x']

def normalize_series(series, data):
    """Normalize possible series data to add to the data or compare with
    other series of the data"""
    if isinstance(series, (str, bytes)) or not isinstance(series, Iterable):
        series = [series]
    if series_expandable(series, data):
        return series_expand(series, data)
    len_series = nrows_or_nelems(series)

    if len_series == data.shape[0]:
        return series
    if data.shape[0] % len_series == 0:
        nrepeat = data.shape[0] // len_series
        if isinstance(series, (list, tuple)):
            return series * nrepeat
        return series.append([series] * (nrepeat - 1))
    return series

def is_grouped(df):
    return hasattr(df, '__plyrda_groups__') and df.__plyrda_groups__

def df_setattr(df, name, value):
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        setattr(df, name, value)

def func_name(func):
    try:
        return func.name
    except AttributeError:
        return func.__name__

def get_series(df, context, column):
    grouped = is_grouped(df)
    if grouped:
        df = df.groupby(df.__plyrda_groups__)
    if context == 'name' or isinstance(column, str):
        return df[column]
    if isinstance(column, SeriesGroupBy):
        # df could be changed, so have to retrieve again
        return df[column.var().name]
    if isinstance(column, Series) and grouped:
        return df[column.name]
    return column

def list_diff(list1, list2):
    return [elem for elem in list1 if elem not in list2]
