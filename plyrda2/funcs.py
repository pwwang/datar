import numpy
from numbers import Number
from collections import OrderedDict
from numpy.core.numeric import NaN

from pandas import DataFrame, Series
import pandas
from pandas.core.groupby import DataFrameGroupBy
from pandas.core.groupby.generic import SeriesGroupBy

from pipda import register_func
from .common import Collection, Inverted, UnaryInvert
from .utils import (
    is_grouped, select_columns, filter_columns, func_name, get_series
)

@register_func
def c(_data, _context, *args):
    return Collection(args)

@register_func
def starts_with(_data, _context, match, ignore_case=True, vars=None):
    return filter_columns(vars or _data.columns,
                          match,
                          ignore_case,
                          lambda mat, cname: cname.startswith(mat))

@register_func
def ends_with(_data, _context, match, ignore_case=True, vars=None):
    return filter_columns(vars or _data.columns,
                          match,
                          ignore_case,
                          lambda mat, cname: cname.endswith(mat))

@register_func
def contains(_data, _context, match, ignore_case=True, vars=None):
    return filter_columns(vars or _data.columns,
                          match,
                          ignore_case,
                          lambda mat, cname: mat in cname)

@register_func
def matches(_data, _context, match, ignore_case=True, vars=None):
    import re
    return filter_columns(vars or _data.columns,
                          match,
                          ignore_case,
                          lambda mat, cname: re.search(mat, cname))

@register_func
def num_range(_data, _context, prefix, range, width=None, vars=None):
    return [f'{prefix}{elem if not width else str(elem).zfill(width)}'
            for elem in range]

@register_func
def everything(_data, _context):
    return _data.columns.to_list()

@register_func
def columns_between(_data, _context, start_col, end_col, inclusive=True):
    colnames = _data.columns.to_list()
    if not isinstance(start_col, int):
        start_col = colnames.index(start_col)
    if not isinstance(end_col, int):
        end_col = colnames.index(end_col)
    if inclusive:
        end_col += 1

    return colnames[start_col:end_col]

@register_func
def columns_from(_data, _context, start_col):
    return columns_between.pipda(_data, _context, start_col, _data.shape[1])

@register_func
def columns_to(_data, _context, end_col, inclusive=True):
    return columns_between.pipda(_data, _context, 0, end_col, inclusive)

@register_func
def where(_data, _context, filter):
    return [column for column in _data.columns if filter(_data[column])]

@register_func
def last_col(_data, _context):
    return _data.columns[-1]

@register_func
def all_of(_data, _context, column, *columns):
    return select_columns(_data, column, *columns)

@register_func
def any_of(_data, _context, column, *columns):
    return select_columns(_data, column, *columns, raise_nonexist=False)

@register_func
def seq_along(_data, _context, along_with):
    return list(range(len(along_with)))

@register_func
def seq_len(_data, _context, length_out):
    return list(range(length_out))

@register_func
def seq(_data, _context, from_=None, to=None, by=None, length_out=None, along_with=None):
    if along_with is not None:
        return seq_along.pipda(_data, _context, along_with)
    if from_ is not None and not isinstance(from_, Number):
        return seq_along.pipda(_data, _context, from_)
    if length_out is not None and from_ is None and to is None:
        return seq_len.pipda(_data, _context, length_out)

    if from_ is None:
        from_ = 0
    elif to is None:
        from_, to = 0, from_

    if length_out is not None:
        by = (float(to) - float(from_)) / float(length_out)
    elif by is None:
        by = 1
        length_out = to - from_
    else:
        length_out = (to - from_ + by - by/10.0) // by
    return [from_ + n * by for n in range(int(length_out))]

@register_func
def n(_data, _context):
    if is_grouped(_data):
        return _data.groupby(_data.__plyrda_groups__).size()
    if isinstance(_data, DataFrame):
        return _data.shape[0]
    return _data.size()

@register_func
def mean(_data, _context, series, na_rm=True):
    raise NotImplementedError

@mean.register(DataFrame)
@mean.register(DataFrameGroupBy)
def _(_data, _context, series, na_rm=True):
    series = get_series(_data, _context, series)
    # SeriesGroupBy.mean doesn't have skipna argument
    # Involve dropna() in getting series?
    return series.mean()

def _ranking(_data, _context, series, na_last, method, percent=False):
    if isinstance(series, Inverted):
        ascending = False
        series = series.operand
    else:
        ascending = True
    series = get_series(_data, _context, series)
    return series.rank(method=method,
                       ascending=ascending,
                       pct=percent, # min-max scaling?
                       na_option=('keep' if na_last == 'keep'
                                  else 'top' if not na_last
                                  else 'bottom'))

@register_func
def min_rank(_data, _context, series, na_last="keep"):
    raise NotImplementedError

@min_rank.register(DataFrame)
@min_rank.register(DataFrameGroupBy)
def _(_data, _context, series, na_last="keep"):
    return _ranking(_data, _context, series, na_last, 'min')

@register_func
def row_number(_data, _context, series, na_last="keep"):
    raise NotImplementedError

@row_number.register(DataFrame)
def _(_data, _context, series=None, na_last="keep"):
    if series is not None:
        return _ranking(_data, _context, series, na_last, 'first')
    if not is_grouped(_data):
        return DataFrame({'n': list(range(_data.shape[0]))})['n']
    grouped = _data.groupby(_data.__plyrda_groups__)
    return row_number.pipda(grouped, _context, None, na_last)

@row_number.register(DataFrameGroupBy)
def _(_data, _context, series=None, na_last="keep"):
    if series is not None:
        return _ranking(_data, _context, series, na_last, 'first')
    return _data.cumcount()

@register_func
def dense_rank(_data, _context, series, na_last="keep"):
    raise NotImplementedError

@dense_rank.register(DataFrame)
@dense_rank.register(DataFrameGroupBy)
def _(_data, _context, series, na_last="keep"):
    return _ranking(_data, _context, series, na_last, 'dense')

@register_func
def percent_rank(_data, _context, series, na_last="keep"):
    raise NotImplementedError

@percent_rank.register(DataFrame)
@percent_rank.register(DataFrameGroupBy)
def _(_data, _context, series, na_last="keep"):
    ranking = _ranking(_data, _context, series, na_last, 'min', True)
    min_rank = ranking.min()
    max_rank = ranking.max()
    ret = ranking.transform(lambda r: (r-min_rank)/(max_rank-min_rank))
    ret[ranking.isna()] = NaN
    return ret

@register_func
def cume_dist(_data, _context, series, na_last="keep"):
    raise NotImplementedError

@cume_dist.register(DataFrame)
@cume_dist.register(DataFrameGroupBy)
def _(_data, _context, series, na_last="keep"):
    ranking = _ranking(_data, _context, series, na_last, 'min')
    max_ranking = ranking.max()
    ret = ranking.transform(lambda r: ranking.le(r).sum() / max_ranking)
    ret[ranking.isna()] = NaN
    return ret

@register_func
def quantile(_data, series, prob=0.5):
    raise NotImplementedError

@quantile.register(DataFrame)
@quantile.register(DataFrameGroupBy)
def _(_data, _context, series, prob=0.5):
    series = get_series(_data, _context, series)
    # drop the quantile index
    ret = series.quantile(q=prob).reset_index(level=1, drop=True)
    return ret

@register_func
def desc(_data, _context, col):
    return Inverted(col)

@register_func
def group_vars(_data, _context):
    return getattr(_data, '__plyrda_groups__', None)

@register_func
def across(_data, _context, _cols=None, _fns=None, *args, **kwargs):
    names = kwargs.pop('_names', None)
    cols = _cols or everything.pipda(_data, _context)
    if not isinstance(cols, (list, tuple)):
        cols = [cols]
    cols = select_columns(_data.columns, *cols)
    fns = OrderedDict()
    if callable(_fns):
        fns[func_name(_fns)] = {'fn': _fns, 'index': 1}
    elif isinstance(_fns, (list, tuple)):
        for i, fn in enumerate(_fns):
            fns[func_name(fn)] = {'fn': fn, 'index': i+1}
    elif isinstance(_fns, dict):
        i = 0
        for key, value in _fns.items():
            fns[key] = {'fn': value, 'index': i+1}
            i += 1

    if not fns:
        return cols

    if _context == 'name':
        fn = list(fns.items())[0][1]['fn']
        return [fn.pipda(_data, _context, col, *args, **kwargs) for col in cols]

    ret = OrderedDict()
    for column in cols:
        for fn_name, fn_index in fns.items():
            fn = fn_index['fn']
            index = fn_index['index']
            name = (names.format(**{
                        '.col': column,
                        '_col': column,
                        '.fn': index,
                        '_fn': index,
                        '.fn0': index - 1,
                        '_fn0': index - 1
                    }) if names
                    else column if not fn_name.endswith('_plyrda_ignore')
                    else f'{column}_{fn_name}' )
            ret[name] = fn.pipda(_data, _context, column, *args, **kwargs)

    return ret

@register_func
def round(_data, _context, column, digits=0):
    if isinstance(column, str):
        return _data[column].round(digits)
    return column.round(digits)

@register_func
def abs(_data, _context, column):
    if isinstance(column, str):
        return _data[column].abs()
    return column.abs()

@register_func
def as_factor(_data, _context, column):
    if _context == 'name' or isinstance(column, str):
        return _data[column].astype('category')
    return column.astype('category')

as_factor.pipda.name = 'as_factor_plyrda_ignore'

@register_func
def between(_data, _context, column, left, right, inclusive=True):
    series = get_series(_data, _context, column)
    ret = series >= left
    if inclusive:
        return ret & (series <= right)
    return ret & (series < right)

@register_func
def ntile(_data, _context, series=None, n=None):
    if n is None:
        raise ValueError('Argument n is required for ntile.')
    if series is None:
        series = row_number(_data, _context)
    return pandas.cut(series, n, labels=range(n))
