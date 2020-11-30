
from typing import Iterable
from .exceptions import PlyrdaColumnNameInvalidException

def is_neg(val):
    from .common import UnaryNeg
    if isinstance(val, UnaryNeg):
        return True

    if not isinstance(val, int):
        return False

    return val < 0

def check_column(column):
    from .common import UnaryNeg
    if not isinstance(column, (
            (int, str, list, tuple, UnaryNeg)
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
        elif isinstance(column.operand, (list, tuple)): # -starts_with('c')
            selected.extend(column.operand)
        else: # -X.cut
            selected.append(column.operand)

    if raise_nonexist:
        for sel in selected:
            if sel not in all_columns:
                raise PlyrdaColumnNameInvalidException("Column `{sel}` doesn't exist.")

    if has_negs:
        selected = [colname for colname in all_columns
                    if colname not in selected]
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

def normalize_kw_series(kwargs, max_nrows=None):

    for key, val in kwargs.items():
        if isinstance(val, (str, bytes)) or not isinstance(val, Iterable):
            kwargs[key] = [val]
    max_nrows = max_nrows or max(nrows_or_nelems(val)
                                 for val in kwargs.values())
    for key, val in kwargs.items():
        nrows = nrows_or_nelems(val)
        if nrows < max_nrows and max_nrows % nrows == 0 and nrows != 1:
            if isinstance(val, (list, tuple)):
                kwargs[key] = val * (max_nrows // nrows)
            else:
                kwargs[key] = val.append([val] * (max_nrows // nrows - 1))
    return kwargs

def is_grouped(df):
    return hasattr(df, '__plyrda_groups__') and df.__plyrda_groups__
