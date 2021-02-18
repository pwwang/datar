from itertools import chain
from pandas.core.indexes.multi import MultiIndex

from pandas.core.series import Series
from pipda.utils import Expression, evaluate_expr
from pipda.symbolic import DirectSubsetRef
from plyrda.group_by import get_groups, get_rowwise, is_grouped, set_groups, set_rowwise
from typing import Any, Iterable, Mapping, Optional, Union
from pandas import DataFrame
from pipda import register_verb, Context

from .utils import align_value, list_diff, select_columns
from .middlewares import Across, CAcross, Collection, UnaryNeg
from .exceptions import ColumnNameInvalidError

@register_verb(DataFrame)
def head(_data, n: int = 5) -> DataFrame:
    """Get the first n rows of the dataframe

    Args:
        n: The number of rows to return

    Returns:
        The dataframe with first N rows
    """
    return _data.head(n)

@register_verb(DataFrame)
def tail(_data, n=5):
    """Get the last n rows of the dataframe

    Args:
        n: The number of rows to return

    Returns:
        The dataframe with last N rows
    """
    return _data.tail(n)

@register_verb(DataFrame, context=Context.NAME)
def select(
        _data: DataFrame,
        *columns: Union[str, Iterable[str], UnaryNeg],
        **renamings: Mapping[str, str]
) -> DataFrame:
    """Select (and optionally rename) variables in a data frame

    Args:
        *columns: The columns to select
        **renamings: The columns to rename and select in new => old column way.

    Returns:
        The dataframe with select columns
    """
    selected = select_columns(
        _data.columns,
        *columns,
        *renamings.values()
    )
    # old -> new
    new_names = {val: key for key, val in renamings.items() if val in selected}
    if new_names:
        return _data[selected].rename(columns=new_names)
    return _data[selected]

    # todo: keep groupby

@register_verb(DataFrame, context=Context.UNSET)
def mutate(
        _data: DataFrame,
        *acrosses: Across,
        _keep: str = 'all',
        _before: Optional[str] = None,
        _after: Optional[str] = None,
        **kwargs: Any
) -> DataFrame:

    used_cols = []
    def ref_counter(expr: Expression):
        if (
                isinstance(expr, DirectSubsetRef) and
                isinstance(expr.ref, str) and
                expr.ref not in used_cols
        ):
            used_cols.append(expr.ref)

    if _before is not None:
        _before = evaluate_expr(_before, _data, Context.NAME)
    if _after is not None:
        _after = evaluate_expr(_after, _data, Context.NAME)

    across = {} # no need OrderedDict in python3.7+ anymore
    for acrs in acrosses:
        acrs = evaluate_expr(acrs, _data, Context.DATA, ref_counter)
        if isinstance(acrs, Across):
            across.update(acrs.evaluate(Context.DATA))
        else:
            across.update(acrs)

    across.update(kwargs)
    kwargs = across

    data = _data.copy()
    set_groups(data, get_groups(_data))
    set_rowwise(data, get_rowwise(_data))

    for key, val in kwargs.items():
        if val is None:
            data.drop(columns=[key], inplace=True)
            continue
        val = evaluate_expr(val, data, Context.DATA, callback=ref_counter)
        if isinstance(val, CAcross):
            val.names = key
        if isinstance(val, Across):
            val = DataFrame(val.evaluate(Context.DATA, data))

        value = align_value(val, data)
        try:
            data[key] = value
        except ValueError as verr:
            # cannot reindex from a duplicate axis
            try:
                data[key] = value.values
            except AttributeError:
                raise verr from None

    outcols = list(kwargs)
    # do the relocate first
    if _before is not None or _after is not None:
        data = relocate(data, *outcols, _before=_before, _after=_after)

    # do the keep
    if _keep == 'used':
        data = data[used_cols + list_diff(outcols, used_cols)]
    elif _keep == 'unused':
        unused_cols = list_diff(data.columns, used_cols)
        data = data[list_diff(unused_cols, outcols) + outcols]
    elif _keep == 'none':
        data = data[list_diff(get_groups(_data), outcols) + outcols]

    return data

@register_verb(DataFrame, context=Context.NAME)
def pivot_longer(
        _data,
        cols,
        names_to="name",
        names_prefix=None,
        names_sep=None,
        names_pattern=None,
        names_ptypes=None,
        names_transform=None,
        # names_repair="check_unique",
        values_to="value",
        values_drop_na=False,
        values_ptypes=None,
        values_transform=None
):
    columns = select_columns(_data.columns, cols)
    id_columns = list_diff(_data.columns, columns)
    var_name = '__tmp_names_to__' if names_pattern or names_sep else names_to
    ret = _data.melt(
        id_vars=id_columns,
        value_vars=columns,
        var_name=var_name,
        value_name=values_to,
    )

    if names_pattern:
        ret[names_to] = ret['__tmp_names_to__'].str.extract(names_pattern)
        ret.drop(['__tmp_names_to__'], axis=1, inplace=True)

    if names_prefix:
        ret[names_to] = ret[names_to].str.replace(names_prefix, '')

    if '.value' in names_to:
        ret2 = ret.pivot(columns='.value', values=values_to)
        rest_columns = list_diff(ret.columns, ['.value', values_to])
        ret2.loc[:, rest_columns] = ret.loc[:, rest_columns]

        ret2_1 = ret2.iloc[:(ret2.shape[0] // 2), ]
        ret2_2 = ret2.iloc[(ret2.shape[0] // 2):, ].reset_index()
        ret = ret2_1.assign(**{col: ret2_2[col] for col in ret2_1.columns
                               if ret2_1[col].isna().all()})

    if values_drop_na:
        ret.dropna(subset=[values_to], inplace=True)
    if names_ptypes:
        for key, ptype in names_ptypes.items():
            ret[key] = ret[key].astype(ptype)
    if values_ptypes:
        for key, ptype in values_ptypes.items():
            ret[key] = ret[key].astype(ptype)
    if names_transform:
        for key, tform in names_transform.items():
            ret[key] = ret[key].apply(tform)
    if values_transform:
        for key, tform in values_transform.items():
            ret[key] = ret[key].apply(tform)

    return ret

@register_verb(DataFrame, context=Context.NAME)
def relocate(_data, column, *columns, _before=None, _after=None):
    all_columns = _data.columns.to_list()
    columns = select_columns(all_columns, column, *columns)
    rest_columns = list_diff(all_columns, columns)
    if _before is not None and _after is not None:
        raise ColumnNameInvalidError(
            'Only one of _before and _after can be specified.'
        )
    if _before is None and _after is None:
        rearranged = columns + rest_columns
    elif _before is not None:
        before_columns = select_columns(all_columns, _before)
        cutpoint = min(rest_columns.index(bcol) for bcol in before_columns)
        rearranged = rest_columns[:cutpoint] + columns + rest_columns[cutpoint:]
    else: #_after
        after_columns = select_columns(all_columns, _after)
        cutpoint = max(rest_columns.index(bcol) for bcol in after_columns) + 1
        rearranged = rest_columns[:cutpoint] + columns + rest_columns[cutpoint:]
    return _data[rearranged]

@register_verb(DataFrame, context=Context.NAME)
def group_by(
        _data: DataFrame,
        column: str,
        *columns: str,
        _add: bool = False
) -> DataFrame:
    data = _data.copy()
    columns = select_columns(data.columns, column, *columns)
    set_groups(data, columns, _add)
    return data


@register_verb(DataFrame)
def summarise(
        _data: DataFrame,
        *acrosses: Across,
        _groups: Optional[str] = None,
        **kwargs: Any
) -> DataFrame:
    """Summarise each group to fewer rows

    See: https://dplyr.tidyverse.org/reference/summarise.html
    """
    across = {} # no need OrderedDict in python3.7+ anymore
    for acrs in acrosses:
        if isinstance(acrs, Across):
            across.update(acrs.evaluate(Context.DATA, _data))
        else:
            across.update(acrs)

    across.update(kwargs)
    kwargs = across

    rowwise = get_rowwise(_data)
    groups = get_groups(_data)
    if rowwise:
        if rowwise is True:
            ret = DataFrame({'__plyrda__': [0] * _data.shape[0]})
        else:
            ret = _data[rowwise]
        set_rowwise(ret, rowwise)
    elif groups:
        grouped = _data.groupby(by=groups)
        ret = grouped.size().to_frame('__plyrda__')
        set_groups(ret, groups)
    else:
        ret = DataFrame({'__plyrda__': [0]})

    for key, val in kwargs.items():
        if isinstance(val, CAcross):
            val.names = key
        if isinstance(val, Across):
            val = DataFrame(val.evaluate(Context.DATA, _data))

        if isinstance(val, Series) and val.index.name == ret.index.name:
            # in case val has more rows than ret, ie. quantile
            # we expand ret
            ret =  ret.loc[val.index, :]
            ret[key] = val
        else:
            ret[key] = align_value(val, ret)

    if '__plyrda__' in ret.columns:
        ret.drop(columns=['__plyrda__'], inplace=True)

    ret.reset_index(level=groups, inplace=True)
    ret.reset_index(drop=True, inplace=True)

    if _groups is None:
        if rowwise and rowwise is not True:
            set_groups(ret, rowwise)
        elif ret.shape[0] == 1:
            _groups = 'drop_last'
        elif isinstance(ret.index, MultiIndex):
            _groups = 'drop_last'

    if _groups == 'drop_last':
        set_groups(ret, groups[:-1])
    elif _groups == 'keep':
        set_groups(ret, groups[:])
    elif _groups == 'rowwise':
         set_rowwise(ret, True)
    # else:
    # todo: raise

    return ret

summarize =summarise

@register_verb(DataFrame, context=Context.NAME)
def arrange(
        _data: DataFrame,
        column: Union[UnaryNeg, Across, str],
        *columns: Union[UnaryNeg, Across, str],
        _by_group: bool = False
) -> DataFrame:
    columns = (column, ) + columns
    by = []
    ascending = []
    for column in Collection(columns):
        if isinstance(column, UnaryNeg):
            cols = select_columns(_data.columns, column.elems)
            by.extend(cols)
            ascending.extend([False] * len(cols))
        elif isinstance(column, Across):
            cols = column.evaluate(Context.NAME)
            if any(isinstance(col, UnaryNeg) for col in cols):
                cols = list(chain(*(col.elems for col in cols)))
                by.extend(cols)
                ascending.extend([False] * len(cols))
            else:
                by.extend(cols)
                ascending.extend([True] * len(cols))
        else:
            cols = select_columns(_data.columns, column)
            by.extend(cols)
            ascending.extend([True] * len(cols))

    if _by_group and is_grouped(_data):
        groups = get_groups(_data)
        by = groups + list_diff(by, groups)
        ascending = [True] * len(groups) + ascending
    return _data.sort_values(by, ascending=ascending)

@register_verb
def rowwise(_data: DataFrame, *columns: str) -> DataFrame:
    data = _data.copy()
    if not columns:
        columns = True
    else:
        columns = select_columns(data.columns, columns)
    set_rowwise(data, columns)
    return data

@register_verb(context=Context.DATA)
def filter(_data, condition, *conditions, _preserve=False):
    # check condition, conditions
    print(condition)
    for cond in conditions:
        condition = condition & cond
    try:
        condition = condition.values.flatten()
    except AttributeError:
        ...
    print(condition)
    return _data[condition]
