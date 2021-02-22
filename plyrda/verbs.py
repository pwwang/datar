from itertools import chain
from plyrda.contexts import ContextEvalWithUsedRefs
from re import DEBUG
import sys
from pandas.core.groupby.groupby import GroupBy
from pandas.core.indexes.multi import MultiIndex
from pandas.core.groupby import DataFrameGroupBy
from pandas.core.series import Series
from pipda.utils import Expression, evaluate_expr
from plyrda.group_by import get_groups, get_rowwise, is_grouped, set_groups, set_rowwise
from typing import Any, Iterable, List, Mapping, Optional, Union
from pandas import DataFrame
from pipda import register_verb, Context

from .utils import align_value, list_diff, list_union, select_columns
from .middlewares import Across, CAcross, Collection, RowwiseDataFrame, Inverted
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

@head.register(DataFrameGroupBy)
def _(_data: DataFrameGroupBy, n: int = 5) -> DataFrame:
    return _data.obj.head(n)

@register_verb(DataFrame)
def tail(_data, n=5):
    """Get the last n rows of the dataframe

    Args:
        n: The number of rows to return

    Returns:
        The dataframe with last N rows
    """
    return _data.tail(n)

@tail.register(DataFrameGroupBy)
def _(_data: DataFrameGroupBy, n: int = 5) -> DataFrame:
    return _data.obj.tail(n)

@register_verb((DataFrame, DataFrameGroupBy))
def select(
        _data: DataFrame,
        *columns: Union[str, Iterable[str], Inverted],
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

@register_verb(DataFrame, context=Context.UNSET)
def mutate(
        _data: DataFrame,
        *acrosses: Across,
        _keep: str = 'all',
        _before: Optional[str] = None,
        _after: Optional[str] = None,
        **kwargs: Any
) -> DataFrame:

    context = ContextEvalWithUsedRefs()
    if _before is not None:
        _before = evaluate_expr(_before, _data, Context.SELECT)
    if _after is not None:
        _after = evaluate_expr(_after, _data, Context.SELECT)

    across = {} # no need OrderedDict in python3.7+ anymore
    for acrs in acrosses:
        acrs = evaluate_expr(acrs, _data, context)
        if isinstance(acrs, Across):
            across.update(acrs.evaluate(context))
        else:
            across.update(acrs)

    across.update(kwargs)
    kwargs = across

    if isinstance(_data, RowwiseDataFrame):
        data = RowwiseDataFrame(_data, _data.rowwise)
    else:
        data = _data.copy()

    for key, val in kwargs.items():
        if val is None:
            data.drop(columns=[key], inplace=True)
            continue
        val = evaluate_expr(val, data, context)
        if isinstance(val, CAcross):
            val.names = key
        if isinstance(val, Across):
            val = DataFrame(val.evaluate(context, data))

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
    used_cols = context.used_refs.keys()
    if _keep == 'used':
        data = data[list_union(used_cols, outcols)]
    elif _keep == 'unused':
        unused_cols = list_diff(data.columns, used_cols)
        data = data[list_union(unused_cols, outcols)]
    elif _keep == 'none':
        data = data[outcols]
    # else:
    # raise
    if (isinstance(_data, RowwiseDataFrame) and
            not isinstance(data, RowwiseDataFrame)):
        return RowwiseDataFrame(data, rowwise=_data.rowwise)

    return data

@mutate.register(DataFrameGroupBy)
def _(
        _data: DataFrameGroupBy,
        *acrosses: Across,
        _keep: str = 'all',
        _before: Optional[str] = None,
        _after: Optional[str] = None,
        **kwargs: Any
) -> DataFrameGroupBy:
    ret = mutate(
        _data.obj,
        *acrosses,
        _keep=_keep,
        _before=_before,
        _after=_after,
        **kwargs
    )
    return ret.groupby(_data.keys)

@register_verb(DataFrame, context=Context.SELECT)
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

@register_verb(DataFrame, context=Context.SELECT)
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

@register_verb(DataFrame)
def group_by(
        _data: DataFrame,
        *columns: str,
        _add: bool = False, # not working, since _data is not grouped
        **kwargs: Any
) -> DataFrameGroupBy:
    if kwargs:
        _data = mutate(_data, **kwargs)

    columns = select_columns(_data.columns, *columns, *kwargs.keys())
    return _data.groupby(columns)

@group_by.register(DataFrameGroupBy)
def _(
    _data: DataFrameGroupBy,
    *columns: str,
    _add: bool = False,
    **kwargs: Any
) -> DataFrameGroupBy:
    if kwargs:
        _data = mutate(_data, **kwargs)

    columns = select_columns(_data.obj.columns, *columns, *kwargs.keys())
    if _add:
        groups = Collection(_data.keys) + columns
        return _data.obj.groupby(groups)
    return _data.obj.groupby(columns)

@register_verb(DataFrameGroupBy)
def ungroup(_data: DataFrameGroupBy) -> DataFrame:
    return _data.obj

@register_verb(DataFrameGroupBy)
def group_vars(_data: DataFrameGroupBy) -> List[str]:
    return _data.keys

@register_verb((DataFrame, DataFrameGroupBy))
def summarise(
        _data: Union[DataFrame, DataFrameGroupBy],
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
            across.update(acrs.evaluate(Context.EVAL, _data))
        else:
            across.update(acrs)

    across.update(kwargs)
    kwargs = across

    ret = None
    if isinstance(_data, RowwiseDataFrame) and _data.rowwise is not True:
        ret = _data[_data.rowwise]

    for key, val in kwargs.items():
        if isinstance(val, CAcross):
            val.names = key
        if isinstance(val, Across):
            val = DataFrame(val.evaluate(Context.EVAL, _data))

        if ret is None:
            try:
                ret = DataFrame(val, columns=[key])
            except ValueError:
                ret = DataFrame([val], columns=[key])
        # if isinstance(val, Series) and val.index.name == ret.index.name:
        #     # in case val has more rows than ret, ie. quantile
        #     # we expand ret
        #     ret =  ret.loc[val.index, :]
        #     ret[key] = val
        else:
            ret[key] = align_value(val, ret)

    if _groups == 'rowwise':
        return RowwiseDataFrame(ret)

    if not isinstance(_data, DataFrameGroupBy):
        return ret

    if _groups is None:
        if ret.shape[0] == 1:
            _groups = 'drop_last'
        elif isinstance(ret.index, MultiIndex):
            _groups = 'drop_last'

    ret.reset_index(inplace=True)

    if _groups == 'drop_last':
        return ret.groupby(_data.keys[:-1]) if _data.keys[:-1] else ret

    if _groups == 'keep':
        return ret.groupby(_data.keys)

    # else:
    # todo: raise
    return ret

summarize = summarise

@register_verb(DataFrame, context=Context.SELECT)
def arrange(
        _data: DataFrame,
        column: Union[Inverted, Across, str],
        *columns: Union[Inverted, Across, str],
        _by_group: bool = False
) -> DataFrame:
    columns = (column, ) + columns
    by = []
    ascending = []
    for column in Collection(columns):
        if isinstance(column, Inverted):
            cols = select_columns(_data.columns, column.elems)
            by.extend(cols)
            ascending.extend([False] * len(cols))
        elif isinstance(column, Across):
            cols = column.evaluate(Context.SELECT)
            if any(isinstance(col, Inverted) for col in cols):
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

@register_verb(DataFrame)
def rowwise(_data: DataFrame, *columns: str) -> RowwiseDataFrame:
    columns = select_columns(_data.columns, columns)
    return RowwiseDataFrame(_data, rowwise=columns)

@rowwise.register(DataFrameGroupBy)
def _(_data: DataFrameGroupBy, *columns: str) -> RowwiseDataFrame:
    columns = select_columns(_data.obj.columns, columns)
    return RowwiseDataFrame(_data, rowwise=columns)

@register_verb(DataFrame, context=Context.EVAL)
def filter(
        _data: DataFrame,
        condition,
        *conditions,
        _preserve=False
):
    # check condition, conditions
    for cond in conditions:
        condition = condition & cond
    try:
        condition = condition.values.flatten()
    except AttributeError:
        ...

    return _data[condition]

@filter.register(DataFrameGroupBy)
def _(_data: DataFrameGroupBy, condition, *conditions, _preserve=True):
    for cond in conditions:
        condition = condition & cond
    try:
        condition = condition.values.flatten()
    except AttributeError:
        ...
    ret = _data.obj[condition]
    if _preserve:
        return ret.groupby(_data.keys)
    return ret

@register_verb((DataFrame, DataFrameGroupBy), context=Context.UNSET)
def debug(
        _data: Union[DataFrame, DataFrameGroupBy],
        *args: Any,
        context: Union[Context, str] = Context.SELECT,
        **kwargs: Any
) -> None: # not going any further

    def print_msg(msg: str, end: str = "\n"):
        sys.stderr.write(f"[plyrda] DEBUG: {msg}{end}")

    if isinstance(_data, DataFrameGroupBy):
        print_msg("# DataFrameGroupBy:")
        print_msg(_data.describe())
    else:
        print_msg("# DataFrame:")
        print_msg(_data)

    if args:
        for i, arg in enumerate(args):
            print_msg(f"# Arg#{i+1}")
            print_msg("## Raw")
            print_msg(arg)
            print_msg("## Evaluated")
            print_msg(evaluate_expr(arg, _data, context))

    if kwargs:
        for key, val in kwargs.items():
            print_msg(f"# Kwarg#{key}")
            print_msg("## Raw")
            print_msg(val)
            print_msg("## Evaluated")
            print_msg(evaluate_expr(val, _data, context))
