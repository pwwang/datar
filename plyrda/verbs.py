import warnings
from typing import Iterable
from collections import OrderedDict

import pandas
from numpy.lib.arraysetops import isin
from pandas import DataFrame, Series
from pandas.core.groupby import DataFrameGroupBy
from pandas.core.indexes.range import RangeIndex
from pandas.core.indexes.multi import MultiIndex
from pipda import register_verb
from pipda.verb import VerbArg
from .exceptions import PlyrdaColumnNameInvalidException, PlyrdaGroupByException
from .common import UnaryNeg, Collection
from .utils import (
    expand_collections, is_neg, check_column, select_columns, nrows_or_nelems,
    normalize_kw_series, is_grouped
)

from varname import debug

@register_verb(DataFrame)
def head(_data, n=5):
    return _data.head(n)

@register_verb(DataFrame)
def tail(_data, n=5):
    return _data.tail(n)

@register_verb(DataFrame, context='name')
def select(_data, column, *columns):
    selected = select_columns(_data.columns, column, *columns)
    return _data[selected]

@register_verb(DataFrame, context='name')
def relocate(_data, column, *columns, _before=None, _after=None):
    all_columns = _data.columns.to_list()
    columns = select_columns(all_columns, column, *columns)
    rest_columns = [column for column in all_columns if column not in columns]
    if _before and _after:
        raise PlyrdaColumnNameInvalidException(
            'Only one of _before and _after can be specified.'
        )
    if not _before and not _after:
        rearranged = columns + rest_columns
    elif _before:
        before_columns = select_columns(all_columns, *_before)
        cutpoint = min(rest_columns.index(bcol) for bcol in before_columns)
        rearranged = rest_columns[:cutpoint] + columns + rest_columns[cutpoint:]
    else: #_after
        after_columns = select_columns(all_columns, *_after)
        cutpoint = max(rest_columns.index(bcol) for bcol in after_columns) + 1
        rearranged = rest_columns[:cutpoint] + columns + rest_columns[cutpoint:]
    return _data[rearranged]

@register_verb(DataFrame, context='name')
def pivot_longer(_data,
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
                 values_transform=None):
    columns = select_columns(_data.columns, cols)
    id_columns = [column for column in _data.columns if column not in columns]
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
        rest_columns = [column for column in ret.columns
                        if column not in ('.value', values_to)]
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

# see: https://tidyr.tidyverse.org/reference/index.html
# TODO: pivot_wider(), spread(), gather()
# TODO: hoist() unnest_longer() unnest_wider() unnest_auto()
# TODO: nest() unnest()
# TODO: extract() separate() separate_rows() unite()
# TODO: complete() drop_na() expand() crossing() nesting() expand_grid() fill()
# full_seq() replace_na()
# TODO: chop() unchop() pack() unpack() uncount()
# TODO:

# see: https://dplyr.tidyverse.org/reference/index.html
@register_verb(DataFrame, context='name')
def arrange(_data, column, *columns, _by_group=False):
    columns = (column, ) + columns
    by = []
    ascending = []
    for column in expand_collections(columns):
        if isinstance(column, UnaryNeg):
            cols = select_columns(_data.columns, column.operand)
            by.extend(cols)
            ascending.extend([False] * len(cols))
        else:
            cols = select_columns(_data.columns, column)
            by.extend(cols)
            ascending.extend([True] * len(cols))

    if _by_group and is_grouped(_data):
        by = _data.__plyrda_groups__ + [
            col for col in by if col not in _data.__plyrda_groups__
        ]
        ascending = [True] * len(_data.__plyrda_groups__) + ascending
    return _data.sort_values(by, ascending=ascending)

@register_verb(DataFrame, context=None)
def count(_data, *columns, **mutates):
    """Count observations by group

    See: https://dplyr.tidyverse.org/reference/count.html
    """
    columns = [column.compile_to('name') if isinstance(column, VerbArg)
               else column
               for column in columns]
    columns = select_columns(_data.columns, *columns)
    wt = mutates.pop('wt', None)
    wt = wt.compile_to('name') if isinstance(wt, VerbArg) else wt
    sort_n = mutates.pop('sort', False)
    name = mutates.pop('name', 'n')
    mutates = {key: (val.compile_to('data') if isinstance(val, VerbArg)
                     else val) for key, val in mutates.items()}
    data = mutate.pipda(_data, **normalize_kw_series(mutates, _data))
    columns = columns + list(mutates)
    grouped = data.groupby(columns, dropna=False)
    if not wt:
        count_frame = grouped[columns].size().to_frame()
    else:
        count_frame = grouped[wt].sum().to_frame()
    count_frame.columns = [name]
    ret = count_frame.reset_index(level=columns)
    if sort_n:
        ret = ret.sort_values([name], ascending=[False])
    return ret

@register_verb(DataFrame, context='name')
def tally(_data, wt=None, sort=False, name='n'):
    if not is_grouped(_data):
        return DataFrame({
            name: [_data.shape[0] if wt is None else _data[wt].sum()]
        })

    return count.pipda(_data, *_data.__plyrda_groups__,
                       wt=wt, sort=sort, name=name)

@register_verb(DataFrame, context=None)
def add_count(_data, *columns, **mutates):
    sort_n = mutates.pop('sort', False)
    count_frame = count.pipda(_data, *columns, **mutates)
    ret = _data.merge(count_frame, on=count_frame.columns.to_list()[:-1])
    if not sort_n:
        return ret
    return ret.sort_values(count_frame.columns.to_list()[-1],
                           ascending=[False])

@register_verb(DataFrame, context='name')
def add_tally(_data, wt=None, sort=False, name='n'):
    tally_frame = tally.pipda(_data, wt=wt, sort=False, name=name)
    if not is_grouped(_data):
        return _data.assign(**{name: tally_frame.iloc[0, 0]})
    ret = _data.merge(tally_frame, on=tally_frame.columns.to_list()[:-1])
    if not sort:
        return ret
    return ret.sort_values(tally_frame.columns.to_list()[-1], ascending=[False])

@register_verb(DataFrame, context=None)
def distinct(_data, *columns, **mutates):
    keep_all = mutates.pop('_keep_all', False)
    columns = [column.compile_to('name') if isinstance(column, VerbArg)
               else column
               for column in columns]
    columns = select_columns(_data.columns, *columns)
    mutates = {key: (val.compile_to('data') if isinstance(val, VerbArg)
                     else val) for key, val in mutates.items()}
    data = mutate.pipda(_data, **normalize_kw_series(mutates, _data))
    columns = columns + list(mutates)

    if not columns:
        columns = data.columns

    if is_grouped(_data):
        columns = _data.__plyrda_groups__ + [
            col for col in columns if col not in _data.__plyrda_groups__
        ]
    uniq_frame = data.drop_duplicates(columns, ignore_index=True)
    if is_grouped(_data):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            uniq_frame.__plyrda_groups__ = _data.__plyrda_groups__
    return uniq_frame if keep_all else uniq_frame[columns]

@register_verb(DataFrame)
def filter(_data, condition, *conditions, _preserve=False):
    # check condition, conditions
    for cond in conditions:
        condition = condition & cond
    return _data[condition]

@register_verb(DataFrame)
def mutate(_data, **kwargs):
    keep = kwargs.pop('_keep', 'all')
    before = kwargs.pop('_before', None)
    after = kwargs.pop('_after', None)
    kwargs = normalize_kw_series(kwargs, _data)
    data = _data.copy()
    for key, val in kwargs.items():
        data[key] = val
    return data

# transmute()

# Create, modify, and delete columns

# pull()

# Extract a single column

# relocate()

# Change column order

# rename() rename_with()

# Rename columns

# select()

# Subset columns using their names and types

@register_verb(DataFrame, context=None)
def summarise(_data, *crosses, _groups=None, **kwargs):
    """Summarise each group to fewer rows

    See: https://dplyr.tidyverse.org/reference/summarise.html
    """
    cross = OrderedDict()
    for crs in crosses:
        cross.update(crs)

    cross.update(kwargs)
    kwargs = cross

    if not is_grouped(_data):
        kwargs = {key: (val.compile_to('data')
                        if isinstance(val, VerbArg)
                        else val)
                  for key, val in kwargs.items()}
        kwargs = normalize_kw_series(kwargs, _data)
        return DataFrame(kwargs)

    grouped = _data.groupby(by=_data.__plyrda_groups__)
    kwargs = {key: (val.set_data(grouped).compile_to('data')
                    if isinstance(val, VerbArg)
                    else val)
              for key, val in kwargs.items()}

    ret = DataFrame(normalize_kw_series(kwargs))
    ret = ret.reset_index(
        level=_data.__plyrda_groups__
    ).reset_index(drop=True)
    if _groups is None:
        if ret.shape[0] == 1:
            _groups = 'drop_last'
        elif isinstance(ret.index, MultiIndex):
            _groups = 'drop_last'
    if _groups == 'drop_last':
        ret.__plyrda_groups__ = _data.__plyrda_groups__[:-1]
    elif _groups == 'keep':
        ret.__plyrda_groups__ = _data.__plyrda_groups__[:]
    elif _groups == 'rowwise':
        ret.__plyrda_groups__ = ['__rowwise__']

    return ret


summarize =summarise

# slice() slice_head() slice_tail() slice_min() slice_max() slice_sample()

# Subset rows using their positions

# Two table verbs
# bind_rows() bind_cols()

# Efficiently bind multiple data frames by row and column

# reexports

# Objects exported from other packages

# inner_join() left_join() right_join() full_join()

# Mutating joins

# nest_join()

# Nest join

# semi_join() anti_join()

# Filtering joins

# Grouping

@register_verb(DataFrame, context='name')
def group_by(_data, column, *columns, _add=False):
    columns = select_columns(_data.columns, column, *columns)
    if _add and is_grouped(_data):
        setattr(
            _data,
            '__plyrda_groups__',
            _data.__plyrda_groups__ +
            [col for col in columns if col not in _data.__plyrda_groups__]
        )
    else:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            # This is intended
            setattr(_data, '__plyrda_groups__', columns)

    return _data

# ungroup()

# Group by one or more variables

# group_cols()

# Select grouping variables

# rowwise()

# Group input by rows

# Vector functions
# across() c_across()

# Apply a function (or a set of functions) to a set of columns

# between()

# Do values in a numeric vector fall in specified range?

# case_when()

# A general vectorised if

# coalesce()

# Find first non-missing element

# cumall() cumany() cummean()

# Cumulativate versions of any, all, and mean

# desc()

# Descending order

# if_else()

# Vectorised if

# lag() lead()

# Compute lagged or leading values

# order_by()

# A helper function for ordering window function output

# cur_data() cur_data_all() cur_group() cur_group_id() cur_group_rows() cur_column()

# Context dependent expressions

# n_distinct()

# Efficiently count the number of unique values in a set of vector

# na_if()

# Convert values to NA

# near()

# Compare two numeric vectors

# nth() first() last()

# Extract the first, last or nth value from a vector

# row_number() ntile() min_rank() dense_rank() percent_rank() cume_dist()

# Windowed rank functions.

# recode() recode_factor()

# Recode values

# Data
# band_members band_instruments band_instruments2

# Band membership

# starwars

# Starwars characters

# storms

# Storm tracks data

# Remote tables
# auto_copy()

# Copy tables to same source, if necessary

# compute() collect() collapse()

# Force computation of a database query

# copy_to()

# Copy a local data frame to a remote src

# ident()

# Flag a character vector as SQL identifiers

# explain() show_query()

# Explain details of a tbl

# tbl() is.tbl()

# Create a table from a data source

# sql()

# SQL escaping.

# Experimental
# Experimental functions are a testing ground for new approaches that we believe to be worthy of greater exposure. There is no guarantee that these functions will stay around in the future, so please reach out if you find them useful.

# group_map() group_modify() group_walk()

# Apply a function to each group

# group_trim()

# Trim grouping structure

# group_split()

# Split data frame by groups

# with_groups()

# Perform an operation with temporary groups

# Questioning
# We have our doubts about questioning functions. We’re not certain that they’re inadequate, or we don’t have a good replacement in mind, but these functions are at risk of removal in the future.

# all_equal()

# Flexible equality comparison for data frames

# Superseded
# Superseded functions have been replaced by new approaches that we believe to be superior, but we don’t want to force you to change until you’re ready, so the existing functions will stay around for several years.

# sample_n() sample_frac()

# Sample n rows from a table

# top_n() top_frac()

# Select top (or bottom) n rows (by value)

# scoped

# Operate on a selection of variables

# arrange_all() arrange_at() arrange_if()

# Arrange rows by a selection of variables

# distinct_all() distinct_at() distinct_if()

# Select distinct rows by a selection of variables

# filter_all() filter_if() filter_at()

# Filter within a selection of variables

# group_by_all() group_by_at() group_by_if()

# Group by a selection of variables

# mutate_all() mutate_if() mutate_at() transmute_all() transmute_if() transmute_at()

# Mutate multiple columns

# summarise_all() summarise_if() summarise_at() summarize_all() summarize_if() summarize_at()

# Summarise multiple columns

# all_vars() any_vars()

# Apply predicate to all variables

# vars()

# Select variables



@register_verb(DataFrame)
def nrow(_data):
    return _data.shape[0]

@register_verb(DataFrame)
def ncol(_data):
    return _data.shape[1]
