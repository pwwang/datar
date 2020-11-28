from pandas import DataFrame, Series
from pipda import single_dispatch
from .exceptions import PlyrdaColumnNameInvalid
from .common import UnaryNeg, Collection
from .utils import is_neg, check_column, select_columns

from varname import debug

@single_dispatch(DataFrame)
def head(_data, n=5):
    return _data.head(n)

@single_dispatch(DataFrame)
def tail(_data, n=5):
    return _data.tail(n)

@single_dispatch(DataFrame, compile_attrs=False)
def select(_data, column, *columns):
    selected = select_columns(_data.columns, column, *columns)
    return _data[selected]

@single_dispatch(DataFrame, compile_attrs=False)
def drop(_data, column, *columns):
    columns = (column, ) + columns
    if any(is_neg(column) for column in columns):
        raise PlyrdaColumnNameInvalid(
            'No negative columns allow for drop, use select instead.'
        )
    selected = select_columns(_data.columns, *columns)
    selected = [column for column in _data.columns if column not in selected]
    return _data[selected]

@single_dispatch(DataFrame, compile_attrs=False)
def relocate(_data, column, *columns, _before=None, _after=None):
    all_columns = _data.columns.to_list()
    columns = select_columns(all_columns, column, *columns)
    rest_columns = [column for column in all_columns if column not in columns]
    if _before and _after:
        raise PlyrdaColumnNameInvalid(
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

@single_dispatch(DataFrame, compile_attrs=False)
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
# One table verbs
# arrange()

# Arrange rows by column values

# count() tally() add_count() add_tally()

# Count observations by group

# distinct()

# Subset distinct/unique rows

# filter()

# Subset rows using column values

# mutate() transmute()

# Create, modify, and delete columns

# pull()

# Extract a single column

# relocate()

# Change column order

# rename() rename_with()

# Rename columns

# select()

# Subset columns using their names and types

# summarise() summarize()

# Summarise each group to fewer rows

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
# group_by() ungroup()

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

# n() cur_data() cur_data_all() cur_group() cur_group_id() cur_group_rows() cur_column()

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
