"""Public APIs for dplyr"""

# pylint: disable=redefined-builtin, unused-import
from .verbs import (
    relocate, rowwise, transmutate,
    ungroup, group_cols,
    group_map, group_modify, group_walk, group_trim,
    group_split, with_groups, filter, distinct, pull, rename,
    rename_with, slice, slice_head, slice_tail, slice_min,
    slice_max, slice_sample, bind_cols, bind_rows, intersect,
    union, setdiff, union_all, setequal, nest_join
)
from .funcs import (
    across, c_across, starts_with, ends_with, contains, matches,
    everything, last_col, all_of, any_of, where, if_any, if_all,
    min_rank, dense_rank, percent_rank, cume_dist, ntile, case_when,
    if_else, n_distinct, row_number, cur_group_id, cur_group_rows,
    cur_group, cur_data, cur_column, cummean, cumall,
    cumany, lead, lag, num_range, recode, recode_factor, recode_categorical,
    coalesce, na_if, near, nth, first, last, between
)

from .group_by import group_by_drop_default, group_by
from .group_data import (
    group_data, group_keys, group_rows, group_indices,
    group_vars, group_size, n_groups
)
from .join import (
    inner_join, left_join, right_join, full_join, semi_join, anti_join
)
from .summarise import summarise, summarize
from .count_tally import count, tally, add_count, add_tally
from .desc import desc
from .arrange import arrange
from .context import n, cur_data_all
from .mutate import mutate
from .select import select
