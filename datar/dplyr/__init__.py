"""Public APIs for dplyr"""

# pylint: disable=redefined-builtin, unused-import
from .verbs import (
    transmutate,
    group_cols,
    group_map, group_modify, group_walk, group_trim,
    group_split, with_groups, pull, rename,
    rename_with, bind_cols, bind_rows,
    union_all, setequal, nest_join
)
from .funcs import (
    starts_with, ends_with, contains, matches,
    all_of, any_of,
    n_distinct, cummean, cumall,
    cumany, lead, lag, num_range, recode, recode_factor, recode_categorical,
    coalesce, na_if, near, nth, first, last, between
)

from .group_by import group_by_drop_default, group_by, rowwise, ungroup
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
from .context import (
    n, cur_data_all, cur_group_id, cur_group_rows,
    cur_group, cur_data, cur_column
)
from .mutate import mutate
from .select import select
from .across import across, c_across, if_all, if_any
from .tidyselect import where, everything, last_col
from .if_else import if_else, case_when
from .relocate import relocate
from .filter import filter
from .distinct import distinct
from .slice import  (
    slice, slice_head, slice_tail, slice_min,
    slice_max, slice_sample
)
from .rank import (
    ntile, row_number, min_rank, dense_rank, percent_rank, cume_dist
)
