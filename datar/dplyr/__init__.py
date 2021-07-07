"""Public APIs for dplyr"""

# pylint: disable=unused-import
from .across import across, c_across, if_all, if_any
from .arrange import arrange
from .bind import bind_cols, bind_rows
from .context import (
    cur_column,
    cur_data,
    cur_data_all,
    cur_group,
    cur_group_id,
    cur_group_rows,
    n,
)
from .count_tally import add_count, add_tally, count, tally
from .desc import desc
from .dfilter import filter as filter_
from .distinct import distinct, n_distinct
from .dslice import slice as slice_
from .dslice import slice_head, slice_max, slice_min, slice_sample, slice_tail
from .funs import (
    between,
    coalesce,
    cumall,
    cumany,
    cummean,
    first,
    last,
    na_if,
    near,
    nth,
)
from .group_by import group_by, group_by_drop_default, rowwise, ungroup
from .group_data import (
    group_cols,
    group_data,
    group_indices,
    group_keys,
    group_rows,
    group_size,
    group_vars,
    n_groups,
)
from .group_iter import (
    group_map,
    group_modify,
    group_split,
    group_trim,
    group_walk,
    with_groups,
)
from .if_else import case_when, if_else
from .join import (
    anti_join,
    full_join,
    inner_join,
    left_join,
    nest_join,
    right_join,
    semi_join,
)
from .lead_lag import lag, lead
from .mutate import mutate, transmute
from .order_by import order_by, with_order
from .pull import pull
from .rank import (
    cume_dist,
    dense_rank,
    min_rank,
    ntile,
    percent_rank,
    row_number,
)
from .recode import recode, recode_categorical, recode_factor
from .relocate import relocate
from .rename import rename, rename_with
from .rows import rows_delete, rows_insert, rows_patch, rows_update, rows_upsert
from .select import select
from .sets import intersect, setdiff, setequal, union, union_all
from .summarise import summarise, summarize
from .tidyselect import (
    all_of,
    any_of,
    contains,
    ends_with,
    everything,
    last_col,
    matches,
    num_range,
    starts_with,
    where,
)

# make sure builtin names are included when
# from datar.dplyr import *
_builtin_names = {  # pylint: disable=invalid-name
    "filter": filter_,
    "slice": slice_,
}

__all__ = [var_ for var_ in locals() if not var_.startswith("_")]
__all__.extend(_builtin_names)

# warn when builtin names are imported directly
# pylint: disable=wrong-import-position
from ..core.warn_builtin_names import warn_builtin_names

__getattr__ = warn_builtin_names(**_builtin_names)
