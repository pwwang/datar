

"""Public APIs for dplyr"""

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
from .glimpse import glimpse
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

from ..core.import_names_conflict import (
    handle_import_names_conflict as _handle_import_names_conflict
)

_conflict_names = {"filter", "slice"}

__all__, _getattr = _handle_import_names_conflict(locals(), _conflict_names)

if _getattr is not None:
    __getattr__ = _getattr
