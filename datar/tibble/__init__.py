"""APIs for R-tibble"""
from .tibble import tibble, tibble_row, tribble, zibble
from .verbs import (
    enframe,
    deframe,
    add_row,
    add_case,
    add_column,
    has_rownames,
    has_index,
    remove_index,
    remove_rownames,
    drop_index,
    rownames_to_column,
    index_to_column,
    rowid_to_column,
    column_to_rownames,
    column_to_index,
)
