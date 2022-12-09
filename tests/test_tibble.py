import pytest  # noqa: F401

from datar.core.utils import NotImplementedByCurrentBackendError
from datar.tibble import (
    add_column,
    add_row,
    as_tibble,
    column_to_rownames,
    deframe,
    enframe,
    has_rownames,
    remove_rownames,
    rowid_to_column,
    rownames_to_column,
    tibble,
    tibble_,
    tibble_row,
    tribble,
)


@pytest.mark.parametrize("verb, data, args, kwargs", [
    (add_column, None, [1], None),
    (add_row, None, [1], None),
    (as_tibble, None, [], None),
    (column_to_rownames, None, [], None),
    (deframe, None, [], None),
    (enframe, None, [], None),
    (has_rownames, None, [], None),
    (remove_rownames, None, [], None),
    (rowid_to_column, None, ["x"], None),
    (rownames_to_column, None, ["x"], None),
    (tibble, None, [], None),
    (tibble_, None, [], None),
    (tibble_row, None, [], None),
    (tribble, None, [], None),
])
def test_default_impl(verb, data, args, kwargs):
    kwargs = kwargs or {}
    with pytest.raises(NotImplementedByCurrentBackendError):
        verb(data, *args, **kwargs)
