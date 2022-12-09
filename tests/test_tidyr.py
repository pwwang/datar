import pytest

from datar.core.utils import NotImplementedByCurrentBackendError
from datar.tidyr import (
    chop,
    complete,
    crossing,
    drop_na,
    expand,
    extract,
    fill,
    full_seq,
    nest,
    nesting,
    pack,
    pivot_longer,
    pivot_wider,
    replace_na,
    separate,
    separate_rows,
    unchop,
    uncount,
    unite,
    unnest,
    unpack,
)


@pytest.mark.parametrize("verb, data, args, kwargs", [
    (chop, None, [], None),
    (complete, None, [], None),
    (crossing, None, [], None),
    (drop_na, None, [], None),
    (expand, None, [], None),
    (extract, None, [1, 1], None),
    (fill, None, [], None),
    (full_seq, None, [1], None),
    (nest, None, [], None),
    (nesting, None, [], None),
    (pack, None, [], None),
    (pivot_longer, None, [1], None),
    (pivot_wider, None, [], None),
    (replace_na, None, [], None),
    (separate, None, [1, 1], None),
    (separate_rows, None, [], None),
    (unchop, None, [], None),
    (uncount, None, [1], None),
    (unite, None, [1], None),
    (unnest, None, [], None),
    (unpack, None, [1], None),
])
def test_default_impl(verb, data, args, kwargs):
    kwargs = kwargs or {}
    with pytest.raises(NotImplementedByCurrentBackendError):
        verb(data, *args, **kwargs)
