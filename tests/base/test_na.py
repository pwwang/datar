import pytest  # noqa

from datar.base.na import NA, is_na, any_na
from datar.base.null import NULL
from ..conftest import assert_iterable_equal


def test_is_na():
    assert is_na(NA)[0]
    assert is_na(NULL)  # instead of logical(0) in R
    assert not is_na(1)[0]
    assert not is_na("a")[0]
    assert not is_na("NA")[0]
    assert not is_na("<NA>")[0]
    assert_iterable_equal(is_na([1, NA]), [False, True])


def test_any_na():
    assert any_na(NA)
    assert not any_na(1)
    assert not any_na("<NA>")
    assert any_na([1, NA])
    assert not any_na([1, [2, NA]])
    # assert any_na([1, [2, NA]], recursive=True)
    # assert not any_na([1, [2, 3]], recursive=True)
