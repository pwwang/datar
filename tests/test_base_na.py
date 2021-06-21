import pytest

from datar.base.na import *
from datar.base.null import *
from .conftest import assert_iterable_equal

def test_is_na():
    assert is_na(NA)
    assert is_na(NULL) # instead of logical(0) in R
    assert not is_na(1)
    assert not is_na('a')
    assert not is_na('NA')
    assert not is_na('<NA>')
    assert_iterable_equal(is_na([1, NA]), [False, True])

def test_any_na():
    assert any_na(NA)
    assert not any_na(1)
    assert not any_na('<NA>')
    assert any_na([1, NA])
    assert not any_na([1, [2, NA]])
    assert any_na([1, [2, NA]], recursive=True)
    assert not any_na([1, [2, 3]], recursive=True)
