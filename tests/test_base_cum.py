import pytest

from datar.base.cum import *
from .conftest import assert_iterable_equal

def test_cumsum():
    assert cumsum(1) == 1
    assert_iterable_equal(cumsum([1,2,3]), [1,3,6])
    assert_iterable_equal(cumsum([1,NA,3]), [1,NA,NA])

def test_cumprod():
    assert cumprod(1) == 1
    assert_iterable_equal(cumprod([1,2,3]), [1,2,6])
    assert_iterable_equal(cumprod([1,NA,3]), [1,NA,NA])

def test_cummin():
    assert cummin(1) == 1
    assert_iterable_equal(cummin([1,2,3]), [1,1,1])
    assert_iterable_equal(cummin([1,NA,3]), [1,NA,NA])

def test_cummax():
    assert cummax(1) == 1
    assert_iterable_equal(cummax([1,2,3]), [1,2,3])
    assert_iterable_equal(cummax([1,NA,3]), [1,NA,NA])
