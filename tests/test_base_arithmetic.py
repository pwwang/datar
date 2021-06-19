import pytest

from datar.base import NA
from datar.base.arithmetic import *
from .conftest import assert_iterable_equal

def test_sum():
    assert sum(1) == 1
    assert sum([1,2]) == 3
    assert_iterable_equal([sum([1,2, NA])], [NA])
    assert sum([1,2,NA], na_rm=True) == 3

def test_mean():
    assert mean(1) == 1
    assert mean([1.0, 2.0]) == 1.5

def test_median():
    assert median(1) == 1
    assert median([1.0, 20, 3.0]) == 3.0

def test_min():
    assert min(1) == 1
    assert min([1,2,3]) == 1

def test_max():
    assert max(1) == 1
    assert max([1,2,3]) == 3

def test_var():
    with pytest.warns(RuntimeWarning):
        assert_iterable_equal([var(1)], [NA])
    assert var([1,2,3]) == 1

def test_pmin():
    assert pmin(1,2,3) == 1
    assert_iterable_equal(pmin(1,[-1, 2], [0, 3]), [-1, 1])

def test_pmax():
    assert pmax(1,2,3) == 3
    assert_iterable_equal(pmax(1,[-1, 2], [0, 3]), [1, 3])

def test_round():
    assert round(1.23456) == 1.0
    assert_iterable_equal(round([1.23456, 3.45678]), [1.0, 3.0])
    assert_iterable_equal(round([1.23456, 3.45678], 1), [1.2, 3.5])

def test_sqrt():
    assert sqrt(1) == 1
    with pytest.warns(RuntimeWarning):
        assert_iterable_equal([sqrt(-1)], [NA])


def test_abs():
    assert abs(1) == 1
    assert_iterable_equal(abs([-1, 1]), [1, 1])

def test_ceiling():
    assert ceiling(1.1) == 2
    assert_iterable_equal(ceiling([-1.1, 1.1]), [-1, 2])

def test_floor():
    assert floor(1.1) == 1
    assert_iterable_equal(floor([-1.1, 1.1]), [-2, 1])
