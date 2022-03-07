from pandas import Series
import pytest  # noqa

import numpy as np
from datar.base.factor import (
    droplevels,
    factor,
    levels,
    as_factor,
    is_factor,
    nlevels,
    is_ordered,
    ordered,
    # is_categorical,
)
from ..conftest import assert_iterable_equal


def test_droplevels():
    fct = Series(factor([1, 2, 3], levels=[1, 2, 3, 4]))
    out = droplevels(fct)
    assert_iterable_equal(fct, out)
    assert_iterable_equal(levels(out), [1, 2, 3])


def test_levels():
    assert levels(1) is None


def test_factor():
    out = factor()
    assert len(out) == 0
    assert len(levels(out)) == 0
    assert len(factor(2)) == 1

    out = factor([1, 2, 3], exclude=None)
    assert len(out) == 3

    out = factor([1, 2, 3], exclude=1)
    assert_iterable_equal(out, [np.nan, 2, 3])
    assert_iterable_equal(levels(out), [2, 3])

    out = factor(out)
    assert_iterable_equal(out, [np.nan, 2, 3])
    assert_iterable_equal(levels(out), [2, 3])


def test_as_facotr():
    out = as_factor([1, 2, 3])
    assert_iterable_equal(out, [1, 2, 3])
    assert_iterable_equal(levels(out), [1, 2, 3])


def test_is_factor():
    out = as_factor([])
    assert is_factor(out)
    assert not is_factor([])


def test_nlevels():
    assert nlevels(1) == 0
    assert nlevels(factor([1, 2, 3])) == 3


def test_is_ordered():
    assert not is_ordered(1)
    assert not is_ordered(factor())
    assert is_ordered(factor(ordered=True))


def test_ordered():
    o = ordered([3, 1, 2])
    assert_iterable_equal(o, [3, 1, 2])
