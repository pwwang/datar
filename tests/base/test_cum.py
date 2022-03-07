import pytest  # noqa

import numpy as np
from datar.base.cum import (
    cummax,
    cummin,
    cumprod,
    cumsum,
)
from ..conftest import assert_iterable_equal


def test_cumsum():
    assert_iterable_equal(cumsum(1), [1])
    assert_iterable_equal(cumsum([1, 2, 3]), [1, 3, 6])
    assert_iterable_equal(cumsum([1, np.nan, 3]), [1, np.nan, 4])


def test_cumprod():
    assert_iterable_equal(cumprod(1), [1])
    assert_iterable_equal(cumprod([1, 2, 3]), [1, 2, 6])
    assert_iterable_equal(cumprod([1, np.nan, 3]), [1, np.nan, 3])


def test_cummin():
    assert_iterable_equal(cummin(1), [1])
    assert_iterable_equal(cummin([1, 2, 3]), [1, 1, 1])
    assert_iterable_equal(cummin([1, np.nan, 3]), [1, np.nan, 1])


def test_cummax():
    assert_iterable_equal(cummax(1), [1])
    assert_iterable_equal(cummax([1, 2, 3]), [1, 2, 3])
    assert_iterable_equal(cummax([1, np.nan, 3]), [1, np.nan, 3])
