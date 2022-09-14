import pytest  # noqa

import numpy as np
from datar.base.testing import (
    is_atomic,
    # is_double,
    is_element,
    # is_float,
    is_in,
    # is_int,
    is_integer,
    is_numeric,
)
from datar.core.backends.pandas import DataFrame, Series
from datar.tibble import tibble
from ..conftest import assert_iterable_equal


def test_is_numeric():
    a = np.array([1, 2, 3])
    assert is_numeric(a)

    assert is_numeric(1)
    assert is_numeric([1, 2, 3])


def test_is_integer():
    a = np.array([1, 2, 3])
    assert is_integer(a)

    assert is_integer(1)

    a = np.array([1, 2], dtype=float)
    assert not is_integer(a)

    a = Series([1, 1]).groupby([1, 2])
    out = is_integer(a)
    assert_iterable_equal(out, [True, True])

    out = is_integer(DataFrame(dict(a=a.obj)))
    assert not out


def test_is_atomic():
    assert is_atomic(1)
    assert not is_atomic([1])


def test_is_element():
    assert_iterable_equal([is_element(1, [1, 2])], [True])
    assert_iterable_equal([is_element(0, [1, 2])], [False])
    assert_iterable_equal(is_element([0, 1], [1, 2]), [False, True])

    df = tibble(x=[1, 2, 1, 2], y=[1, 1, 2, 2]).groupby("y")
    out = is_in(df.x, df.y)
    assert_iterable_equal(out, [True, False, False, True])

    out = is_in(1, df.x)
    assert_iterable_equal(out, [True, True])
    assert_iterable_equal(out.index, [1, 2])

    out = is_in(df.x, [2, 3])
    assert_iterable_equal(out.obj, [False, True, False, True])

    df = tibble(x=[1, 2, 1, 2], y=[1, 1, 2, 2]).rowwise()
    out = is_in(df.x, [2, 3])
    assert out.is_rowwise

    out = is_in(df.x.obj, [2, 3])
    assert_iterable_equal(out, [False, True, False, True])
    assert_iterable_equal(out.index, df.index)


def test_all_any():
    assert all(is_atomic(x) for x in [1, 2, 3])
    assert any(is_atomic(x) for x in [1, [2, 3]])
