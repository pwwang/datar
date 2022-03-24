import pytest  # noqa

import numpy as np
from datar.core.backends.pandas.api.types import is_float_dtype
from datar.base.casting import (
    as_double,
    as_integer,
    as_numeric,
)
from datar.base.factor import (
    factor,
)
from datar.tibble import tibble
from ..conftest import assert_iterable_equal


def test_as_double():
    assert isinstance(as_double(1), np.double)
    assert as_double(np.array([1, 2])).dtype == np.double
    assert np.array(as_double([1, 2])).dtype == np.double

    x = tibble(a=[1, 2, 3]).rowwise()
    out = as_double(x.a)
    assert is_float_dtype(out.obj)
    assert out.is_rowwise


def test_as_float():
    assert isinstance(as_double(1), np.float_)


def test_as_integer():
    assert isinstance(as_integer(1), np.int_)
    fct = factor(list("abc"))
    assert_iterable_equal(as_integer(fct), [0, 1, 2])
    # np.nans kept
    out = as_integer(np.nan)
    assert_iterable_equal([out], [np.nan])
    out = as_integer(np.array([1.0, np.nan]))
    assert out.dtype == object

    x = tibble(a=factor(["a", "b"])).group_by("a")
    out = as_integer(x.a)
    assert_iterable_equal(out.obj, [0, 1])


def test_as_numeric():
    assert as_numeric("1") == 1
    assert as_numeric("1.1", _keep_na=False) == 1.1
    assert_iterable_equal(as_numeric(["1", np.nan]), [1, np.nan])
