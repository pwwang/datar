import pytest

import numpy as np
from datar2.base.casting import *
from datar2.base.factor import *
from ..conftest import assert_iterable_equal


def test_as_double():
    assert isinstance(as_double(1), np.double)
    assert as_double(np.array([1, 2])).dtype == np.double
    assert np.array(as_double([1, 2])).dtype == np.double


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


def test_as_numeric():
    assert as_numeric("1") == 1
    assert as_numeric("1.1", _keep_na=False) == 1.1
    assert_iterable_equal(as_numeric(["1", np.nan]), [1, np.nan])
