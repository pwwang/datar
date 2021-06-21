import pytest

import numpy
from datar.base.casting import *
from datar.base.factor import *
from .conftest import assert_iterable_equal

def test_as_double():
    assert isinstance(as_double(1), numpy.double)
    assert as_double(numpy.array([1,2])).dtype == numpy.double
    assert numpy.array(as_double([1,2])).dtype == numpy.double

def test_as_float():
    assert isinstance(as_double(1), numpy.float_)

def test_as_integer():
    assert isinstance(as_integer(1), numpy.int_)
    fct = factor(list('abc'))
    assert_iterable_equal(as_integer(fct), [0,1,2])
    # NAs kept
    out = as_integer(NA)
    assert_iterable_equal([out], [NA])
    out = as_integer(numpy.array([1.0,NA]))
    assert out.dtype == object

def test_as_numeric():
    assert as_numeric('1') == 1
    assert as_numeric('1.1', _keep_na=False) == 1.1
    assert_iterable_equal(as_numeric(['1', NA]), [1, NA])
