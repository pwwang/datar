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

def test_as_numeric():
    assert as_numeric('1') == 1
    assert as_numeric('1.1') == 1.1
