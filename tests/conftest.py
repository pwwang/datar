import functools

import pytest
import numpy
from pandas import Categorical, isna
from pandas.core.series import Series

@functools.singledispatch
def to_list(x):
    return x

@to_list.register(numpy.ndarray)
def _(x):
    return x.tolist()

@to_list.register(Series)
def _(x):
    return x.values.tolist()

@to_list.register(Categorical)
def _(x):
    return (x.tolist(), x.categories.tolist())

def assert_equal(x, y):
    assert to_list(x) == to_list(y)

def assert_iterable_equal(x, y, na=8525.8525, approx=False):
    x = [na if isna(elt) else elt for elt in x]
    y = [na if isna(elt) else elt for elt in y]
    if approx is True:
        x = pytest.approx(x)
    elif approx:
        x = pytest.approx(x, rel=approx)
    assert x == y

def assert_factor_equal(x, y, na=8525.8525, approx=False):
    xlevs = x.categories
    ylevs = y.categories
    assert_iterable_equal(x, y, na=na, approx=approx)
    assert_iterable_equal(xlevs, ylevs, na=na, approx=approx)

def is_installed(pkg):
    try:
        __import__(pkg)
        return True
    except ImportError:
        return False
