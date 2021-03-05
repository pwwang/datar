import functools

import numpy
from pandas import Categorical
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
