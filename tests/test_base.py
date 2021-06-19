import pytest
import warnings
import datetime

import numpy
from pandas import Interval, Categorical, DataFrame
from datar import stats
from datar.base import *

from .conftest import assert_equal, assert_iterable_equal

@pytest.fixture(autouse=True)
def supress_warnings():
    """Ignore pipda not being able to get node to detect piping"""
    warnings.simplefilter("ignore")
    yield
    warnings.simplefilter("default")

def test_c():
    assert c(1,2,3) == [1,2,3]
    assert c(c(1,2), 3) == [1,2,3]

def test_rowcolnames():
    df = DataFrame(dict(x=[1,2,3]))
    assert colnames(df) == ['x']
    assert rownames(df) == [0, 1, 2]
    df = DataFrame([1,2,3], index=['a', 'b', 'c'])
    assert colnames(df) == [0]
    assert rownames(df) == ['a', 'b', 'c']

def test_diag():
    assert dim(diag(3)) == (3,3)
    assert dim(diag(10, 3, 4)) == (3,4)
    x = diag(c(1j,2j))
    assert x.iloc[0,0] == 0+1j
    assert x.iloc[0,1] == 0+0j
    assert x.iloc[1,0] == 0+0j
    assert x.iloc[1,1] == 0+2j
    x = diag(TRUE, 3)
    assert sum(x.values.flatten()) == 3
    x = diag(c(2,1), 4)
    assert_iterable_equal(diag(x), [2,1,2,1])


def test_table():
    # https://www.rdocumentation.org/packages/base/versions/3.6.2/topics/table
    from datar import f
    from datar.datasets import warpbreaks, state_division, state_region, airquality
    z = stats.rpois(100, 5)
    x = table(z)
    assert sum(x.values.flatten()) == 100

    #-----------------
    with data_context(warpbreaks) as _:
        tab = table(f.wool, f.tension)

    assert tab.columns.tolist() == ['H', 'L', 'M']
    assert tab.index.tolist() == ['A', 'B']
    assert_equal(tab.values.flatten(), [9] * 6)

    tab = table(warpbreaks.loc[:, ['wool', 'tension']])
    assert tab.columns.tolist() == ['H', 'L', 'M']
    assert tab.index.tolist() == ['A', 'B']
    assert_equal(tab.values.flatten(), [9] * 6)

    #-----------------
    tab = table(state_division, state_region)
    assert tab.loc['New England', 'Northeast'] == 6

    #-----------------
    # wait for cut
    # with context(airquality) as _:
    #     tab = table(cut(f.Temp, stats.quantile(f.Temp)), f.Month)

    # assert tab.iloc[0,0] == 24

    #-----------------
    a = letters[:3]
    tab = table(a, sample(a))
    assert sum(tab.values.flatten()) == 3

    #-----------------
    tab = table(a, sample(a), dnn=['x', 'y'])
    assert tab.index.name == 'x'
    assert tab.columns.name == 'y'

    #-----------------
    a = c(NA, Inf, (1.0/(i+1) for i in range(3)))
    a = a * 10
    # tab = table(a)
    # assert_equal(tab.values.flatten(), [10] * 4)

    tab = table(a, exclude=None)
    assert_equal(tab.values.flatten(), [10] * 5)

    #------------------
    b = as_factor(c("A","B","C") * 10)
    tab = table(b)
    assert tab.shape == (1, 3)
    assert_equal(tab.values.flatten(), [10] * 3)

    tab = table(b, exclude="B")
    assert tab.shape == (1, 2)
    assert_equal(tab.values.flatten(), [10] * 2)
    assert 'B' not in tab.columns

    #-------------------
    d = factor(rep(c("A","B","C"), 10), levels=c("A","B","C","D","E"))
    tab = table(d, exclude="B", dnn=['x'])
    assert_equal(tab.columns.to_list(), ["A", "C", "D", "E"])
    assert_equal(tab.values.flatten(), [10, 10, 0, 0])

    d2 = factor(rep(c("A","B","C"), 10), levels=c("A","B","C","D","E"))
    tab = table(d, d2, exclude="B")
    assert tab.shape == (4, 4)

    tab = table("abc", "cba", dnn='x')
    assert tab.shape == (3,3)
    assert sum(tab.values.flatten()) == 3

    with data_context(airquality) as _:
        tab = table(f.Ozone, f.Solar_R, exclude=None)
    assert '<NA>' in tab.columns
    assert '<NA>' in tab.index

def test_table_error():
    from datar.datasets import iris, warpbreaks
    with pytest.raises(ValueError):
        table(iris)
    with pytest.raises(ValueError):
        table(warpbreaks, iris)
    with pytest.raises(ValueError):
        table(warpbreaks.wool, iris)
