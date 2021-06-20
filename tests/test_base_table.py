import pytest

from datar import stats
from datar.base import *
from datar import f
from datar.datasets import warpbreaks, state_division, state_region, airquality

from .conftest import assert_iterable_equal

def test_table():
    # https://www.rdocumentation.org/packages/base/versions/3.6.2/topics/table
    z = stats.rpois(100, 5)
    x = table(z)
    assert sum(x.values.flatten()) == 100

    #-----------------
    with data_context(warpbreaks) as _:
        tab = table(f.wool, f.tension)

    assert tab.columns.tolist() == ['H', 'L', 'M']
    assert tab.index.tolist() == ['A', 'B']
    assert_iterable_equal(tab.values.flatten(), [9] * 6)

    tab = table(warpbreaks.loc[:, ['wool', 'tension']])
    assert tab.columns.tolist() == ['H', 'L', 'M']
    assert tab.index.tolist() == ['A', 'B']
    assert_iterable_equal(tab.values.flatten(), [9] * 6)

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
    # assert_iterable_equal(tab.values.flatten(), [10] * 4)

    tab = table(a, exclude=None)
    assert_iterable_equal(tab.values.flatten(), [10] * 5)

    #------------------
    b = as_factor(c("A","B","C") * 10)
    tab = table(b)
    assert tab.shape == (1, 3)
    assert_iterable_equal(tab.values.flatten(), [10] * 3)

    tab = table(b, exclude="B")
    assert tab.shape == (1, 2)
    assert_iterable_equal(tab.values.flatten(), [10] * 2)
    assert 'B' not in tab.columns

    #-------------------
    d = factor(rep(c("A","B","C"), 10), levels=c("A","B","C","D","E"))
    tab = table(d, exclude="B", dnn=['x'])
    assert_iterable_equal(tab.columns.to_list(), ["A", "C", "D", "E"])
    assert_iterable_equal(tab.values.flatten(), [10, 10, 0, 0])

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
    with pytest.raises(ValueError):
        table(iris.iloc[:, []])
    with pytest.raises(ValueError):
        table(iris.iloc[:, [1,2]], iris)
    with pytest.raises(ValueError):
        table(iris.iloc[:, [1]], iris, iris)
    with pytest.raises(ValueError):
        table(iris.iloc[:, [1]], iris.iloc[:, []])
