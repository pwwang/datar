import pytest
import warnings
import datetime

import numpy
from pandas import Interval, Categorical, DataFrame
from datar import stats
from datar.base import *

from .conftest import assert_equal

@pytest.fixture(autouse=True)
def supress_warnings():
    """Ignore pipda not being able to get node to detect piping"""
    warnings.simplefilter("ignore")
    yield
    warnings.simplefilter("default")

@pytest.mark.parametrize('x,expected', [
    ([1,2,3], ['1', '2', '3']),
    (numpy.array([1,2,3]), ['1','2','3']),
    (1, '1'),
])
def test_as_character(x, expected):
    assert_equal(as_character(x), expected)

@pytest.mark.parametrize('x,format,try_formats,optional,tz,origin,expected', [
    (["1jan1960", "2jan1960", "31mar1960", "30jul1960"],
     "%d%b%Y", None, False, 0, None,
     [datetime.date(1960, 1, 1),
      datetime.date(1960, 1, 2),
      datetime.date(1960, 3, 31),
      datetime.date(1960, 7, 30)]),
    (["02/27/92", "02/27/92", "01/14/92", "02/28/92", "02/01/92"],
     "%m/%d/%y", None, False, 0, None,
     [datetime.date(1992, 2, 27),
      datetime.date(1992, 2, 27),
      datetime.date(1992, 1, 14),
      datetime.date(1992, 2, 28),
      datetime.date(1992, 2, 1)]),
    (32768,
     None, None, False, 0, "1900-01-01",
     [datetime.date(1989, 9, 19)]),
    (35981,
     None, None, False, 0, "1899-12-30",
     [datetime.date(1998, 7, 5)]),
    (34519,
     None, None, False, 0, "1904-01-01",
     [datetime.date(1998, 7, 5)]),
    (734373 - 719529,
     None, None, False, 0, datetime.datetime(1970, 1, 1),
     [datetime.date(2010, 8, 23)]),
    ([datetime.date(2010, 4, 13)],
     None, None, False, 12, None,
     [datetime.date(2010, 4, 13)]),
    ([datetime.date(2010, 4, 13)],
     None, None, False, 12 + 13, None,
     [datetime.date(2010, 4, 14)]),
    ([datetime.datetime(2010, 4, 13)],
     None, None, False, 0 - 10, None,
     [datetime.date(2010, 4, 12)]),
])
def test_as_date(x, format, try_formats, optional, tz, origin, expected):
    assert_equal(as_date(
        x,
        format,
        try_formats,
        optional,
        tz,
        origin
    ), expected)

def test_as_date_error():
    with pytest.raises(ValueError):
        as_date(1.1)

    with pytest.raises(ValueError):
        as_date("1990-1-1", "%Y")

    assert as_date("1990-1-1", "Y", optional=True).isna().all()

@pytest.mark.parametrize('x,expected', [
    ([1,2,3], [1.0, 2.0, 3.0]),
    (numpy.array(['1','2','3']), [1.0, 2.0, 3.0])
])
def test_as_double(x, expected):
    assert_equal(as_double(x), expected)

@pytest.mark.parametrize('x,expected', [
    ([1,2,3], Categorical([1,2,3])),
    (numpy.array(['1','2','3']), Categorical(['1', '2', '3']))
])
def test_as_categorical(x, expected):
    assert_equal(as_categorical(x), expected)

@pytest.mark.parametrize('x,expected', [
    ([1.0,2.0,3.0], [1,2,3]),
    (numpy.array(['1','2','3']), [1,2,3]),
    (factor(['1','2','3']), [0,1,2]),
])
def test_as_int(x, expected):
    assert_equal(as_int(x), expected)

@pytest.mark.parametrize('x,expected', [
    ([1.0,2.0,3.0], [1,2,3]),
    (numpy.array(['1','2','3']), [1,2,3])
])
def test_as_integer(x, expected):
    assert_equal(as_integer(x), expected)

@pytest.mark.parametrize('x,expected', [
    ([1.0,0.0], [True, False]),
    (numpy.array(['1','0']), [True, False])
])
def test_as_logical(x, expected):
    assert_equal(as_logical(x), expected)

def test_factor():
    ff = factor(list("statistics"), levels=letters)
    assert_equal(ff.to_numpy().flatten(), list("statistics"))
    assert_equal(ff.categories.tolist(), letters)

    ffi = as_integer(ff)
    assert_equal(ffi, [18,19,0,19,8,18,19,8,2,18])

    ff = factor(ff)
    assert_equal(ff.categories.tolist(), list('acist'))

    z = factor(LETTERS[-3:], ordered=TRUE)
    assert "['X' < 'Y' < 'Z']" in str(z)

    x = factor(c(1, 2, NA), exclude=None)
    assert len(x) == 3
    # different from R
    assert_equal(x.categories.tolist(), [1, 2])

    x = factor(z, exclude = "X")
    assert x[0] is NA
    assert len(x.categories) == 2

@pytest.mark.parametrize('x,expected', [
    (1.4, 2),
    ([1.1,2.1], [2, 3])
])
def test_ceiling(x, expected):
    assert_equal(ceiling(x), expected)

@pytest.mark.parametrize('x,expected', [
    (1.4, 1),
    ([1.1,2.1], [1, 2])
])
def test_floor(x, expected):
    assert_equal(floor(x), expected)

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

def test_cumx():
    #                                  1,2,3,4,5
    assert_equal(cumsum(range(1,6)),  [1,3,6,10,15])
    assert_equal(cumprod(range(1,6)), [1,2,6,24,120])
    assert_equal(cummin([3,2,1]), [3,2,1])
    assert_equal(cummax([3,2,1]), [3,3,3])

def test_cut():
    z = stats.rnorm(10000)
    tab = table(cut(z, breaks=range(-6, 7)))
    assert tab.shape == (1, 12)
    assert tab.columns.tolist() == [
        Interval(-6, -5, closed='right'),
        Interval(-5, -4, closed='right'),
        Interval(-4, -3, closed='right'),
        Interval(-3, -2, closed='right'),
        Interval(-2, -1, closed='right'),
        Interval(-1, 0, closed='right'),
        Interval(0, 1, closed='right'),
        Interval(1, 2, closed='right'),
        Interval(2, 3, closed='right'),
        Interval(3, 4, closed='right'),
        Interval(4, 5, closed='right'),
        Interval(5, 6, closed='right'),
    ]
    assert sum(tab.values.flatten()) == 10000

    z = cut([1] * 5, 4)
    assert_equal(z.to_numpy(), [Interval(0.9995, 1.0, closed='right')] * 5)
    assert_equal(z.categories.to_list(), [
        Interval(0.999,  0.9995, closed='right'),
        Interval(0.9995, 1.0, closed='right'),
        Interval(1.0,    1.0005, closed='right'),
        Interval(1.0005, 1.001, closed='right'),
    ])

    z = stats.rnorm(100)
    tab = table(cut(z, breaks=[pi/3.0*i for i in range(0,4)]))
    assert str(tab.columns.tolist()[0]) == '(0.0, 1.05]'

    tab = table(cut(z, breaks=[pi/3.0*i for i in range(0,4)], precision=3))
    assert str(tab.columns.tolist()[0]) == '(0.0, 1.047]'

    aaa = c(1,2,3,4,5,2,3,4,5,6,7)
    ct = cut(aaa, 3, precision=3, ordered_result=True)
    assert str(ct[0]) == '(0.994, 3.0]'

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
    assert_equal(diag(x).values.flatten(), [2,1,2,1])

def test_sample():
    x = sample(range(1, 13))
    assert set(x) == set(range(1,13))

    y = sample(x, replace=TRUE)
    assert len(y) <= 12

    z = sample(c(0,1), 100, replace=TRUE)
    assert set(z) == {0, 1}
    assert len(z) == 100

    w = sample('abc', 100, replace=TRUE)
    assert set(w) == {'a', 'b', 'c'}
    assert len(z) == 100

def test_table():
    # https://www.rdocumentation.org/packages/base/versions/3.6.2/topics/table
    from datar import f
    from datar.datasets import warpbreaks, state_division, state_region, airquality
    z = stats.rpois(100, 5)
    # x = table(z)
    # assert sum(x.values.flatten()) == 100

    #-----------------
    with context(warpbreaks) as _:
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
    with context(airquality) as _:
        tab = table(cut(f.Temp, stats.quantile(f.Temp)), f.Month)

    assert tab.iloc[0,0] == 24

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
    tab = table(a)
    assert_equal(tab.values.flatten(), [10] * 4)

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

    with context(airquality) as _:
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

@pytest.mark.parametrize('x, times, length, each, expected', [
    (range(4), 2, None, 1, [0,1,2,3]*2),
    (range(4), 1, None, 2, [0,0,1,1,2,2,3,3]),
    (range(4), [2]*4, None, 1, [0,0,1,1,2,2,3,3]),
    (range(4), [2,1]*2, None, 1, [0,0,1,2,2,3]),
    (range(4), 1, 4, 2, [0,0,1,1]),
    (range(4), 1, 10, 2, [0,0,1,1,2,2,3,3,0,0]),
    (range(4), 3, None, 2, [0,0,1,1,2,2,3,3] * 3),
    (1, 7, None, 1, [1,1,1,1,1,1,1]),
])
def test_rep(x, times, length, each, expected):
    assert rep(x, times=times, length=length, each=each) == expected

def test_rep_error():
    with pytest.raises(ValueError):
        rep(c(1,2,3), c(1,2))
    with pytest.raises(ValueError):
        rep(c(1,2,3), c(1,2,3), each=2)

def test_droplevels():
    x = factor(c(1,2,3), levels=c(1,2,3,4))
    x = droplevels(x)
    assert len(x.categories) == 3
