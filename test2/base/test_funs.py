import pytest

from pandas import Interval, DataFrame
from pandas.testing import assert_frame_equal
from datar2.base.funs import *
from datar2.base import table, pi, paste0
from datar2.stats import rnorm
from ..conftest import assert_iterable_equal

def test_cut():
    z = rnorm(10000)
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
    assert_iterable_equal(z.to_numpy(), [Interval(0.9995, 1.0, closed='right')] * 5)
    assert_iterable_equal(z.categories.to_list(), [
        Interval(0.999,  0.9995, closed='right'),
        Interval(0.9995, 1.0, closed='right'),
        Interval(1.0,    1.0005, closed='right'),
        Interval(1.0005, 1.001, closed='right'),
    ])

    z = rnorm(100)
    tab = table(cut(z, breaks=[pi/3.0*i for i in range(0,4)]))
    assert str(tab.columns.tolist()[0]) == '(0.0, 1.05]'

    tab = table(cut(z, breaks=[pi/3.0*i for i in range(0,4)], precision=3))
    assert str(tab.columns.tolist()[0]) == '(0.0, 1.047]'

    aaa = [1,2,3,4,5,2,3,4,5,6,7]
    ct = cut(aaa, 3, precision=3, ordered_result=True)
    assert str(ct[0]) == '(0.994, 3.0]'

def test_identity():
    assert identity(1) == 1
    assert identity(1.23) == 1.23

def test_expandgrid():
    df = expandgrid([1,2], [3,4])
    assert_frame_equal(df, DataFrame({
        '[1, 2]': [1,1,2,2],
        '[3, 4]': [3,4,3,4],
    }))

def test_data_context():
    from datar import f
    df = DataFrame(dict(a=[1,2], b=[3,4]))
    with data_context(df) as _:
        out = expandgrid(f.a, f.b)

    assert_frame_equal(out, DataFrame({
        'a': [1,1,2,2],
        'b': [3,4,3,4],
    }))

def test_outer():
    out = outer([1,2], [1,2,3])
    assert_frame_equal(out, DataFrame([
        [1,2,3],
        [2,4,6]
    ]))

    out = outer([1,2], [1,2,3], fun=paste0)
    assert_frame_equal(out, DataFrame([
        ["11", "12", "13"],
        ["21", "22", "23"]
    ]))

def test_make_names():
    names = ["a", "1aA b", "_1aA b"]
    assert_iterable_equal(
        make_names(names),
        ['a', '_1aA_b', '_1aA_b']
    )
    assert_iterable_equal(
        make_names(names, unique=True),
        ['a', '_1aA_b__1', '_1aA_b__2']
    )
    assert_iterable_equal(
        make_unique(names),
        ['a', '_1aA_b__1', '_1aA_b__2']
    )
    assert make_names(1) == ['_1']
