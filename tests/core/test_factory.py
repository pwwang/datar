import pytest

import numpy as np
from pandas import DataFrame, Series
from pandas.testing import assert_frame_equal
from pandas.core.generic import NDFrame
from pandas.core.groupby import SeriesGroupBy
from datar2.core.factory import func_factory
from datar2.core.tibble import SeriesRowwise, TibbleGrouped, TibbleRowwise
from datar2.tibble import tibble

from ..conftest import assert_iterable_equal


def test_transform_default():
    @func_factory(kind="transform")
    def double(x):
        return x * 2

    # scalar
    out = double(3)
    assert out == 6

    out = double(np.array([1, 2], dtype=int))
    assert_iterable_equal(out, [2, 4])

    # not vectorized
    out = double([1, 2])
    assert out == [1, 2, 1, 2]

    @func_factory(kind="transform", is_vectorized=False)
    def double(x):
        return x * 2

    out = double([1, 2])
    assert_iterable_equal(out, [2, 4])

    # default on series
    x = Series([2, 3], index=['a', 'b'])
    out = double(x)
    assert isinstance(out, Series)
    assert_iterable_equal(out.index, ['a', 'b'])
    assert_iterable_equal(out, [4, 6])

    # default on dataframe
    x = DataFrame({'a': [3, 4]})
    out = double(x)
    assert isinstance(x, DataFrame)
    assert_iterable_equal(out.a, [6, 8])

    # default on seriesgroupby
    x = Series([1, 2, 1, 2]).groupby([1, 1, 2, 2])
    out = double(x)
    assert isinstance(out, SeriesGroupBy)
    assert_iterable_equal(out.obj, [2, 4, 2, 4])
    assert out.grouper.ngroups == 2

    # on tibble grouped
    x = tibble(x=[1, 2, 1, 2], g=[1, 1, 2, 2]).group_by('g')
    out = double(x)
    assert isinstance(out, TibbleGrouped)
    assert_frame_equal(out, out._datar["grouped"].obj)
    assert_iterable_equal(out.x.obj, [2, 4, 2, 4])

    x = tibble(x=[1, 2, 1, 2], g=[1, 1, 2, 2]).rowwise('g')
    out = double(x)
    assert isinstance(out, TibbleRowwise)
    assert_frame_equal(out, out._datar["grouped"].obj)
    assert_iterable_equal(out.x.obj, [2, 4, 2, 4])
    assert_iterable_equal(out.group_vars, ['g'])


def test_transform_register():
    @func_factory(kind="transform", is_vectorized=False)
    def double(x):
        return x * 2

    @double.register(NDFrame)
    def _(x):
        return x * 3

    x = Series([2, 3])
    out = double(x)
    assert_iterable_equal(out, [6, 9])

    double.register(Series, lambda x: x * 4)
    out = double(x)
    assert_iterable_equal(out, [8, 12])

    x = tibble(a=[1, 3])
    out = double(x)
    assert_iterable_equal(out.a, [3, 9])

    out = double([1, 4])
    assert_iterable_equal(out, [2, 8])

    # register an available string func for tranform
    double.register(SeriesGroupBy, "sum")
    x = Series([1, -2]).groupby([1, 2])
    out = double(x)
    assert_iterable_equal(out.obj, [1, -2])

    # seriesrowwise
    double.register(SeriesRowwise, lambda x: x + 1)
    x.is_rowwise = True
    out = double(x)
    assert_iterable_equal(out.obj, [2, -1])
    assert out.is_rowwise


def test_transform_hooks():
    @func_factory(kind="transform")
    def times(x, t):
        return x * t

    with pytest.raises(ValueError):
        times.register(Series, replace=True, pre=1, func=None)

    times.register(
        Series,
        func=None,
        pre=lambda x, t: (x, (-t, ), {}),
        post=lambda out, x, t: out + t
    )

    x = Series([1, 2])
    out = times(x, -1)
    assert_iterable_equal(out, [2, 3])

    @times.register(Series, replace=True)
    def _(x, t):
        return x + t

    out = times(x, 10)
    assert_iterable_equal(out, [11, 12])

    @times.register(SeriesGroupBy, replace=True)
    def _(x, t):
        return x.transform("sum")

    x = Series([1, 2, 1, 2]).groupby([1, 1, 2, 2])
    out = times(x, 1)
    assert_iterable_equal(out, [3, 3, 3, 3])

    times.register(
        SeriesGroupBy,
        func=None,
        pre=lambda x, t: (x, (t + 1, ), {}),
        post=lambda out, x, *args, **kwargs: out
    )
    out = times(x, 1)
    assert_iterable_equal(out, [2, 4, 2, 4])

    times.register(
        Series,
        func=None,
        pre=lambda *args, **kwargs: None,
        post=lambda out, x, t: out + t
    )
    x = Series([1, 2])
    out = times(x, 3)
    assert_iterable_equal(out, [6, 9])

    @times.register(DataFrame, replace=True)
    def _(x, t):
        return x ** t

    x = tibble(a=[1, 2], b=[2, 3])
    out = times(x, 3)
    assert_iterable_equal(out.a, [1, 8])
    assert_iterable_equal(out.b, [8, 27])

    # TibbleGrouped
    times.register(
        TibbleGrouped,
        func=None,
        pre=lambda x, t: (x, (t - 1, ), {}),
        post=lambda out, x, t: out.reindex([1, 0])
    )
    x = x.group_by('a')
    out = times(x, 3)
    assert isinstance(out, TibbleGrouped)
    assert_iterable_equal(out.group_vars, ['a'])
    assert_iterable_equal(out.b.obj, [6, 4])

    @times.register(
        TibbleGrouped,
        replace=True,
    )
    def _(x, t):
        out = x.transform(lambda d, t: d * t, 0, t - 1)
        out.iloc[0, 1] = 10
        return out

    # x = tibble(a=[1, 2], b=[2, 3])  # grouped by a
    out = times(x, 3)
    assert isinstance(out, TibbleGrouped)
    assert_iterable_equal(out.group_vars, ['a'])
    assert_iterable_equal(out.b.obj, [10, 6])


def test_agg():
    men = func_factory("agg", name="men", func=np.mean)

    x = [1, 2, 3]
    out = men(x)
    assert out == 2.0

    x = Series([1, 2, 3])
    out = men(x)
    assert out == 2.0

    # SeriesGroupBy
    men.register(SeriesGroupBy, func="mean")
    x = Series([1, 2, 4]).groupby([1, 2, 2])
    out = men(x)
    assert_iterable_equal(out.index, [1, 2])
    assert_iterable_equal(out, [1.0, 3.0])

    # SeriesRowwise
    x.is_rowwise = True
    out = men(x)
    assert_iterable_equal(out.obj, x.obj)

    men.register(SeriesRowwise, func="sum")
    out = men(x)
    assert_iterable_equal(out.index, [1, 2])
    assert_iterable_equal(out, [1.0, 6.0])

    # TibbleRowwise
    x = tibble(a=[1, 2, 3], b=[4, 5, 6]).rowwise()
    out = men(x)
    assert_iterable_equal(out, [2.5, 3.5, 4.5])

    # TibbleGrouped
    x = tibble(a=[1, 2, 3], b=[4, 5, 5]).group_by('b')
    out = men(x)
    assert_iterable_equal(out.a, [1.0, 2.5])
