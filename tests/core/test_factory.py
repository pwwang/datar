import inspect
import pytest

import numpy as np
from pandas import Categorical, DataFrame, Series
from pandas.testing import assert_frame_equal
from pandas.core.groupby import SeriesGroupBy
from datar.core.factory import func_factory
from datar.core.tibble import (
    SeriesCategorical,
    SeriesRowwise,
    TibbleGrouped,
    TibbleRowwise,
)
from datar.tibble import tibble

from ..conftest import assert_iterable_equal


def test_transform_default():
    @func_factory("transform", "x")
    def double(x):
        return x * 2

    # scalar
    out = double(3)
    assert out[0] == 6

    out = double(np.array([1, 2], dtype=int))
    assert_iterable_equal(out, [2, 4])

    @func_factory("transform", "x")
    def double(x):
        return x * 2

    out = double([1, 2])
    assert_iterable_equal(out, [2, 4])

    # default on series
    x = Series([2, 3], index=["a", "b"])
    out = double(x)
    assert isinstance(out, Series)
    assert_iterable_equal(out.index, ["a", "b"])
    assert_iterable_equal(out, [4, 6])

    # default on dataframe
    x = DataFrame({"a": [3, 4]})
    out = double(x)
    assert isinstance(out, DataFrame)
    assert_iterable_equal(out.a, [6, 8])

    # default on seriesgroupby
    x = Series([1, 2, 1, 2]).groupby([1, 1, 2, 2])
    out = double(x)
    assert isinstance(out, SeriesGroupBy)
    assert_iterable_equal(out.obj, [2, 4, 2, 4])
    assert out.grouper.ngroups == 2

    # on tibble grouped
    x = tibble(x=[1, 2, 1, 2], g=[1, 1, 2, 2]).group_by("g")
    out = double(x)
    # grouping variables not included
    assert_iterable_equal(out.x.obj, [2, 4, 2, 4])

    x = tibble(x=[1, 2, 1, 2], g=[1, 1, 2, 2]).rowwise("g")
    out = double(x)
    assert isinstance(out, TibbleRowwise)
    assert_frame_equal(out, out._datar["grouped"].obj)
    assert_iterable_equal(out.x.obj, [2, 4, 2, 4])
    assert_iterable_equal(out.group_vars, ["g"])


def test_transform_register():
    @func_factory(kind="transform", data_args="x")
    def double(x):
        return x * 2

    @double.register(DataFrame)
    def _(x):
        return x * 3

    x = Series([2, 3])
    out = double(x)
    assert_iterable_equal(out, [4, 6])

    double.register(Series, lambda x: x * 4)

    out = double(x)
    assert_iterable_equal(out, [8, 12])

    x = tibble(a=[1, 3])
    out = double(x)
    assert_iterable_equal(out.a, [3, 9])

    out = double([1, 4])
    assert_iterable_equal(out, [4, 16])

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
    @func_factory(kind="transform", data_args="x")
    def times(x, t):
        return x * t

    with pytest.raises(ValueError):
        times.register(Series, meta=False, pre=1, func=None)

    times.register(
        Series,
        func=None,
        pre=lambda x, t: (x, (-t,), {}),
        post=lambda out, x, t: out + t,
    )

    x = Series([1, 2])
    out = times(x, -1)
    assert_iterable_equal(out, [2, 3])

    @times.register(Series, meta=False)
    def _(x, t):
        return x + t

    out = times(x, 10)
    assert_iterable_equal(out, [11, 12])

    @times.register(SeriesGroupBy, meta=True)
    def _(x, t):
        return x + 10

    x = Series([1, 2, 1, 2]).groupby([1, 1, 2, 2])
    out = times(x, 1)
    assert_iterable_equal(out.obj, [11, 12, 11, 12])

    times.register(
        SeriesGroupBy,
        func=None,
        pre=lambda x, t: (x, (t + 1,), {}),
        post=lambda out, x, *args, **kwargs: out,
    )
    out = times(x, 1)
    assert_iterable_equal(out, [2, 4, 2, 4])

    times.register(
        Series,
        func=None,
        pre=lambda *args, **kwargs: None,
        post=lambda out, x, t: out + t,
    )
    x = Series([1, 2])
    out = times(x, 3)
    assert_iterable_equal(out, [4, 5])

    @times.register(DataFrame, meta=True)
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
        pre=lambda x, t: (x, (t - 1,), {}),
        post=lambda out, x, t: out.reindex([1, 0]),
    )
    x = x.group_by("a")
    out = times(x, 3)
    assert_iterable_equal(out.b, [6, 4])

    @times.register(
        TibbleGrouped,
        meta=False,
    )
    def _(x, t):
        out = x.transform(lambda d, t: d * t, 0, t - 1)
        out.iloc[0, 1] = 10
        return out

    # x = tibble(a=[1, 2], b=[2, 3])  # grouped by a
    out = times(x, 3)
    assert isinstance(out, TibbleGrouped)
    assert_iterable_equal(out.group_vars, ["a"])
    assert_iterable_equal(out.b.obj, [10, 6])


def test_agg():
    men = func_factory(
        "agg",
        "a",
        name="men",
        func=np.mean,
        signature=inspect.signature(lambda a: None),
    )

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
    df = tibble(x=[1, 2, 4]).rowwise()
    out = men(df.x)
    assert_iterable_equal(out, df.x.obj)

    men.register(SeriesRowwise, func="sum")
    out = men(df.x)
    assert_iterable_equal(out.index, [0, 1, 2])
    assert_iterable_equal(out, [1.0, 2.0, 4.0])

    # TibbleRowwise
    x = tibble(a=[1, 2, 3], b=[4, 5, 6]).rowwise()
    out = men(x)
    assert_iterable_equal(out, [2.5, 3.5, 4.5])

    # TibbleGrouped
    x = tibble(a=[1, 2, 3], b=[4, 5, 5]).group_by("b")
    out = men(x)
    assert_iterable_equal(out.a, [1.0, 2.5])


def test_varargs_data_args():
    @func_factory("agg", {"x", "args[0]"})
    def mulsum(x, *args):
        return (x + args[0]) * args[1]

    out = mulsum([1, 2], 2, 3)
    assert_iterable_equal(out, [9, 12])

    @func_factory("agg", {"x", "args"})
    def mulsum(x, *args):
        return x + args[0] + args[1]

    out = mulsum([1, 2], [1, 2], [2, 3])
    assert_iterable_equal(out, [4, 7])


def test_dataargs_not_exist():
    fun = func_factory("agg", "y")(lambda x: None)
    with pytest.raises(ValueError):
        fun(1)


def test_args_frame():
    @func_factory("agg", {"x", "y"})
    def frame(x, y, __args_frame=None):
        return __args_frame

    out = frame(1, 2)
    assert_iterable_equal(sorted(out.columns), ["x", "y"])


def test_args_raw():
    @func_factory("agg", {"x"})
    def raw(x, __args_raw=None):
        return x, __args_raw["x"]

    outx, rawx = raw(1)
    assert isinstance(outx, Series)
    assert rawx == 1


def test_apply():
    @func_factory("apply", "x")
    def rn(x):
        return tibble(x=[1, 2, 3])

    x = tibble(a=[1, 2], b=[2, 3]).rowwise()
    out = rn(x)
    assert out.shape == (2,)
    assert out.iloc[0].shape == (3, 1)


def test_no_func_registered():
    fun = func_factory("agg", "x", func=lambda x: None)
    with pytest.raises(ValueError):
        fun.register(SeriesGroupBy, func=None, meta=False)


def test_run_error():
    @func_factory("agg", "x")
    def error(x):
        raise RuntimeError

    with pytest.raises(ValueError, match="registered function"):
        error(1)


def test_series_cat():
    @func_factory("agg", "x")
    def sum1(x):
        return x.sum()

    @sum1.register(SeriesCategorical)
    def _(x):
        return x[0]

    out = sum1([1, 2])
    assert out == 3

    out = sum1(Categorical([1, 2]))
    assert out == 1


def test_str_fun():
    sum2 = func_factory(
        "agg",
        "x",
        name="sum2",
        qualname="sum2",
        func="sum",
        signature=inspect.signature(lambda x: None),
    )
    assert sum2([1, 2, 3]) == 6
