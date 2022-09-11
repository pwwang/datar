import inspect
import pytest

import numpy as np
from datar import f
from datar.core.backends.pandas import Categorical, DataFrame, Series
from datar.core.backends.pandas.testing import assert_frame_equal
from datar.core.backends.pandas.core.groupby import SeriesGroupBy
from datar.core.factory import func_factory, is_categorical_dtype
from datar.core.tibble import (
    SeriesCategorical,
    SeriesRowwise,
    Tibble,
    TibbleGrouped,
    TibbleRowwise,
)

from ..conftest import assert_iterable_equal


def test_fun_meta():
    @func_factory(kind="transform")
    def fun(x):
        """My docstring"""

    assert fun.__name__ == "fun"
    assert fun.__qualname__ == "test_fun_meta.<locals>.fun"
    assert fun.__module__ == "tests.core.test_factory"
    assert fun.__doc__ == "My docstring"
    assert fun.signature == inspect.signature(lambda x: None)


def test_given_meta():
    sig = inspect.signature(lambda y: None)
    @func_factory(
        kind="transform",
        name="func1",
        doc="My docstring1",
        qualname="qfunc1",
        module="mymod",
        signature=sig,
    )
    def fun(x):
        """My docstring"""

    assert fun.__name__ == "func1"
    assert fun.__qualname__ == "qfunc1"
    assert fun.__module__ == "mymod"
    assert fun.__doc__ == "My docstring1"
    assert fun.signature == sig


def test_transform_default():
    @func_factory(kind="transform")
    def double(x):
        return x * 2

    # scalar
    out = double(3)
    assert out[0] == 6

    # list
    out = double([1, 2])
    assert isinstance(out, np.ndarray)
    assert_iterable_equal(out, [2, 4])

    # ndarray
    out = double(np.array([1, 2], dtype=int))
    assert isinstance(out, np.ndarray)
    assert_iterable_equal(out, [2, 4])

    # series
    x = Series([2, 3], index=["a", "b"])
    out = double(x)
    assert isinstance(out, Series)
    assert_iterable_equal(out.index, ["a", "b"])
    assert_iterable_equal(out, [4, 6])

    # seriesgroupby
    x = x.groupby([1, 2])
    out = double(x)
    assert isinstance(out, SeriesGroupBy)
    assert_iterable_equal(out.obj, [4, 6])

    # seriesrowwise
    x.is_rowwise = True
    out = double(x)
    assert isinstance(out, SeriesGroupBy)
    assert out.is_rowwise
    assert_iterable_equal(out.obj, [4, 6])

    # dataframe/tibble
    x = Tibble.from_args(x=[1, 2], y=[3, 4])
    out = double(x)
    assert_frame_equal(out, Tibble.from_args(x=[2, 4], y=[6, 8]))

    # tibblegrouped
    y = x.group_by("x")
    out = double(y)
    assert isinstance(out, TibbleGrouped)
    assert_frame_equal(out, Tibble.from_args(x=[2, 4], y=[6, 8]))

    # tibblerowwise
    y = x.rowwise("x")
    out = double(y)
    assert isinstance(out, TibbleRowwise)
    assert_frame_equal(out, Tibble.from_args(x=[2, 4], y=[6, 8]))


def test_transform_register():
    # register for different types

    @func_factory(kind="transform")
    def times(x, n):
        return x * n

    out = times([1, 2], 2)
    # Series not kept
    assert not isinstance(out, Series)
    assert_iterable_equal(out, [2, 4])

    times.register(np.ndarray, func="default", keep_series=True)
    # Series kept
    out = times(np.array([1, 2]), 2)
    assert isinstance(out, Series)
    assert_iterable_equal(out, [2, 4])

    # Series
    x = Series(Categorical([1, 2]))
    out = times(x, 3)
    assert is_categorical_dtype(out)
    assert_iterable_equal(out, [3, 6])

    # SeriesCategorical
    @times.register(SeriesCategorical)
    def _(x, n):
        return x * (n-1)
    out = times(x, 3)
    assert is_categorical_dtype(out)
    assert_iterable_equal(out, [2, 4])

    @times.register(
        SeriesGroupBy,
        pre=lambda x, *args, **kwargs: (
            x.transform(lambda y: y+1).groupby([1, 2]), args, kwargs
        )
    )
    def _(x, n):
        return x * (n + 1)

    x = Series([1, 2]).groupby([1, 2])
    out = times(x, 2)
    assert isinstance(out, SeriesGroupBy)
    assert_iterable_equal(out.obj, [6, 9])

    x.is_rowwise = True
    out = times(x, 2)
    assert isinstance(out, SeriesGroupBy)
    assert out.is_rowwise
    assert_iterable_equal(out.obj, [2, 4])

    @times.register(
        TibbleGrouped,
        post=lambda out, x, *args, **kwargs: out.sum().sum()
    )
    def _(x, n):
        return x + n

    x = Tibble.from_args(a=[1, 2], b=[3, 4]).group_by("b")
    out = times(x, 1)
    assert out == 14


def test_agg():
    men = func_factory(
        kind="agg",
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
    df = Tibble.from_args(x=[1, 2, 4]).rowwise()
    out = men(df.x)
    assert_iterable_equal(out, df.x.obj)

    men.register(SeriesRowwise, func="sum")
    out = men(df.x)
    assert_iterable_equal(out.index, [0, 1, 2])
    assert_iterable_equal(out, [1.0, 2.0, 4.0])

    # TibbleRowwise
    x = Tibble.from_args(a=[1, 2, 3], b=[4, 5, 6]).rowwise()
    out = men(x)
    assert_iterable_equal(out, [2.5, 3.5, 4.5])

    # TibbleGrouped
    x = Tibble.from_args(a=[1, 2, 3], b=[4, 5, 5]).group_by("b")
    out = men(x)
    assert_iterable_equal(out.a, [1.0, 2.5])


def test_apply():
    @func_factory(kind="apply")
    def rn(x, n):
        return x.sum() * n

    x = Series([np.array([1, 2]), np.array([2, 3])])  # .sum() works
    out = rn(x, 2)
    assert_iterable_equal(out, [6, 10])

    x = Series([np.array([1, 1]), np.array([1, 2]), np.array([2, 3])]).groupby(
        [1, 2, 1]
    )
    out = rn(x, 2)
    assert_iterable_equal(out.iloc[0], [6, 8])
    assert_iterable_equal(out.iloc[1], [2, 4])

    x = Tibble.from_args(a=[1, 2], b=[1, 3])
    out = rn(x, 1)
    assert_iterable_equal(out, [3, 4])

    x = Tibble.from_args(a=[1, 2], b=[1, 3]).rowwise()
    out = rn(x, 1)
    assert_iterable_equal(out, [2, 5])

    @rn.register(TibbleGrouped)
    def rn(x, n):
        return x.a.sum() * n

    x = Tibble.from_args(
        a=[1, 2, 3, 4],
        b=[1, 2, 1, 2],
    ).group_by("b")
    out = rn(x, 1)
    assert_iterable_equal(out, [4, 6])


def test_apply_df():
    @func_factory({"x", "w"}, "apply_df")
    def weighted_mean(x, w):
        return np.average(x, weights=w)

    df = Tibble.from_args(
        x=[7, 2, 2, 4],
        w=[2, 3, 1, 1],
        g=[1, 1, 2, 2],
    )

    out = weighted_mean(df.x, df.w)
    assert_iterable_equal([out], [26/7], approx=True)

    gf = df.group_by("g")
    out = weighted_mean(gf.x, gf.w)
    assert_iterable_equal(out, [4, 3])

    rf = df.rowwise()
    out = weighted_mean(rf.x, rf.w)
    assert_iterable_equal(out, [7, 2, 2, 4])

    @func_factory({'x', 'w', 'g'})
    def listify(x, w, g):
        return [x, w, g]

    out = listify(rf.x, rf.w, rf.g)
    assert isinstance(out, Series)
    assert len(out) == 4
    assert out[0] == [7, 2, 1]
    assert out[1] == [2, 3, 1]
    assert out[2] == [2, 1, 2]
    assert out[3] == [4, 1, 2]


def test_func_factory_default_kind_apply_df():
    @func_factory({'x', 'n'})
    def div(x, n):
        return x / n.sum()

    df = Tibble.from_args(
        x=[7, 2, 2, 4],
        w=[2, 3, 1, 1],
        g=[1, 1, 2, 2],
    ).group_by("g")
    out = div(df.x, df.w)
    assert_iterable_equal(out, [1.4, 0.4, 1.0, 2.0])


def test_work_with_exprs():
    @func_factory(kind="transform")
    def add(x, n):
        return x + n

    df = Tibble.from_args(x=[1, 2], y=[4, 5])
    out = add(f.x, f.y.sum())._pipda_eval(df)
    assert_iterable_equal(out, [10, 11])


def test_dataargs_not_exist():
    fun = func_factory("y", "agg")(lambda x: None)
    with pytest.raises(ValueError):
        fun(1)


def test_args_frame():
    @func_factory({"x", "y"}, "agg")
    def frame(x, y, __args_frame=None):
        return __args_frame

    out = frame(1, 2)
    assert_iterable_equal(sorted(out[0].columns), ["x", "y"])


def test_args_raw():
    @func_factory(kind="agg")
    def raw(x, __args_raw=None):
        return x, __args_raw["x"]

    assert raw(1)[0] == (1, 1)


def test_run_error():
    @func_factory(kind="agg")
    def error(x):
        raise RuntimeError

    with pytest.raises(RuntimeError):
        error(1)


def test_series_cat():
    @func_factory(kind="agg")
    def sum1(x):
        return x.sum()

    @sum1.register(SeriesCategorical)
    def _(x):
        return x[0]

    out = sum1([1, 2])
    assert out == 3

    out = sum1(Categorical([1, 2]))
    assert out == 1


def test_nested_frames():
    df = Tibble.from_args(
        x=[1, 2, 3, 4],
        y=[5, 6, 7, 8],
    )
    ndf = Tibble.from_args(
        z=df,
        w=[
            np.array([1, 2]),
            np.array([3, 4]),
            np.array([5, 6]),
            np.array([7, 8]),
        ],
        g=[1, 2, 1, 2],
    )
    gdf = ndf.group_by("g")
    rdf = ndf.rowwise()

    @func_factory({"z", "w"})
    def ndimx(z, w):
        return z.ndim + w.sum()

    out = ndimx(ndf['z'], ndf['w'])
    assert_iterable_equal(out, [18, 22])

    out = ndimx(gdf['z'], gdf['w'])
    assert_iterable_equal(out.iloc[0], [8, 10])
    assert_iterable_equal(out.iloc[1], [12, 14])

    out = ndimx(rdf['z'], rdf['w'])
    assert_iterable_equal(out, [4, 8, 12, 16])

    @ndimx.register(TibbleRowwise)
    def _(z, w):
        return z.sum() + w.sum()

    out = ndimx(rdf['z'], rdf['w'])
    assert_iterable_equal(out, [9, 15, 21, 27])


def test_apply_df_can_only_register_dataframe():
    @func_factory({"x", "y"})
    def fun(x, y):
        ...

    with pytest.raises(TypeError):
        fun.register(np.ndarray, func=lambda x, y: ...)


def test_apply_df_meta():
    @func_factory({"x", "w"})
    def weighted_mean(x, w):
        return np.average(x, weights=w)

    weighted_mean.register(
        DataFrame,
        func="default",
        pre=lambda **kwargs: {"w": kwargs["w"] + 1},
        post=lambda out, **kwargs: out + 1,
    )

    df = Tibble.from_args(
        x=[7, 2, 2, 4],
        w=[1, 2, 0, 0],
    )
    out = weighted_mean(df.x, df.w)
    assert_iterable_equal([out], [33/7], approx=True)


def test_varargs():
    @func_factory()
    def pmin(*x, __args_frame=None):
        return __args_frame.agg('min', axis=1)

    df = Tibble.from_args(x=[7, 2], w=[8, 0])
    out = pmin(df.x, df.w)
    assert_iterable_equal(out, [7, 0])
