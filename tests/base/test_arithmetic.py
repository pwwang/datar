import pytest

import numpy as np
from pandas import Series
from pandas.testing import assert_frame_equal
from pandas.testing import assert_series_equal
from datar import f
from datar.base import NA

# from datar.datar import drop_index
from datar.tibble import tibble, tribble
from datar.base.arithmetic import (
    sum,
    mean,
    median,
    var,
    pmax,
    pmin,
    prod,
    abs,
    ceiling,
    col_means,
    col_medians,
    col_sds,
    col_sums,
    cov,
    exp,
    floor,
    log,
    log10,
    log1p,
    log2,
    max,
    min,
    row_means,
    row_medians,
    round,
    row_sums,
    row_sds,
    scale,
    sign,
    signif,
    sqrt,
    trunc,
)
from datar.base import pi
from ..conftest import assert_iterable_equal


def test_sum(caplog):
    assert sum(1) == 1
    assert sum([1, 2]) == 3
    assert_iterable_equal([sum([1, 2, NA], na_rm=False)], [NA])
    assert sum([1, 2, NA], na_rm=True) == 3
    Series
    assert sum(Series([1, 2])) == 3
    # Series GroupBy
    out = sum(Series([1, 2, 3, 4]).groupby([1, 1, 2, 2]))
    assert_series_equal(out, Series([3, 7], index=[1, 2]))

    caplog.clear()
    out = sum(Series([1, 2, 3, 4]).groupby([1, 1, 2, 2]), na_rm=False)
    assert "always True" in caplog.text


def test_mean():
    assert mean(1) == 1
    assert mean([1.0, 2.0]) == 1.5


def test_prod():
    assert prod(1) == 1
    assert prod([1.0, 2.0]) == 2.0


def test_median():
    assert median(1) == 1
    assert median([1.0, 20, 3.0]) == 3.0


def test_min():
    assert min(1) == 1
    assert min([1, 2, 3]) == 1


def test_max():
    assert max(1) == 1
    assert max([1, 2, 3]) == 3


def test_var():
    with pytest.warns(RuntimeWarning):
        assert_iterable_equal([var(1)], [NA])
    assert var([1, 2, 3]) == 1


def test_pmin():
    assert_iterable_equal(pmin(1, 2, 3), [1])
    assert_iterable_equal(pmin(1, [-1, 2], [0, 3]), [-1, 1])


def test_pmax():
    assert_iterable_equal(pmax(1, 2, 3), [3])
    assert_iterable_equal(pmax(1, [-1, 2], [0, 3]), [1, 3])


def test_round():
    assert round(1.23456) == 1.0
    assert_iterable_equal(round([1.23456, 3.45678]), [1.0, 3.0])
    assert_iterable_equal(round([1.23456, 3.45678], 1), [1.2, 3.5])


def test_sqrt():
    assert sqrt(1) == 1
    with pytest.warns(RuntimeWarning):
        assert_iterable_equal([sqrt(-1)], [NA])


def test_abs():
    assert abs(1) == 1
    assert_iterable_equal(abs([-1, 1]), [1, 1])


def test_ceiling():
    assert ceiling(1.1) == 2
    assert_iterable_equal(ceiling([-1.1, 1.1]), [-1, 2])


def test_floor():
    assert floor(1.1) == 1
    assert_iterable_equal(floor([-1.1, 1.1]), [-2, 1])


def test_cov():
    df = tibble(x=f[1:4], y=f[4:7])
    out = df >> cov()
    assert_frame_equal(
        out.reset_index(drop=True), tibble(x=[1.0, 1.0], y=[1.0, 1.0])
    )

    out = [1, 2, 3] >> cov([4, 5, 6])
    assert out == 1.0

    with pytest.raises(ValueError):
        cov(df, 1)

    gf = tibble(x=[1, 1, 1, 2, 2, 2], y=[1, 2, 3, 4, 5, 6]).group_by("x")
    out = cov(gf)
    assert_iterable_equal(out.y, [1, 1])
    assert_iterable_equal(out.index, [1, 2])

    out = cov(gf.y, [3, 3, 3])
    assert_iterable_equal(out, [0, 0])


def test_col_row_verbs():
    df = tribble(f.x, f.y, f.z, 1, NA, 6, 2, 4, 9, 3, 6, 15)
    assert_iterable_equal(row_medians(df), [NA, 4, 6])
    assert_iterable_equal(row_medians(df, na_rm=True), [3.5, 4, 6])
    assert_iterable_equal(col_medians(df), [2, NA, 9])
    assert_iterable_equal(col_medians(df, na_rm=True), [2, 5, 9])

    assert_iterable_equal(row_means(df), [NA, 5, 8])
    assert_iterable_equal(row_means(df, na_rm=True), [3.5, 5, 8])
    assert_iterable_equal(col_means(df), [2, NA, 10])
    assert_iterable_equal(col_means(df, na_rm=True), [2, 5, 10])

    assert_iterable_equal(row_sums(df), [NA, 15, 24])
    assert_iterable_equal(row_sums(df, na_rm=True), [7, 15, 24])
    assert_iterable_equal(col_sums(df), [6, NA, 30])
    assert_iterable_equal(col_sums(df, na_rm=True), [6, 10, 30])

    assert_iterable_equal(
        row_sds(df), [NA, 3.605551275463989, 6.244997998398398], approx=True
    )
    assert_iterable_equal(
        row_sds(df, na_rm=True),
        [3.5355339059327378, 3.605551275463989, 6.244997998398398],
        approx=True,
    )
    assert_iterable_equal(
        col_sds(df), [1.0, NA, 4.58257569495584], approx=True
    )
    assert_iterable_equal(
        col_sds(df, na_rm=True),
        [1.0, 1.4142135623730951, 4.58257569495584],
        approx=True,
    )

    # grouped
    df = tibble(x=[1, 1, 2, 2], y=[3, 4, 3, 4]).group_by('x')
    assert_iterable_equal(col_sums(df).y, [7, 7])
    assert_iterable_equal(col_means(df).y, [3.5, 3.5])
    assert_iterable_equal(col_medians(df).y, [3.5, 3.5])
    assert_iterable_equal(col_sds(df).y, [0.7071, 0.7071], approx=1e-3)


def test_scale():

    out = [1, 2, 3] >> scale()
    assert_iterable_equal(out.scaled, [-1.0, 0.0, 1.0])
    assert_iterable_equal(out.attrs["scaled:center"], [2])
    assert_iterable_equal(out.attrs["scaled:scale"], [1])

    out = scale([1, 2, 3], center=1)
    assert_iterable_equal(out.scaled, [0.0, 0.6324555, 1.2649111], approx=True)
    assert_iterable_equal(out.attrs["scaled:center"], [1])
    assert_iterable_equal(out.attrs["scaled:scale"], [1.581139], approx=True)

    out = [1, 2, 3] >> scale(scale=1)
    assert_iterable_equal(out.scaled, [-1.0, 0.0, 1.0])
    assert_iterable_equal(out.attrs["scaled:center"], [2])
    assert_iterable_equal(out.attrs["scaled:scale"], [1])

    with pytest.raises(ValueError):
        scale([1, 2, 3], center=[1, 2])
    with pytest.raises(ValueError):
        [1, 2, 3] >> scale(scale=[1, 2])

    df = tibble(x=[1, 2, 3], y=[4, 5, 6])
    assert_frame_equal(scale(df, False, False), df)

    df = tibble(x=["a", "b"])
    with pytest.raises(ValueError):
        scale(df)


def test_signif():
    x2 = pi * 100.0 ** np.array([-1, 0, 1, 2, 3])
    out = signif(x2, 3)
    assert_iterable_equal(
        out, [3.14e-02, 3.14e00, 3.14e02, 3.14e04, 3.14e06], approx=True
    )


def test_sign():
    assert sign(2) == 1
    assert sign(-2) == -1


def test_trunc():
    assert trunc(1.1) == 1


def test_log():
    assert pytest.approx(log(exp(1))) == 1.0
    assert pytest.approx(log(4, 4)) == 1.0
    assert pytest.approx(log([exp(1), exp(2)])) == [1.0, 2.0]
    assert pytest.approx(log2(2)) == 1.0
    assert pytest.approx(log10(10)) == 1.0
    assert pytest.approx(log1p(np.e - 1)) == 1.0
