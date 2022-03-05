import pytest

import numpy as np
from datar.base import NA, weighted_mean, quantile, sd, rnorm, rpois, runif
from datar.tibble import tibble

from ..conftest import assert_iterable_equal


def test_weighted_mean():
    assert pytest.approx(weighted_mean([1,2,3], [1,2,3])) == 2.3333333
    assert weighted_mean(1, 1) == 1
    assert weighted_mean([1, NA], [1, NA], na_rm=True) == 1
    assert np.isnan(weighted_mean([1, NA], [1, NA], na_rm=False))
    assert_iterable_equal([weighted_mean([1,2], [-1, 1])], [NA])
    with pytest.raises(ValueError):
        weighted_mean([1,2], [1,2,3])


def test_quantile():
    df = tibble(x=[1, 2, 3], g=[1, 2, 2])
    out = quantile(df.x, probs=0.5)
    assert_iterable_equal([out], [2])

    out = quantile(df.x, probs=[0.5, 1])
    assert_iterable_equal(out, [2, 3])

    gf = df.group_by('g')
    out = quantile(gf.x, probs=0.5)
    assert_iterable_equal(out.obj, [1, 2.5, 2.5])


def test_sd():
    df = tibble(x=[1, 2, 3], g=[1, 2, 2])
    out = sd(df.x)
    assert out == 1

    gf = df.group_by('g')
    out = sd(gf.x)
    assert_iterable_equal(out, [np.nan, 0.7071067811865476], approx=True)


def test_rnorm():
    assert_iterable_equal(rnorm(3, [1, 2], [0, 0]), [1, 2, 1])
    assert_iterable_equal(rnorm(1, [1, 2], [0, 0]), [1])

    df = tibble(x=[1, 2, 3], y=[1, 1, 2])
    assert_iterable_equal(rnorm(df.x, df.y, 0), [1, 1, 2])

    gf = df.group_by("y")
    out = rnorm(gf.x, 1, 0)
    assert_iterable_equal(out.values[0], [1, 1])
    assert_iterable_equal(out.values[1], [1])

    rf = df.rowwise()
    out = rnorm(rf.x, rf.y, 0)
    assert len(out) == 3


def test_runif():
    assert len(runif(3, [1, 2], [0, 0])) == 3
    assert len(runif(1, [1, 2], [0, 0])) == 1

    df = tibble(x=[1, 2, 3], y=[1, 1, 2])
    assert len(runif(df.x, df.y, 0)) == 3

    gf = df.group_by("y")
    out = runif(gf.x, 1, 0)
    assert len(out) == 2

    rf = df.rowwise()
    out = runif(rf.x, rf.y, 0)
    assert len(out) == 3


def test_rpois():
    assert len(rpois(3, [1, 2], [0, 0])) == 3
    assert len(rpois(1, [1, 2], [0, 0])) == 1

    df = tibble(x=[1, 2, 3], y=[1, 1, 2])
    assert len(rpois(df.x, df.y)) == 3

    gf = df.group_by("y")
    out = rpois(gf.x, 1)
    assert len(out) == 2

    rf = df.rowwise()
    out = rpois(rf.x, rf.y)
    assert len(out) == 3