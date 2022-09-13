# tests grabbed from:
# https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-rank.r
import numpy as np
import pytest

from datar import f
from datar.tibble import tibble
from datar.base import c, NA, nrow, rep
from datar.dplyr import (
    mutate,
    row_number,
    ntile,
    min_rank,
    dense_rank,
    percent_rank,
    cume_dist,
    count,
    pull,
    lead,
    lag,
)
from ..conftest import assert_iterable_equal


def ntile_h(x, n):
    out = tibble(x=x) >> mutate(y=ntile(x, n=n))
    return out.y.tolist()


def test_ntile_ignores_number_of_nas():
    x = c(1, 2, 3, NA, NA)
    out = ntile(x, n=3)
    assert_iterable_equal(out, [1, 2, 3, NA, NA])

    out = ntile_h(x, 3)
    assert_iterable_equal(out, [1, 2, 3, NA, NA])

    x1 = c(1, 1, 1, NA, NA, NA)
    out = ntile(x1, n=1)
    assert_iterable_equal(out, [1, 1, 1, NA, NA, NA])
    out = ntile_h(x1, 1)
    assert_iterable_equal(out, [1, 1, 1, NA, NA, NA])


def test_ntile_always_returns_an_integer():
    out = ntile([], n=3)
    assert_iterable_equal(out, [])
    out = ntile_h([], n=3)
    assert_iterable_equal(out, [])
    out = ntile([NA], n=3)
    assert_iterable_equal(out, [NA])
    out = ntile_h([NA], n=3)
    assert_iterable_equal(out, [NA])


def test_ntile_does_not_overflow():
    m = int(1e2)
    res = tibble(a=range(1,m+1)) >> mutate(
        b=ntile(f.a, n=m)
    ) >> count(f.b) >> pull(to='list')
    assert sum(res) == 100


def test_row_number_handles_empty_dfs():
    df = tibble(a=[])
    res = df >> mutate(
        row_number_0=row_number(),
        # row_number_a=row_number(f.a), # row_number doesn't support extra arg
        ntile=ntile(f.a, n=2),
        min_rank=min_rank(f.a),
        percent_rank=percent_rank(f.a),
        dense_rank=dense_rank(f.a),
        cume_dist=cume_dist(f.a),
    )
    assert_iterable_equal(
        res.columns,
        [
            "a",
            "row_number_0",
            "ntile",
            "min_rank",
            "percent_rank",
            "dense_rank",
            "cume_dist",
        ],
    )
    assert nrow(res) == 0


def test_lead_lag_inside_mutates_handles_expressions_as_value_for_default():
    df = tibble(x=[1,2,3])
    res = mutate(
        df,
        leadn=lead(f.x, default=f.x[0]),
        lagn=lag(f.x, default=f.x[0])
    )
    assert_iterable_equal(res.leadn, lead(df.x, default=df.x[0]))
    assert_iterable_equal(res.lagn, lag(df.x, default=df.x[0]))

    res = mutate(
        df,
        leadn=lead(f.x, default=[1]),
        lagn=lag(f.x, default=[1])
    )
    assert_iterable_equal(res.leadn, lead(df.x, default=[1]))
    assert_iterable_equal(res.lagn, lag(df.x, default=[1]))


def test_ntile_puts_large_groups_first():

    assert_iterable_equal(ntile(range(1), n=5), [1])
    assert_iterable_equal(ntile(range(2), n=5), np.arange(2) + 1)
    assert_iterable_equal(ntile(range(3), n=5), np.arange(3) + 1)
    assert_iterable_equal(ntile(range(4), n=5), np.arange(4) + 1)
    assert_iterable_equal(ntile(range(5), n=5), np.arange(5) + 1)
    assert_iterable_equal(ntile(range(6), n=5), c(1, np.arange(5) + 1))
    assert_iterable_equal(ntile(range(1), n=7), [1])
    assert_iterable_equal(ntile(range(2), n=7), np.arange(2) + 1)
    assert_iterable_equal(ntile(range(3), n=7), np.arange(3) + 1)
    assert_iterable_equal(ntile(range(4), n=7), np.arange(4) + 1)
    assert_iterable_equal(ntile(range(5), n=7), np.arange(5) + 1)
    assert_iterable_equal(ntile(range(6), n=7), np.arange(6) + 1)
    assert_iterable_equal(ntile(range(7), n=7), np.arange(7) + 1)
    assert_iterable_equal(ntile(range(8), n=7), c(1, np.arange(7) + 1))


def test_plain_arrays():
    out = min_rank([1, 1, 2])
    assert_iterable_equal(out, [1, 1, 3])
    out = row_number([1, 1, 2])
    assert_iterable_equal(out, [1, 2, 3])
    out = ntile(1, n=1)
    assert_iterable_equal(out, [1])
    out = ntile((i for i in range(1)), n=1)
    assert_iterable_equal(out, [1])
    out = cume_dist(1)
    assert_iterable_equal(out, [1])
    out = cume_dist([])
    assert_iterable_equal(out, [])
    out = percent_rank([])
    assert_iterable_equal(out, [])
    out = percent_rank([1, 2, 3])
    assert_iterable_equal(out, [0, 0.5, 1.0], approx=True)


# Groups
def test_row_number_with_groups():
    df = tibble(x=[3, 3, 4, 4]).group_by("x")
    out = df >> mutate(n=row_number())
    assert_iterable_equal(out.n.obj, [1, 2, 1, 2])

    out = df >> mutate(n=row_number() + 1)
    assert_iterable_equal(out.n.obj, [2, 3, 2, 3])


def test_ntile_with_groups():
    df = tibble(x=c[1:9], y=[1] * 4 + [2] * 4)
    out = ntile(df.x, n=2)
    assert out.tolist() == [1, 1, 1, 1, 2, 2, 2, 2]

    df = df.groupby("y")
    out = ntile(df.x, n=2)
    assert out.tolist() == [1, 1, 2, 2, 1, 1, 2, 2]


def test_min_rank_with_groups():
    df = tibble(x=rep(c[1:5], each=2), y=rep([1, 2], each=4))
    out = min_rank(df.x)
    assert out.tolist() == [1, 1, 3, 3, 5, 5, 7, 7]

    df = df.groupby("y")
    out = min_rank(df.x)
    assert out.tolist() == [1, 1, 3, 3, 1, 1, 3, 3]


def test_dense_rank_with_groups():
    df = tibble(x=rep(c[1:5], each=2), y=rep([1, 2], each=4))
    out = dense_rank(df.x)
    assert out.tolist() == [1, 1, 2, 2, 3, 3, 4, 4]

    df = df.groupby("y")
    out = dense_rank(df.x)
    assert out.tolist() == [1, 1, 2, 2, 1, 1, 2, 2]


def test_percent_rank_with_groups():
    df = tibble(x=rep(c[1:5], each=2), y=rep([1, 2], each=4))
    out = percent_rank(df.x)
    assert_iterable_equal(
        out,
        [0.0, 0.0, 0.333, 0.333, 0.666, 0.666, 1.0, 1.0],
        approx=1e-3,
    )

    df = df.groupby("y")
    out = percent_rank(df.x)
    assert_iterable_equal(
        out,
        [0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0],
    )


def test_cume_dist_with_groups():
    df = tibble(x=rep(c[1:5], each=2), y=rep([1, 2], each=4))
    out = cume_dist(df.x)
    assert_iterable_equal(
        out,
        [0.25, 0.25, 0.5, 0.5, 0.75, 0.75, 1.0, 1.0],
        approx=1e-3,
    )

    df = df.groupby("y")
    out = cume_dist(df.x)
    assert_iterable_equal(
        out,
        [0.5, 0.5, 1.0, 1.0, 0.5, 0.5, 1.0, 1.0],
    )
