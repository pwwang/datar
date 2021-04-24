# tests grabbed from:
# https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-rank.r
import pytest
import pandas
from datar.all import *

def assert_iterable_equal(it1, it2):
    for i, elem in enumerate(it1):
        if pandas.isna(elem) and pandas.isna(it2[i]):
            continue
        assert elem == it2[i], (
            f"{i}th elemment is different: {elem} != {it2[i]}"
        )

def ntile_h(x, n):
    return tibble(x=x) >> mutate(y=ntile(x,n)) >> pull(f.y, to='list')

def test_ntile_ignores_number_of_nas():
    x = c(1,2,3,NA, NA)
    out = ntile(x, n=3)
    assert_iterable_equal(out, [0,1,2,NA,NA])

    out = ntile_h(x, 3)
    assert_iterable_equal(out, [0,1,2,NA,NA])

    x1 = c(1,1,1,NA,NA,NA)
    out = ntile(x1, n=1)
    assert_iterable_equal(out, [0,0,0,NA,NA,NA])
    out = ntile_h(x1, 1)
    assert_iterable_equal(out, [0,0,0,NA,NA,NA])

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
        ntile=ntile(f.a, 2),
        min_rank=min_rank(f.a),
        percent_rank=percent_rank(f.a),
        dense_rank=dense_rank(f.a),
        cume_dist=cume_dist(f.a)
    )
    assert names(res) == c("a", "row_number_0","ntile", "min_rank", "percent_rank", "dense_rank", "cume_dist")
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

    assert_iterable_equal(ntile(range(1), n=5), [0])
    assert_iterable_equal(ntile(range(2), n=5), list(range(2)))
    assert_iterable_equal(ntile(range(3), n=5), list(range(3)))
    assert_iterable_equal(ntile(range(4), n=5), list(range(4)))
    assert_iterable_equal(ntile(range(5), n=5), list(range(5)))
    assert_iterable_equal(ntile(range(6), n=5), c(0, range(5)))
    assert_iterable_equal(ntile(range(1), n=7), [0])
    assert_iterable_equal(ntile(range(2), n=7), list(range(2)))
    assert_iterable_equal(ntile(range(3), n=7), list(range(3)))
    assert_iterable_equal(ntile(range(4), n=7), list(range(4)))
    assert_iterable_equal(ntile(range(5), n=7), list(range(5)))
    assert_iterable_equal(ntile(range(6), n=7), list(range(6)))
    assert_iterable_equal(ntile(range(7), n=7), list(range(7)))
    assert_iterable_equal(ntile(range(8), n=7), c(0, range(7)))
