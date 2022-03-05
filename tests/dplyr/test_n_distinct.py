# tests grabbed from:
# https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-n_distinct.R
import pytest  # noqa

import numpy as np
from datar import f
from datar.base import c, factor, letters, NA, identity, sum
from datar.dplyr import (
    n_distinct,
    summarise,
    group_by,
    pull,
)
from datar.tibble import tibble
from datar.datasets import iris
from ..conftest import assert_iterable_equal


df_var = tibble(
    l=c(True, False, False),
    i=c(1, 1, 2),
    # d = Sys.Date() + c(1, 1, 2),
    f=factor(letters[c(1, 1, 2)]),
    n=np.array(c(1, 1, 2)) + 0.5,
    # t = Sys.time() + c(1, 1, 2),
    c=letters[c(1, 1, 2)],
)


def test_n_disinct_gives_the_correct_results_on_iris():
    out = iris.apply(n_distinct)
    exp = iris.apply(lambda col: len(col.unique()))
    assert_iterable_equal(out, exp)


def test_n_distinct_treats_na_correctly():
    # test_that("n_distinct treats NA correctly in the REALSXP case (#384)", {
    assert n_distinct(c(1.0, NA, NA), na_rm=False) == 2


def test_n_distinct_recyles_len1_vec():
    # assert n_distinct(1, [1, 2, 3, 4]) == 4
    # assert n_distinct([1, 2, 3, 4], 1) == 4
    assert n_distinct(4) == 1
    assert n_distinct(NA, na_rm=True) == 0
    assert n_distinct([1, 2, 3, 4]) == 4

    d = tibble(x=[1, 2, 3, 4])
    res = d >> summarise(
        y=sum(f.x),
        # summrise fail to mix input and summarised data in one expression
        # n1=n_distinct(f.y, f.x),
        # n2=n_distinct(f.x, f.y),
        n3=n_distinct(f.y),
        n4=n_distinct(identity(f.y)),
        n5=n_distinct(f.x),
    )
    # assert res.n1.tolist() == [4]
    # assert res.n2.tolist() == [4]
    assert res.n3.tolist() == [1]
    assert res.n4.tolist() == [1]
    assert res.n5.tolist() == [4]

    res = (
        tibble(g=c(1, 1, 1, 1, 2, 2), x=c(1, 2, 3, 1, 1, 2))
        >> group_by(f.g)
        >> summarise(
            y=sum(f.x),
            # n1=n_distinct(f.y, f.x),
            # n2=n_distinct(f.x, f.y),
            n3=n_distinct(f.y),
            n4=n_distinct(identity(f.y)),
            n5=n_distinct(f.x),
        )
    )
    # assert res.n1.tolist() == [3,2]
    # assert res.n2.tolist() == [3,2]
    assert res.n3.tolist() == [1, 1]
    assert res.n4.tolist() == [1, 1]
    assert res.n5.tolist() == [3, 2]


def test_n_distinct_handles_unnamed():
    x = iris.Sepal_Length
    y = iris.Sepal_Width

    out = n_distinct(iris.Sepal_Length)
    exp = n_distinct(x)

    assert out == exp

    out = n_distinct(iris.Sepal_Width)
    exp = n_distinct(y)
    assert out == exp


def test_n_distinct_handles_in_na_rm():
    d = tibble(x=c([1, 2, 3, 4], NA))
    yes = True
    no = False

    out = d >> summarise(n=n_distinct(f.x, na_rm=True)) >> pull(to="list")
    assert out == [4]
    out = d >> summarise(n=n_distinct(f.x, na_rm=False)) >> pull(to="list")
    assert out == [5]
    out = d >> summarise(n=n_distinct(f.x, na_rm=yes)) >> pull(to="list")
    assert out == [4]
    out = d >> summarise(n=n_distinct(f.x, na_rm=no)) >> pull(to="list")
    assert out == [5]

    out = (
        d
        >> summarise(n=n_distinct(f.x, na_rm=True or True))
        >> pull(to="list")
    )
    assert out == [4]


def test_n_distinct_respects_data():
    df = tibble(x=42)
    out = df >> summarise(n=n_distinct(df.x))
    exp = tibble(n=1)
    assert out.equals(exp)


def test_n_distinct_works_with_str_col():
    wrapper = lambda data, col: summarise(
        data, result=n_distinct(f[col], na_rm=True)
    )

    df = tibble(x=[1, 1, 3, NA])
    out = wrapper(df, "x")
    exp = tibble(result=2)
    assert out.equals(exp)
