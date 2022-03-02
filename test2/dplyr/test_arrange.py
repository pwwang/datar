# https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-arrange.r

import pytest
from pandas import Series
from pandas.testing import assert_frame_equal
from datar2 import f
from datar2.tibble import tibble
from datar2.base import NA, rep, c
from datar2.dplyr import arrange, desc, group_by, group_vars, group_rows, across
from datar2.testing import assert_tibble_equal
from datar2.core.tibble import TibbleGrouped
from datar2.core.exceptions import NameNonUniqueError

from ..conftest import assert_iterable_equal


def test_empty_returns_self():
    df = tibble(x=range(1, 11), y=range(1, 11))
    gf = df >> group_by(f.x)

    assert arrange(df).equals(df)

    out = arrange(gf)
    assert out.equals(gf)
    assert group_vars(out) == group_vars(gf)


def test_sort_empty_df():
    df = tibble()
    out = df >> arrange()
    assert_tibble_equal(out, df)


def test_na_end():
    df = tibble(x=c(2, 1, NA))  # NA makes it float
    out = df >> arrange(f.x)
    assert_iterable_equal(out.x, [1, 2, None])
    out = df >> arrange(desc(f.x))
    assert_iterable_equal(out.x, [2, 1, None])


def test_errors():
    x = Series(1, name="x")
    df = tibble(x, x, _name_repair="minimal")

    with pytest.raises(NameNonUniqueError):
        df >> arrange(f.x)

    df = tibble(x=x)
    with pytest.raises(KeyError):
        df >> arrange(f.y)

    with pytest.raises(ValueError, match="incompatible index"):
        df >> arrange(rep(f.x, 2))


def test_df_cols():
    df = tibble(x=[1, 2, 3], y=tibble(z=[3, 2, 1]))
    out = df >> arrange(f.y)
    expect = tibble(x=[3, 2, 1], y=tibble(z=[1, 2, 3]))
    assert out.reset_index(drop=True).equals(expect)


def test_complex_cols():
    df = tibble(x=[1, 2, 3], y=[3 + 2j, 2 + 2j, 1 + 2j])
    out = df >> arrange(f.y)
    assert_iterable_equal(out.x, [3, 2, 1])


def test_ignores_group():
    df = tibble(g=[2, 1] * 2, x=[4, 3, 2, 1])
    gf = df >> group_by(f.g)
    out = gf >> arrange(f.x)
    assert out.equals(df.iloc[[3, 2, 1, 0], :].reset_index(drop=True))

    out = gf >> arrange(f.x, _by_group=True)
    exp = df.iloc[[3, 1, 2, 0], :].reset_index(drop=True)
    assert_frame_equal(out, exp)


def test_update_grouping():
    df = tibble(g=[2, 2, 1, 1], x=[1, 3, 2, 4])
    res = df >> group_by(f.g) >> arrange(f.x)
    assert isinstance(res, TibbleGrouped)
    assert group_rows(res) == [[0, 2], [1, 3]]


def test_across():
    df = tibble(x=[1, 3, 2, 1], y=[4, 3, 2, 1])

    out = df >> arrange(across())
    expect = df >> arrange(f.x, f.y)
    assert out.equals(expect)

    out = df >> arrange(across(None, desc))
    expect = df >> arrange(desc(f.x), desc(f.y))
    assert out.equals(expect)

    out = df >> arrange(across(f.x))
    expect = df >> arrange(f.x)
    assert out.equals(expect)

    out = df >> arrange(across(f.y))
    expect = df >> arrange(f.y)
    assert out.equals(expect)
