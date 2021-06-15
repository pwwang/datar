# tests grabbed from:
# https://github.com/tidyverse/tidyr/blob/HEAD/tests/testthat/test-fill.R
import pytest
from datar.all import *

from pandas.testing import assert_frame_equal
from .conftest import assert_iterable_equal

def test_all_missing_left_unchanged():
    df = tibble(
        a = c(NA, NA),
        b = c(NULL, NA),
        c = c(None, NA),
    )
    down = fill(df, f.a, f.b, f.c)
    up = fill(df, f.a, f.b, f.c, _direction="up")

    # DataFrame.fill() shrinks dtype
    up['b'] = up.b.astype(object)
    up['c'] = up.b.astype(object)

    assert_frame_equal(down, df)
    assert_frame_equal(up, df)

def test_missings_are_filled_correctly():
    df = tibble(x=c(NA, 1, NA, 2, NA, NA))

    out = fill(df, f.x)
    assert_iterable_equal(out.x, c(NA, 1,1,2,2,2))

    out = fill(df, f.x, _direction="up")
    assert_iterable_equal(out.x, c(1,1,2,2,NA,NA))

    out = fill(df, f.x, _direction="downup")
    assert_iterable_equal(out.x, c(1,1,1,2,2,2))

    out = fill(df, f.x, _direction="updown")
    assert_iterable_equal(out.x, c(1,1,2,2,2,2))

def test_missings_filled_down_for_each_atomic_vector():
    df = tibble(
        lgl = c(True, NA),
        int = c(1, NA),
        dbl = c(1.0, NA),
        chr = c("a", NaN),
        lst = [seq(1,5), NULL]
    )
    out = fill(df, everything())
    assert_iterable_equal(out.lgl, [True, True])
    assert_iterable_equal(out.int, [1, 1])
    assert_iterable_equal(out.dbl, [1.0, 1.0])
    assert_iterable_equal(out.chr, ["a", "a"])
    assert [x.tolist() for x in out.lst.tolist()] == [[1,2,3,4,5]] * 2

def test_missings_filled_up_for_each_atomic_vector():
    df = tibble(
        lgl = c(NA, True),
        int = c(NA, 1),
        dbl = c(NA, 1.0),
        chr = c(NaN, "a"),
        lst = [NULL, seq(1,5)]
    )
    out = fill(df, everything(), _direction="up")
    assert_iterable_equal(out.lgl, [True, True])
    assert_iterable_equal(out.int, [1, 1])
    assert_iterable_equal(out.dbl, [1.0, 1.0])
    assert_iterable_equal(out.chr, ["a", "a"])
    assert [x.tolist() for x in out.lst.tolist()] == [[1,2,3,4,5]] * 2

def test_fill_preserves_attributes():
    df = tibble(x=c(NA, 1))
    df.attrs['a'] = 10
    out = fill(df, f.x)
    assert out.attrs['a'] == 10

def test_fill_respects_grouping():
    df = tibble(x = c(1, 1, 2), y = c(1, NA, NA))
    out = df >> group_by(f.x) >> fill(f.y)
    assert_iterable_equal(out.y, [1,1,NA])
