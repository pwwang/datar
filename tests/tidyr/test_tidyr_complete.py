# tests grabbed from:
# https://github.com/tidyverse/tidyr/blob/HEAD/tests/testthat/test-complete.R
import pytest
from pandas.testing import assert_frame_equal
from datar.datasets import mtcars
from datar.all import *

from .conftest import assert_iterable_equal

def test_complete_with_no_vars_return_data_asis():
    assert_frame_equal(complete(mtcars), mtcars)

def test_basic_invocation_works():
    df = tibble(x=f[1:2], y=f[1:2], z=f[3:4])
    out = complete(df, f.x, f.y)
    assert nrow(out) == 4
    assert_iterable_equal(out.z, [3, NA, NA, 4])

def test_preserves_grouping():
    df = tibble(x=f[1:2], y=f[1:2], z=f[3:4]) >> group_by(f.x)
    out = complete(df, f.x, f.y)
    assert group_vars(out) == group_vars(df)

def test_expands_empty_factors():
    ff = factor(levels=c("a", "b", "c"))
    df = tibble(one=ff, two=ff)
    assert nrow(complete(df, f.one, f.two)) == 9
    assert ncol(complete(df, f.one, f.two)) == 2

def test_empty_expansion_returns_original():
    df = tibble(x=[])
    rs = complete(df, y=NULL)
    assert_frame_equal(rs, df)

    df = tibble(x=f[1:4])
    rs = complete(df, y=NULL)
    assert_frame_equal(rs, df)

def test_not_drop_unspecified_levels_in_complete():
    df = tibble(
        x=f[1:3],
        y=f[1:3],
        z=c("a", "b", "c")
    )
    df2 = df >> complete(z=c("a", "b"))

    exp = df[['z', 'x', 'y']]
    assert_frame_equal(df2, exp)
