# tests grabbed from:
# https://github.com/tidyverse/tidyr/blob/HEAD/tests/testthat/test-drop-na.R
import pytest
from pandas.testing import assert_frame_equal
from datar.all import *


def test_empty_call_drops_every_row():
    df = tibble(x=c(1,2,NA), y=c("a", NA, "b"))
    # NA is a float
    exp = tibble(x=1, y="a", _dtypes={'x': float})
    assert_frame_equal(drop_na(df), exp)

def test_only_considers_specified_vars():
    df = tibble(x=c(1,2,NA), y=c("a", None, "b"))
    exp = tibble(x=[1,2], y=c("a", None), _dtypes={'x': float})
    out = drop_na(df, f.x)
    assert_frame_equal(out, exp)

    exp = tibble(x=[1], y=c("a"), _dtypes={'x': float})
    out = drop_na(df, f[f.x:f.y])
    assert_frame_equal(out, exp)

def test_groups_are_preserved():
    df = tibble(g = c("A", "A", "B"), x = c(1, 2, NA), y = c("a", NA, "b"))
    exp = tibble(g = c("A", "B"), x = c(1, NA), y = c("a", "b"))

    gdf = group_by(df, f.g)
    gexp = group_by(exp, f.g)

    out = drop_na(gdf, f.y)
    assert_frame_equal(out, gexp)
    assert group_vars(out) == group_vars(gexp)

def test_empty_call_drops_every_row():
    df = tibble(x=c(1,2,NA), y=c("a", NA, "b"))
    out = drop_na(df)
    assert_frame_equal(out, tibble(x=1., y="a"))

def test_errors_are_raised():
    df = tibble(x=c(1,2,NA), y=c("a", NA, "b"))
    with  pytest.raises(KeyError):
        drop_na(df, f.z)

def test_single_variable_var_doesnot_lose_dimension():
    df = tibble(x=c(1,2,NA))
    out = drop_na(df, f.x)
    exp = tibble(x=c(1.,2.))
    assert_frame_equal(out, exp)

def test_works_with_list_cols():
    df = tibble(x=[[1], NULL, [3]], y=[1, 2, NA])
    rs = drop_na(df)

    assert_frame_equal(rs, tibble(x=[[1]], y=1.))

def test_preserves_attributes():
    df = tibble(x = c(1, NA))
    df.attrs['a'] = 10
    out = drop_na(df)
    assert_frame_equal(out, tibble(x=1.))
    assert out.attrs['a'] == 10
