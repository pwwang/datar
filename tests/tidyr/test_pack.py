# tests grabbed from:
# https://github.com/tidyverse/tidyr/blob/master/tests/testthat/test-pack.R
import pytest
from datar.core.backends.pandas.testing import assert_frame_equal
from datar.all import *

from ..conftest import assert_iterable_equal, assert_equal

# pack --------------------------------------------------------------------

def test_can_pack_multiple_columns():
    df = tibble(a1=1, a2=2, b1=1, b2=2)
    out = df >> pack(a=c(f.a1, f.a2), b=c(f.b1, f.b2))

    assert_iterable_equal(colnames(out), ['a', 'b'])
    assert_frame_equal(out >> pull(f.a), df[['a1', 'a2']])
    assert_frame_equal(out >> pull(f.b), df[['b1', 'b2']])

def test_pack_no_columns_returns_input():
    df = tibble(a1=1, a2=2, b1=1, b2=2)
    assert_frame_equal(pack(df), df)

def test_can_strip_outer_names_from_inner_names():
    df = tibble(ax=1, ay=2)
    out = pack(df, a=c(f.ax, f.ay), _names_sep="")
    out = out >> pull(f.a) >> colnames()
    assert_iterable_equal(out, ['x', 'y'])

def test_grouping_preserved():
    df = tibble(g1=1, g2=2, g3=3)
    out = df >> group_by(f.g1, f.g2) >> pack(g=c(f.g2, f.g3))
    assert_equal(group_vars(out), ['g1'])


# unpack ------------------------------------------------------------------

def test_unpack_preserves_grouping():
    df = tibble(g=1, x=tibble(y=1))
    out = df >> group_by(f.g) >> unpack(f.x)
    assert_equal(group_vars(out), ['g'])
    assert out.columns.tolist() == ['g', 'y']

def test_unpack_error_on_atomic_columns():
    df = tibble(x=c[1:2])
    with pytest.raises(ValueError, match="must be a data frame column"):
        df >> unpack(f.x)

def test_df_cols_are_directly_unpacked():
    df = tibble(x=c[1:3], y=tibble(a=c[1:3], b=c[3:1]))
    out = df >> unpack(f.y)
    assert out.columns.tolist() == ['x', 'a', 'b']
    exp = df >> pull(f.y)
    assert_frame_equal(out[['a', 'b']], exp)

def test_cannot_unpack_0col_dfs():
    # Since we only have fake packed data frame columns,
    # this gives nothing about the column, so it can't be unpacked
    df = tibble(x=c[1:4], y=tibble(_rows=3))
    # `y` doesn't even exist
    with pytest.raises(ValueError):
        df >> unpack(f.y)

# test_that("can unpack 0-col dataframe", {
#   df <- tibble(x = 1:3, y = tibble(.rows = 3))
#   out <- df %>% unpack(y)
#   expect_named(out, c("x"))
# })

def test_can_unpack_0row_dfs():
    df = tibble(x=[], y=tibble(a=[]))
    out = df >> unpack(f.y)
    assert out.columns.tolist() == ['x', 'a']

def test_unpack_0row_df():
    df = tibble(x=[], y=[])
    out = df >> unpack(f.y)
    assert_frame_equal(out, df)

def test_unpack_can_choose_separator():
    df = tibble(x = 1, y = tibble(a = 2), z = tibble(a = 3))
    out = df >> unpack([f.y, f.z], names_sep='_')
    assert out.columns.tolist() == ['x', 'y_a', 'z_a']

    out = df >> unpack([f.y], names_sep='_')
    assert out.columns.tolist() == ['x', 'y_a', 'z$a']

    out = df >> unpack([1], names_sep='_')
    assert out.columns.tolist() == ['x', 'y_a', 'z$a']

def test_unpack_cannot_select_multiple_columns_of_packed_df_by_indexes():
    df = tibble(x = 1, y = tibble(a = 2, b=3))
    with pytest.raises(ValueError, match="already been selected"):
        df >> unpack([1,2])
