# tests grabbed from:
# https://github.com/tidyverse/tidyr/blob/master/tests/testthat/test-chop.R
import pytest
from pandas.testing import assert_frame_equal
from datar.all import *

# chop --------------------------------------------------------------------
def test_chop_multiple_columns():
    df = tibble(x=[1,1,2], a=[1,2,3], b=[1,2,3])
    out = df >> chop([f.a, f.b])

    assert_frame_equal(
        out,
        tibble(x=[1,2], a=[[1,2], [3]], b=[[1,2], [3]])
    )

def test_chop_no_columns_returns_input():
    df = tibble(a1 = 1, a2 = 2, b1 = 1, b2 = 2)
    assert_frame_equal(chop(df), df)

def test_chop_grouping_preserves():
    df = tibble(g = c(1, 1), x = [1,2])
    out = df >> group_by(f.g) >> chop(f.x)
    assert group_vars(out) == ['g']

def test_can_chop_empty_frame():
    df = tibble(x=[], y=[])
    df.index = []
    df['x'] = df['x'].astype(object)
    df['y'] = df['y'].astype(object)
    assert_frame_equal(chop(df, f.y), df)
    assert_frame_equal(chop(df, f.x), df[['y', 'x']])

def test_chop_with_all_column_vals():
    df = tibble(x=[1,1,2], a=[1,2,3], b=[1,2,3])
    out = chop(df, ['x', 'a', 'b'])
    assert_frame_equal(out, tibble(
        x=[[1,1,2]], a=[[1,2,3]], b=[[1,2,3]]
    ))

def test_chop_with_all_column_keys():
    df = tibble(x=[1,1,2], a=[1,2,3], b=[1,2,3])
    out = chop(df, [])
    assert_frame_equal(out, df)

# unchop ------------------------------------------------------------------

def test_unchop_extends_into_rows():
    df = tibble(x = [1, 2], y = [NULL, seq(1, 4)])
    out = df >> unchop(f.y, ptype=int)
    assert_frame_equal(out, tibble(x=[2,2,2,2], y=[1,2,3,4]))

def test_can_unchop_multiple_cols():
    df = tibble(x=[1,2], y=[[1], [2,3]], z=[[4], [5,6]])
    out = df >> unchop(c(f.y, f.z), ptype=int)
    assert_frame_equal(out, tibble(
        x=[1,2,2],
        y=[1,2,3],
        z=[4,5,6]
    ))

def test_unchopping_nothing_leaves_input_unchanged():
    df = tibble(x = f[1:3], y = f[4:6])
    assert_frame_equal(unchop(df, []), df)

def test_unchopping_null_inputs_are_dropped():
    df = tibble(
        x = f[1:4],
        y = [NULL, [1,2], 4, NULL],
        z = [NULL, [1,2], NULL, 5]
    )
    out = df >> unchop(c(f.y, f.z), ptype=float)
    assert_frame_equal(out, tibble(
        x=[2,2,3,4],
        y=[1,2,4,NA],
        z=[1,2,NA,5],
        _dtypes=float
    ))

def test_unchop_optionally_keep_empty_rows():
    df = tibble(
        x = [1,2],
        y = [NULL, [1,2]],
        # unchopping y meaning x, z will be keys and they have to be hashable
        # z = [tibble(x=[]), tibble(x=[1,2])]
    )
    out = df >> unchop(f.y, keep_empty=True)
    assert_frame_equal(out, tibble(x=[1,2,2], y=[None, 1,2], _dtypes={'y': object}))

#   out <- df %>% unchop(z, keep_empty = TRUE)
#   expect_equal(out$x, c(1, 2, 2))
#   expect_equal(out$z, tibble(x = c(NA, 1L, 2L)))
# })

def test_unchop_preserves_columns_of_empty_inputs():
    df = tibble(x=[], y=[], z=[], _dtypes={'x': int})
    assert unchop(df, f.y).columns.tolist() == ['x', 'y', 'z']
    assert unchop(df, [f.y, f.z]).columns.tolist() == ['x', 'y', 'z']

# test_that("respects list_of types", {
#   df <- tibble(x = integer(), y = list_of(.ptype = integer()))
#   expect_equal(df %>% unchop(y), tibble(x = integer(), y = integer()))
# })

def test_unchop_preserves_grouping():
    df = tibble(g=1, x=[[1,2]])
    out = df >> group_by(f.g) >> unchop(f.x)
    assert group_vars(out) == ['g']

def test_unchop_empty_list():
    df = tibble(x=[], y=[])
    out = unchop(df, f.y).y.to_list()
    assert out == []

    df = tibble(x=[], y=tibble(z=[]))
    # support nested df?
    out = unchop(df, f['y$z']) >> pull(f.y)
    assert_frame_equal(out >> drop_index(), tibble(z=[], _dtypes=object))

def test_unchop_recycles_size_1_inputs():
    df = tibble(x=[[1], [2,3]], y=[[2,3], [1]])
    out = unchop(df, [f.x, f.y], ptype=int)
    exp = tibble(x=[1,2,3], y=[2,3,1])
    # exp = tibble(x=[1,1,2,3], y=[2,3,1,1])
    assert_frame_equal(out, exp)

def test_unchop_can_specify_dtypes():
    df = tibble(x=1, y=[[1,2]])
    dtypes = {'y': int, 'z': int}
    # No extra columns added
    exp = tibble(x=[1,1], y=[1,2])
    # exp = tibble(x=[1,1], y=[1,2], z=[NA,NA])
    out = unchop(df, f.y, ptype=dtypes)
    assert_frame_equal(out, exp)

# test_that("can specify a ptype with extra columns", {
#   df <- tibble(x = 1, y = list(1, 2))
#   ptype <- tibble(y = numeric(), z = numeric())

#   expect <- tibble(x = c(1, 1), y = c(1, 2), z = c(NA_real_, NA_real_))

#   expect_identical(unchop(df, y, ptype = ptype), expect)
# })

def test_unchop_can_specify_dtypes_to_force_output_type():
    df = tibble(x=[[1,2]])
    out = unchop(df, f.x, ptype=float)
    exp = tibble(x=[1.0,2.0])
    assert_frame_equal(out, exp)

def test_can_unchop_empty_data_frame():
    chopped = tibble(x=[], y=[[]])
    out = unchop(chopped, f.y)
    assert out.shape == (0, 2)

# test_that("unchop retrieves correct types with emptied chopped df", {
#   chopped <- chop(tibble(x = 1:3, y = 4:6), y)
#   empty <- vec_slice(chopped, 0L)
#   expect_identical(unchop(empty, y), tibble(x = integer(), y = integer()))
# })
