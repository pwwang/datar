# tests grabbed from:
# https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-funs.R
# https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-window.R
# https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-na-if.R
# https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-near.R
# https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-nth-value.R

# testing functions in datar.dplyr.funs

import pandas
import pytest
from datar.all import *

def test_between():
    b = between([2,5], 1, 3).tolist()
    assert b == [True, False]

def test_between_returns_na_if_any_arg_is_na():
    out = between(1, 1, NA)
    assert is_na(out)
    out = between(1, NA, 1)
    assert is_na(out)
    out = between(NA, 1, 1)
    assert is_na(out)

# only scalar supported for left and right
def test_clearly_errors_that_not_vectorized():
    with pytest.raises(ValueError, match="right"):
        between(1, 1, [1,2])
    with pytest.raises(ValueError, match="left"):
        between(1, [1,2], 1)

# In python: a <= x <= b not working as expected when x is iterable (Series or ndarray).
# test_that("compatible with base R", {
#   x <- runif(1e3)
#   expect_equal(between(x, 0.25, 0.5), x >= 0.25 & x <= 0.5)
# })

# test_that("warns when called on S3 object", {
#   expect_warning(between(structure(c(1, 5), class = "foo"), 1, 3), "numeric vector with S3 class")
#   expect_warning(between(factor("x"), 1, 2), "S3 class")
# })

# test_that("unless it's a date or date time", {
#   expect_warning(between(Sys.Date(), 1, 3), NA)
#   expect_warning(between(Sys.time(), 1, 3), NA)
# })

# window
# ------------------------------------------------------------

def test_lead_lag_return_x_if_n_eqs_0():
    assert lead([1,2], 0) == [1,2]
    assert lag([1,2], 0) == [1,2]

def test_lead_lag_return_all_nas_if_n_eqs_lenx():
    assert lead([1,2], 2).fillna(0).tolist() == [0., 0.]
    assert lag([1,2], 2).fillna(0).tolist() == [0., 0.]

def test_cumany_cumall_handle_nas_consistently():
    batman = [NA] * 5
    assert all(is_na(cumany(batman)))
    assert all(is_na(cumall(batman)))

    out = cumall(c(True, NA, False, NA)).fillna(-1.).tolist()
    assert out == [1., -1., 0., 0.]

    out = cumall(c(False, NA, True)).fillna(-1.).tolist()
    assert out == [0.,0.,0.]

    out = cumall(c(NA, True)).fillna(-1.).tolist()
    assert out == [-1., -1.]

    out = cumall(c(NA, False)).fillna(-1.).tolist()
    assert out == [-1., 0.]

    out = cumany(c(True, NA, False)).fillna(-1.).tolist()
    assert out == [1., 1., 1.]

    out = cumany(c(False, NA, True)).fillna(-1.).tolist()
    assert out == [0.,-1.,1.]

    # scalars
    assert is_na(cumall(NA)).all()
    assert is_na(cumany(NA)).all()
    assert cumall(True).all()
    assert not cumall(False).all()
    assert cumany(True).all()
    assert not cumany(False).all()

    assert cumall([]).tolist() == []
    assert cumany([]).tolist() == []

    assert cumall([True, True]).all()

def test_percent_rank_ignores_nas():
    out = percent_rank(c([1,2,3], NA)).fillna(-1).tolist()
    assert out == pytest.approx(c(0., .5, 1., -1.))

def test_cume_dist_ignores_nas():
    out = cume_dist(c([1.0, 2.0, 3.0], NA)).fillna(-1.).tolist()
    assert out == pytest.approx([1./3., 2./3., 1., -1.])

def test_cummean_is_not_confused_by_fp_error():
    a = rep(99., 9)
    assert all(cummean(a) == a)

def test_cummean_is_consistent_with_cumsum_and_seq_along():
    x = range(1,6)
    out = cummean(x).tolist()
    assert out == pytest.approx([1., 1.5, 2., 2.5, 3.])

    # seq_along is 0-based
    assert out == pytest.approx(cumsum(x) / (seq_along(x) + 1))
    assert cummean([]).tolist() == []

# wait for order_by
def test_order_by_returns_correct_value():
    expected = [15, 14, 12, 9, 5]
    out = with_order(range(5, 0, -1), cumsum, range(1, 6))
    assert out.tolist() == expected

    x = [5,4,3,2,1]
    y = [1,2,3,4,5]
    out = with_order(x, cumsum, y)
    assert out.tolist() == expected

# test_that("order_by() works in arbitrary envs (#2297)", {
#   env <- child_env("base")
#   expect_equal(
#     with_env(env, dplyr::order_by(5:1, cumsum(1:5))),
#     rev(cumsum(rev(1:5)))
#   )
#   expect_equal(
#     order_by(5:1, cumsum(1:5)),
#     rev(cumsum(rev(1:5)))
#   )
# })

def test_order_by_errors():
    # ok, datar's order_by cannot deal with calls with real values
    order_by(NULL, 1)
    with pytest.raises(TypeError):
        with_order(NULL, 1)

# na_if
# ----------------------------------------------------------------
def test_na_if_scalar_y_replaces_all_matching_x():
    x = [0, 1, 0]
    out = na_if(x, 0).fillna(-1.0).tolist()
    assert out == [-1., 1., -1.]

def test_na_if_errors():
    with pytest.raises(ValueError, match='Lengths'):
        na_if([1,2,3], [1,2])
    with pytest.raises(ValueError, match='Lengths'):
        na_if(1, [1,2])

# near
# -----------------------------------------------------------------
def test_near():
    assert near(sqrt(2)**2, 2).all()

# nth
# -----------------------------------------------------------------
def test_nth_works_with_lists():
    x = [1,2,3]
    assert nth(x, 0) == 1 # 0-based
    assert pandas.isna(nth(x, 3))
    assert nth(x, 3, default=1) == 1
    assert first(x) == 1
    assert last(x) == 3
    assert first(x, order_by=[3,2,1]) == 3
    assert last(x, order_by=[3,2,1]) == 1

def test_nth_negative_index():
    x = [1,2,3,4,5]
    assert nth(x, -1) == 5
    assert nth(x, -1, order_by=[3,2,1]) == 1
    assert nth(x, -3) == 3

def test_nth_index_past_ends_returns_default_value():
    x = [1,2,3,4]
    assert pandas.isna(nth(x, 4))
    assert pandas.isna(nth(x, -5))
    assert pandas.isna(nth(x, 10))

def test_nth_errors():
    with pytest.raises(TypeError):
        nth(range(1,11), "x")

def test_first_uses_default_value_for_0len_input():
    # we are not distinguish NAs
    assert pandas.isna(first([]))
    assert first([], default=1) == 1
    assert pandas.isna(last([]))
    assert last([], default=1) == 1
