# tests grabbed from:
# https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-funs.R
# https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-window.R
# https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-na-if.R
# https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-near.R
# https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-nth-value.R

# testing functions in datar.dplyr.funs
import pytest

import pandas as pd
from datar.dplyr import (
    between,
    lead,
    lag,
    cumany,
    cumall,
    nth,
    order_by,
    with_order,
    percent_rank,
    cume_dist,
    cummean,
    na_if,
    near,
    desc,
    first,
    last,
)
from datar.base import (
    c,
    NA,
    rep,
    cumsum,
    seq_along,
    NULL,
    sqrt,
    factor,
    letters,
)
from datar.tibble import tibble

from ..conftest import assert_iterable_equal


def test_between():
    b = between([2, 5], 1, 3).tolist()
    assert b == [True, False]


def test_between_returns_na_if_any_arg_is_na():
    out = between(1, 1, NA)
    assert_iterable_equal(out, [False])
    out = between(1, NA, 1)
    assert_iterable_equal(out, [False])
    out = between(NA, 1, 1)
    assert_iterable_equal(out, [False])


# only scalar supported for left and right
def test_clearly_errors_that_not_vectorized():
    with pytest.raises(ValueError):
        between(1, 1, [1, 2])
    with pytest.raises(ValueError):
        between(1, [1, 2], 1)


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
    assert_iterable_equal(lead([1, 2], 0), [1, 2])
    assert_iterable_equal(lag([1, 2], 0), [1, 2])


def test_lead_lag_return_all_nas_if_n_eqs_lenx():
    assert lead([1, 2], 2).fillna(0).tolist() == [0.0, 0.0]
    assert lag([1, 2], 2).fillna(0).tolist() == [0.0, 0.0]


def test_cumany_cumall_handle_nas_consistently():
    batman = [NA] * 5
    assert not cumany(batman).all()
    assert not cumall(batman).all()

    out = cumall(c(True, NA, False, NA)).tolist()
    assert out == [True, False, False, False]

    out = cumall(c(False, NA, True)).tolist()
    assert out == [False, False, False]

    out = cumall(c(NA, True)).tolist()
    assert out == [False, False]

    out = cumall(c(NA, False)).tolist()
    assert out == [False, False]

    out = cumany(c(True, NA, False)).tolist()
    assert out == [True, True, True]

    out = cumany(c(False, NA, True)).tolist()
    assert out == [False, False, True]

    # scalars
    assert_iterable_equal(cumall(NA), [False])
    assert_iterable_equal(cumany(NA), [False])
    assert cumall(True).all()
    assert_iterable_equal(cumall(False), [False])
    assert_iterable_equal(cumany(True), [True])
    assert_iterable_equal(cumany(False), [False])

    assert cumall([]).tolist() == []
    assert cumany([]).tolist() == []

    assert cumall([True, True]).all()


def test_percent_rank_ignores_nas():
    out = percent_rank(c([1, 2, 3], NA))
    assert_iterable_equal(out, c(0.0, 0.5, 1.0, NA), approx=True)


def test_cume_dist_ignores_nas():
    out = cume_dist(c([1.0, 2.0, 3.0], NA))
    assert_iterable_equal(out, [1.0 / 3.0, 2.0 / 3.0, 1.0, NA], approx=True)


def test_cummean_is_not_confused_by_fp_error():
    a = rep(99.0, 9)
    assert all(cummean(a) == a)


def test_cummean_is_consistent_with_cumsum_and_seq_along():
    x = range(1, 6)
    out = cummean(x).tolist()
    assert out == pytest.approx([1.0, 1.5, 2.0, 2.5, 3.0])

    # seq_along is 0-based
    assert out == pytest.approx(cumsum(x) / seq_along(x))
    assert cummean([]).tolist() == []


def test_order_by_returns_correct_value():
    expected = [15, 14, 12, 9, 5]
    out = with_order(range(5, 0, -1), cumsum, range(1, 6))
    assert out.tolist() == expected

    x = [5, 4, 3, 2, 1]
    y = [1, 2, 3, 4, 5]
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
    # with_order(NULL, 1)
    with pytest.raises(ValueError):
        order_by(NULL, 1)


# na_if
# ----------------------------------------------------------------
def test_na_if_scalar_y_replaces_all_matching_x():
    x = [0, 1, 0]
    out = na_if(x, 0)
    assert_iterable_equal(out, [NA, 1.0, NA])


def test_na_if_errors():
    with pytest.raises(ValueError):
        na_if([1, 2, 3], [1, 2])
    with pytest.raises(ValueError):
        na_if(1, [1, 2])


def test_na_if_sgb():
    df = tibble(x=[1, 2], y=[2, 2]).rowwise()
    out = na_if(df.x, df.y)
    assert_iterable_equal(out.obj, [1, NA])


# near
# -----------------------------------------------------------------
def test_near():
    assert near(sqrt(2) ** 2, 2).all()

    df = tibble(x=sqrt(2) ** 2, y=2).rowwise()
    out = near(df.x, df.y)
    assert_iterable_equal(out.obj, [True])


# nth
# -----------------------------------------------------------------
def test_nth_works_with_lists():
    x = [1, 2, 3]
    assert nth(x, 0) == 1  # 0-based
    assert pd.isna(nth(x, 3))
    assert nth(x, 3, default=1) == 1
    assert_iterable_equal([first(x)], [1])
    assert_iterable_equal([last(x)], [3])
    assert_iterable_equal([first(x, order_by=[3, 2, 1])], [3])
    assert_iterable_equal([last(x, order_by=[3, 2, 1])], [1])


def test_nth_negative_index():
    x = [1, 2, 3, 4, 5]
    assert nth(x, -1) == 5
    assert nth(x, -1, order_by=[5, 4, 3, 2, 1]) == 1
    assert nth(x, -3) == 3


def test_nth_index_past_ends_returns_default_value():
    x = [1, 2, 3, 4]
    assert pd.isna(nth(x, 4))
    assert pd.isna(nth(x, -5))
    assert pd.isna(nth(x, 10))


def test_nth_errors():
    with pytest.raises(ValueError):
        nth(range(1, 11), "x")


def test_first_uses_default_value_for_0len_input():
    # we are not distinguish NAs
    assert_iterable_equal([first([])], [NA])
    assert_iterable_equal([first([], default=1)], [1])
    assert_iterable_equal([last([])], [NA])
    assert_iterable_equal([last([], default=1)], [1])


# desc -------------------------------------------------------------
def test_desc():
    x = factor(c(letters[:3], NA), levels=letters[:3])
    out = desc(x)
    assert_iterable_equal(out, [-0.0, -1.0, -2.0, NA])

    out = desc([1, 2, 3])
    assert_iterable_equal(out, [-1, -2, -3])

    out = desc(["a", "b", "c"])
    assert_iterable_equal(out, [-0.0, -1.0, -2.0])
