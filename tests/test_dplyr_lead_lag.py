# tests grabbed from:
# https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-lead-lag.R
import pytest
from datar.all import *
from .conftest import assert_iterable_equal

def test_preserve_orders():
    x = factor(c("a", "b", "c"))
    assert levels(lead(x)).tolist() == ["a", "b", "c"]
    assert levels(lag(x)).tolist() == ["a", "b", "c"]

# test_that("lead and lag preserves dates and times", {
#   x <- as.Date("2013-01-01") + 1:3
#   y <- as.POSIXct(x)

#   expect_s3_class(lead(x), "Date")
#   expect_s3_class(lag(x), "Date")

#   expect_s3_class(lead(y), "POSIXct")
#   expect_s3_class(lag(y), "POSIXct")
# })

def test_dplyr_issue_925():
    data = tribble(
        f.name, f.time,
        "Rob",  3,
        "Pete", 2,
        "Rob",  5,
        "John", 3,
        "Rob",  2,
        "Pete", 3,
        "John", 2,
        "Pete", 4,
        "John", 1,
        "Pete", 1,
        "Rob",  4,
        "Rob",  1
    )
    res = data >> group_by(f.name) >> mutate(lag_time = lag(f.time))
    rob = filter(res, f.name == "Rob") >> ungroup() >> replace_na(0) >> pull(to='list')
    assert rob == [0., 3., 5., 2., 4.]
    pete = filter(res, f.name == "Pete") >> ungroup() >> replace_na(0) >> pull(to='list')
    assert pete == [0., 2., 3., 4.]
    john = filter(res, f.name == "John") >> ungroup() >> replace_na(0) >> pull(to='list')
    assert john == [0., 3., 2.]

def test_dplyr_issue_937():
    df = tibble(
        name = rep(c("Al", "Jen"), 3),
        score = rep(c(100, 80, 60), 2)
    )

    res = df >> group_by(f.name) >> mutate(next_score=lead(f.score))
    al = filter(res, f.name=='Al') >> ungroup() >> replace_na(0) >> pull(to='list')
    assert al == [60., 80., 0.]
    jen = filter(res, f.name=='Jen') >> ungroup() >> replace_na(0) >> pull(to='list')
    assert jen == [100., 60., 0.]

# test_that("lead() and lag() work for matrices (#5028)", {
#   m <- matrix(1:6, ncol = 2)
#   expect_equal(lag(m, 1), matrix(c(NA_integer_, 1L, 2L, NA_integer_, 4L, 5L), ncol = 2))
#   expect_equal(lag(m, 1, default = NA), matrix(c(NA_integer_, 1L, 2L, NA_integer_, 4L, 5L), ncol= 2))

#   expect_equal(lead(m, 1), matrix(c(2L, 3L, NA_integer_, 5L, 6L, NA_integer_), ncol = 2))
#   expect_equal(lead(m, 1, default = NA), matrix(c(2L, 3L, NA_integer_, 5L, 6L, NA_integer_), ncol = 2))
# })

def test_check_size_of_default():
    with pytest.raises(ValueError):
        lead(range(1,11), default=[])
    with pytest.raises(ValueError):
        lag(range(1,11), default=[])

# # Errors ------------------------------------------------------------------

def test_errors():
    with pytest.raises(ValueError):
        lead(letters, -1)
    with pytest.raises(ValueError):
        lead(letters, "1")
    with pytest.raises(ValueError):
        lag(letters, -1)
    with pytest.raises(ValueError):
        lag(letters, "1")
    # with pytest.raises(ValueError):
    #     lag(["1", "2", "3"], default=False)
    with pytest.raises(ValueError):
        lag(["1", "2", "3"], default=[])

#   "# ts"
#   expect_snapshot(error = TRUE, lag(ts(1:10)))

#   "# incompatible default"
#   expect_snapshot(error = TRUE, lag(c("1", "2", "3"), default = FALSE))
#   expect_snapshot(error = TRUE, lag(c("1", "2", "3"), default = character()))
# })

def test_order_by():
    x = seq(1,10)
    out = lag(x)
    assert_iterable_equal(out, c(NA, seq(1,9)))

    out = lag(x, order_by=seq(10,1))
    assert_iterable_equal(out, c(seq(2,10), NA))

