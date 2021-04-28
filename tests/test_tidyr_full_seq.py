# tests grabbed from:
# https://github.com/tidyverse/tidyr/blob/HEAD/tests/testthat/test-full_seq.R
import pytest
from datar.all import *

from .conftest import assert_iterable_equal

def test_full_seq_errors_if_seq_isnot_regular():
    with pytest.raises(ValueError, match="not a regular sequence"):
        full_seq(c(1,3,4),2)
    with pytest.raises(ValueError, match="not a regular sequence"):
        full_seq(c(0, 10, 20), 11, tol = 1.8)

def test_full_seq_with_tol_gt_0_allows_seqs_to_fall_short_of_period():
    out = full_seq(c(0,10,20),11,tol=2)
    assert_iterable_equal(out, c(0,11,22))

def test_full_seq_pads_length_correctly_for_tol_gt_0():
    out = full_seq(c(0,10,16), 11, tol=5)
    assert_iterable_equal(out, c(0, 11))

def test_seq_donot_have_to_start_at_zero():
    out = full_seq(c(1,5),2)
    assert_iterable_equal(out, c(1,3,5))

def test_full_seq_fills_in_gaps():
    out = full_seq(c(1,3), 1)
    assert_iterable_equal(out, c(1,2,3))

# test_that("preserves attributes", {
#   x1 <- as.Date("2001-01-01") + c(0, 2)
#   x2 <- as.POSIXct(x1)

#   expect_s3_class(full_seq(x1, 2), "Date")
#   expect_s3_class(full_seq(x2, 86400), c("POSIXct", "POSIXt"))
# })
