# tests grabbed from:
# https://github1s.com/tidyverse/dplyr/blob/master/tests/testthat/test-rows.R
import pytest
from pandas.testing import assert_frame_equal
from datar.all import *

@pytest.fixture
def data():
    return tibble(a = seq(1,3), b = c(letters[[0,1]], NA), c = [0.5, 1.5, 2.5])

def test_rows_insert(data):
    out = rows_insert(data, tibble(a = 4, b = "z"), by = "a")
    exp = tibble(a = seq(1,4), b = c("a", "b", NA, "z"), c = c(0.5, 1.5, 2.5, NA))
    assert_frame_equal(out, exp)

    with pytest.raises(ValueError, match="insert duplicate"):
        rows_insert(data, tibble(a = 3, b = "z"), by = "a")

def test_rows_update(data):
    out = rows_update(data, tibble(a = [2,3], b = "z"), by = "a")
    exp = tibble(a = seq(1,3), b = c("a", "z", "z"), c = data.c)
    assert_frame_equal(out, exp)

    with pytest.raises(ValueError, match="update missing"):
        rows_update(data, tibble(a = [2,3], b = "z"), by = c("a", "b"))

    out = rows_update(data, tibble(b = "z", a = [2,3]), by = "a")
    exp = tibble(a = seq(1,3), b = c("a", "z", "z"), c = data.c)
    assert_frame_equal(out, exp)

def test_rows_patch(data):
    out = rows_patch(data, tibble(a = [2,3], b = "z"), by = "a")
    exp = tibble(a = seq(1,3), b = c("a", "b", "z"), c = data.c)
    assert_frame_equal(out, exp)

    with pytest.raises(ValueError, match="patch missing"):
        rows_patch(data, tibble(a = [2, 3], b = "z"), by = c("a", "b"))

    out = rows_patch(data, tibble(b = "z", a = [2,3]), by = "a")
    exp = tibble(a = seq(1,3), b = c("a", "b", "z"), c = data.c)
    assert_frame_equal(out, exp)

def test_rows_upsert(data):
    out = rows_upsert(data, tibble(a = [2,3,4], b = "z"), by = "a")
    exp = tibble(a = seq(1,4), b = c("a", "z", "z", "z"), c = c(data.c.values, NA))
    assert_frame_equal(out, exp)

def test_rows_delete(data):
    out = rows_delete(data, tibble(a=[2,3]), by="a")
    assert_frame_equal(out, data.iloc[[0], :])

    with pytest.raises(ValueError, match="delete missing"):
        rows_delete(data, tibble(a=[2,3,4]), by="a")

    out = rows_delete(data, tibble(a = [2,3], b = "b"), by = "a")
    assert_frame_equal(out, data.iloc[[0], :])

    with pytest.raises(ValueError, match="delete missing"):
        rows_delete(data, tibble(a = [2,3], b = "b"), by = c("a", "b"))

def test_rows_errors(data):
    # by must be string or strings
    with pytest.raises(ValueError, match="must be a string"):
        rows_delete(data, tibble(a = [2,3]), by=1)

    # Insert
    with pytest.raises(ValueError):
        rows_insert(data, tibble(a = 3, b = "z"))

    with pytest.raises(ValueError):
        rows_insert(data.iloc[[0,0], ], tibble(a = 3))

    with pytest.raises(ValueError):
        rows_insert(data, tibble(a = 4, b = "z"), by = "e")

    with pytest.raises(ValueError):
        rows_insert(data, tibble(d = 4))

    # Update
    with pytest.raises(ValueError):
        rows_update(data, tibble(a = [2,3], b = "z"), by = c("a", "b"))

    # Variants: patch
    with pytest.raises(ValueError):
        rows_patch(data, tibble(a = [2,3], b = "z"), by = c("a", "b"))

    # Delete and truncate
    with pytest.raises(ValueError):
        rows_delete(data, tibble(a = [2,3,4]))

    with pytest.raises(ValueError):
        rows_delete(data, tibble(a = [2,3], b = "b"), by = c("a", "b"))

    # works
    # rows_delete(data, tibble(a = [2,3]))
    # rows_delete(data, tibble(a = [2,3], b = "b"))
