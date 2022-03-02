# tests grabbed from:
# https://github.com/tidyverse/tidyr/blob/HEAD/tests/testthat/test-separate.R
from datar.core.grouped import DataFrameGroupBy
import pytest
from datar.all import *
from pandas.testing import assert_frame_equal
from .conftest import assert_iterable_equal

def test_missing_values_in_input_are_missing_in_output():
    df = tibble(x=c(NA, "a b"))
    out = separate(df, f.x, c("x", "y"))

    assert_iterable_equal(out.x, [NA, "a"])
    assert_iterable_equal(out.y, [NA, "b"])

def test_positive_integer_values_specific_position_between_strings():
    df = tibble(x = c(NA, "ab", "cd"))
    out = separate(df, f.x, c("x", "y"), 1)
    assert_iterable_equal(out.x, c(NA, "a", "c"))
    assert_iterable_equal(out.y, c(NA, "b", "d"))

def test_negative_integer_values_specific_position_between_strings():
    df = tibble(x = c(NA, "ab", "cd"))
    out = separate(df, f.x, c("x", "y"), -1)
    assert_iterable_equal(out.x, c(NA, "a", "c"))
    assert_iterable_equal(out.y, c(NA, "b", "d"))

def test_extreme_integer_values_handled_sensibly():
    df = tibble(x = c(NA, "a", "bc", "def"))

    out = separate(df, f.x, c("x", "y"), 3)
    assert_iterable_equal(out.x, c(NA, "a", "bc", "def"))
    assert_iterable_equal(out.y, c(NA, "", "", ""))

    out = separate(df, f.x, c("x", "y"), -3)
    assert_iterable_equal(out.x, c(NA, "", "", ""))
    assert_iterable_equal(out.y, c(NA, "a", "bc", "def"))

def test_convert_produces_integers_etc():
    df = tibble(x = "1-1.5-")
    out = separate(df, f.x, c("x", "y", "z"), "-", convert = {
        'x': int,
        'y': float,
        'z': bool,
    })
    assert_iterable_equal(out.x, [1])
    assert_iterable_equal(out.y, [1.5])
    assert_iterable_equal(out.z, [FALSE])

def test_convert_keeps_characters_as_character():
    df = tibble(x = "X-1")
    out = separate(df, f.x, c("x", "y"), "-", convert = {
        'x': str, 'y': int
    })
    assert_iterable_equal(out.x, ["X"])
    assert_iterable_equal(out.y, [1])

def test_too_many_pieces_dealt_with_as_requested(caplog):
    df = tibble(x = c("a b", "a b c"))
    separate(df, f.x, c("x", "y"))
    assert "Additional pieces discarded" in caplog.text
    caplog.clear()

    merge = separate(df, f.x, c("x", "y"), extra = "merge")
    assert_iterable_equal(merge.iloc[:, 0], c("a", "a"))
    assert_iterable_equal(merge.iloc[:, 1], c("b", "b c"))

    drop = separate(df, f.x, c("x", "y"), extra = "drop")
    assert_iterable_equal(drop.iloc[:, 0], c("a", "a"))
    assert_iterable_equal(drop.iloc[:, 1], c("b", "b"))

#   suppressWarnings(
#     expect_warning(separate(df, x, c("x", "y"), extra = "error"), "deprecated")
#   )


def test_too_few_pieces_dealt_with_as_requested(caplog):
    df = tibble(x = c("a b", "a b c"))

    separate(df, f.x, c("x", "y", "z"))
    assert "Missing pieces filled" in caplog.text
    caplog.clear()

    left = separate(df, f.x, c("x", "y", "z"), fill = "left")
    assert_iterable_equal(left.x, c(NA, "a"))
    assert_iterable_equal(left.y, c("a", "b"))
    assert_iterable_equal(left.z, c("b", "c"))

    right = separate(df, f.x, c("x", "y", "z"), fill = "right")
    assert_iterable_equal(right.z, c(NA, "c"))


def test_preserves_grouping():
    df = tibble(g = 1, x = "a:b") >> group_by(f.g)
    rs = df >> separate(f.x, c("a", "b"))
    assert group_vars(df) == group_vars(rs)


def test_drops_grouping_when_needed():
    df = tibble(x = "a:b") >> group_by(f.x)
    rs = df >> separate(f.x, c("a", "b"))
    assert_iterable_equal(rs.a, ["a"])
    assert group_vars(rs) == []


def test_overwrites_existing_columns():
    df = tibble(x = "a:b")
    rs = df >> separate(f.x, c("x", "y"))

    assert_iterable_equal(rs.columns, c("x", "y"))
    assert_iterable_equal(rs.x, ["a"])


def test_drops_NA_columns():
    df = tibble(x = c(NA, "ab", "cd"))
    out = separate(df, f.x, c(NA, "y"), 1)
    assert_iterable_equal(names(out), "y")
    assert_iterable_equal(out.y, c(NA, "b", "d"))


def test_checks_type_of_into_and_sep():
    df = tibble(x = "a:b")
    with pytest.raises(ValueError, match="Index 0 given for 1-based indexing"):
        # False for sep interpreted as 0
        separate(df, f.x, "x", FALSE)

    with pytest.raises(ValueError, match="must be a string"):
        df >> separate(f.x, FALSE)

def test_remove_false():
    df = tibble(x=c("a b"))
    out = separate(df, f.x, c("x", "y"), remove=False)
    assert out.columns.tolist() == ['x', 'y']
    out = separate(df, f.x, c("a", "b"), remove=False)
    assert out.columns.tolist() == ['x', 'a', 'b']

def test_separate_on_group_vars():
    df = tibble(x=c("a b")) >> group_by(f.x)
    out = separate(df, f.x, c("x", "y"), remove=False)
    assert group_vars(out) == ['x']

    df = tibble(x=c("a b"), y=1) >> group_by(f.x, f.y)
    out = separate(df, f.x, c("x", "y"), remove=False)
    assert group_vars(out) == ['x', 'y']

# separate_rows --------------------------------

def test_can_handle_collapsed_rows():
    df = tibble(x=f[1:3], y=c("a", "d,e,f", "g,h"))
    out = separate_rows(df, f.y)
    assert_iterable_equal(out.y, list("adefgh"))

def test_can_handle_empty_dfs():
    df = tibble(a=[], b=[], dtypes_=str)
    rs = separate_rows(df, f.b)
    assert_frame_equal(rs, df)

# test_that("default pattern does not split decimals in nested strings", {
#   df <- dplyr::tibble(x = 1:3, y = c("1", "1.0,1.1", "2.1"))
#   expect_equal(separate_rows(df, y)$y, unlist(strsplit(df$y, ",")))
# })

def test_preserves_grouping():
    df = tibble(g=1, x="a:b") >> group_by(f.g)
    rs = df >> separate_rows(f.x)
    assert group_vars(df) == group_vars(rs)

def test_drops_grouping_when_needed():
    df = tibble(x=1, y="a:b") >> group_by(f.x, f.y)
    out = df >> separate_rows(f.y)
    assert_iterable_equal(out.y, c("a", "b"))
    assert group_vars(out) == ['x']

    out = df >> group_by(f.y) >> separate_rows(f.y)
    assert not isinstance(out, DataFrameGroupBy)

def test_drops_grouping_on_zero_row_dfs_when_needed():
    df = tibble(x = [], y = []) >> group_by(f.y)
    out = df >> separate_rows(f.y)
    assert not isinstance(out, DataFrameGroupBy)





# test_that("drops grouping on zero row data frames when needed (#886)", {
#   df <- tibble(x = numeric(), y = character()) %>% dplyr::group_by(y)
#   out <- df %>% separate_rows(y)
#   expect_equal(dplyr::group_vars(out), character())
# })

# test_that("convert produces integers etc", {
#   df <- tibble(x = "1,2,3", y = "T,F,T", z = "a,b,c")

#   out <- separate_rows(df, x, y, z, convert = TRUE)
#   expect_equal(class(out$x), "integer")
#   expect_equal(class(out$y), "logical")
#   expect_equal(class(out$z), "character")
# })

# test_that("leaves list columns intact (#300)", {
#   df <- tibble(x = "1,2,3", y = list(1))

#   out <- separate_rows(df, x)
#   # Can't compare tibbles with list columns directly
#   expect_equal(names(out), c("x", "y"))
#   expect_equal(out$x, as.character(1:3))
#   expect_equal(out$y, rep(list(1), 3))
# })

# test_that("does not silently drop blank values (#1014)", {
#   df <- tibble(x = 1:3, y = c("a", "d,e,f", ""))

#   out <- separate_rows(df, y)
#   expect_equal(out, tibble(x = c(1, 2, 2, 2, 3),
#                            y = c("a", "d", "e", "f", "")))
# })
