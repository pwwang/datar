# tests grabbed from:
# url
import pytest  # noqa

from datar.all import *
from datar.core.backends.pandas.testing import assert_frame_equal
from ..conftest import assert_iterable_equal


def test_unite_pastes_columns_togeter_and_removes_old_col():
    df = tibble(x="a", y="b")
    out = df >> unite('z', f[f.x:])
    assert_frame_equal(out, tibble(z="a_b"))


def test_unite_does_not_remove_new_col_in_case_of_name_clash():
    df = tibble(x = "a", y = "b")
    out = df >> unite('x', f[f.x:])
    cols = out >> names()
    assert_iterable_equal(cols, ["x"])
    assert_iterable_equal(out.x, ["a_b"])


def test_unite_preserves_grouping():
    df = tibble(g = 1, x = "a") >> group_by(f.g)
    rs = df >> unite('x', f.x)
    assert_frame_equal(df, rs)
    assert group_vars(df) == group_vars(rs)


def test_drops_grouping_when_needed():
    df = tibble(g = 1, x = "a") >> group_by(f.g)
    rs = df >> unite('gx', f.g, f.x)
    assert_iterable_equal(rs.gx, ["1_a"])
    assert group_vars(rs) == []

def test_empty_var_spec_uses_all_vars():
    df = tibble(x = "a", y = "b")
    assert_iterable_equal(df >> unite("z"), tibble(z = "a_b"))

def test_can_remove_missing_vars_on_request():
    df = expand_grid(x = ["a", NA], y = ["b", NA])
    out = df >> unite("z", f[f.x:], na_rm = TRUE)

    assert_iterable_equal(out.z, c("a_b", "a", "b", ""))


# test_that("regardless of the type of the NA", {
#   vec_unite <- function(df, vars) {
#     unite(df, "out", any_of(vars), na.rm = TRUE)$out
#   }

#   df <- tibble(
#     x = c("x", "y", "z"),
#     lgl = NA,
#     dbl = NA_real_,
#     chr = NA_character_
#   )

#   expect_equal(vec_unite(df, c("x", "lgl")), c("x", "y", "z"))
#   expect_equal(vec_unite(df, c("x", "dbl")), c("x", "y", "z"))
#   expect_equal(vec_unite(df, c("x", "chr")), c("x", "y", "z"))
# })


# GH#105
def test_sep_none_does_not_join_strings():
    df = tibble(x = "a", y = "b")
    out = df >> unite('z', f[f.x:], sep = None)
    assert_frame_equal(out, tibble(z = [["a", "b"]]))
