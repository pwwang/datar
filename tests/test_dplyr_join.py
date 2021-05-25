# tests grabbed from:
# https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-join.r
from pandas.core.frame import DataFrame
import pytest
from pandas.testing import assert_frame_equal
from datar.all import *

def test_mutating_joins_preserve_row_and_column_order():
    df1 = tibble(a=[1,2,3])
    df2 = tibble(b=1, c=2, a=[4,3,2,1])

    out = inner_join(df1, df2, by="a")
    assert out.columns.tolist() == ["a", "b", "c"]
    assert out.a.tolist() == [1,2,3]

    out = left_join(df1, df2, by="a")
    assert out.columns.tolist() == ["a", "b", "c"]
    assert out.a.tolist() == [1,2,3]

    out = right_join(df1, df2, by="a")
    assert out.columns.tolist() == ["a", "b", "c"]
    # order preserved based on df2
    assert out.a.tolist() == [4,3,2,1]

    out = full_join(df1, df2, by="a")
    assert out.columns.tolist() == ["a", "b", "c"]
    assert out.a.tolist() == [1,2,3,4]

def test_even_when_column_names_change():
    df1 = tibble(x=[1,1,2,3], z=[1,2,3,4], a=1)
    df2 = tibble(z=[1,2,3,4], b=1, x=[1,2,2,4])

    out = inner_join(df1, df2, by="x")
    assert out.columns.tolist() == ["x","z_x", "a", "z_y", "b"]

def test_by_empty_generates_cross():
    # test_that("by = character() generates cross (#4206)", {
    df1 = tibble(x=[1,2])
    df2 = tibble(y=[1,2])

    out = left_join(df1, df2, by=[]) # by=None, will try to find common columns

    assert out.x.tolist() == [1,1,2,2]
    assert out.y.tolist() == [1,2,1,2]

def test_filtering_joins_preserve_row_and_column_order():
    df1 = tibble(a=[4,3,2,1], b=1)
    df2 = tibble(b=1, c=2, a=[2,3])

    out = semi_join(df1, df2, by="a")
    assert out.columns.tolist() == ["a", "b"]
    assert out.a.tolist() == [3,2]

    out = anti_join(df1, df2, by="a")
    assert out.columns.tolist() == ["a", "b"]
    assert out.a.tolist() == [4,1]

def test_keys_are_coerced_to_symmetric_type():
    foo = tibble(id=factor(c("a", "b")), var1="foo")
    bar = tibble(id=c("a", "b"), var2="bar")

    idcoltype = inner_join(foo, bar, by="id").id.dtype.name
    assert idcoltype != "category"
    idcoltype = inner_join(bar, foo, by="id").id.dtype.name
    assert idcoltype != "category"

    df1 = tibble(x=1, y=factor("a"))
    df2 = tibble(x=2, y=factor("b"))
    out = full_join(df1, df2, by=["x", "y"])
    assert out.y.dtype.name == "category"

def test_when_keep_eqs_true_left_join_preserves_both_sets_of_keys():
    # test_that("when keep = TRUE, left_join() preserves both sets of keys", {

    # when keys have different names
    df1 = tibble(a=[2,3], b=[1,2])
    df2 = tibble(x=[3,4], y=[3,4])
    out = left_join(df1, df2, by={"a": "x"}, keep=True)

    assert out.a.tolist() == [2,3]
    assert out.x.fillna(0).tolist() == [0,3]

    # when keys have same name
    df1 = tibble(a = c(2, 3), b = c(1, 2))
    df2 = tibble(a = c(3, 4), y = c(3, 4))
    out = left_join(df1, df2, by = c("a"), keep = TRUE)

    assert out['a_x'].tolist() == [2,3]
    assert out['a_y'].fillna(0).tolist() == [0, 3]

def test_when_keep_eqs_true_right_join_preserves_both_sets_of_keys():
    # test_that("when keep = TRUE, right_join() preserves both sets of keys", {

    # when keys have different names
    df1 = tibble(a = c(2, 3), b = c(1, 2))
    df2 = tibble(x = c(3, 4), y = c(3, 4))
    out = right_join(df1, df2, by = dict(a="x"), keep = TRUE)
    assert out.a.fillna(0).tolist() == [3, 0]
    assert out.x.tolist() == [3, 4]

    # when keys have same name
    df1 = tibble(a = c(2, 3), b = c(1, 2))
    df2 = tibble(a = c(3, 4), y = c(3, 4))
    out = right_join(df1, df2, by = c("a"), keep = TRUE)
    assert out['a_x'].fillna(0).tolist() == [3, 0]
    assert out['a_y'].tolist() == [3,4]

def test_when_keep_eqs_true_full_join_preserves_both_sets_of_keys():
    # test_that("when keep = TRUE, full_join() preserves both sets of keys", {
    # when keys have different names
    df1 = tibble(a = c(2, 3), b = c(1, 2))
    df2 = tibble(x = c(3, 4), y = c(3, 4))
    out = full_join(df1, df2, by = dict(a="x"), keep = TRUE)
    assert out.a.fillna(0).tolist() == [2,3,0]
    assert out.x.fillna(0).tolist() == [0,3,4]

    # when keys have same name
    df1 = tibble(a = c(2, 3), b = c(1, 2))
    df2 = tibble(a = c(3, 4), y = c(3, 4))
    out = full_join(df1, df2, by = c("a"), keep = TRUE)
    assert out['a_x'].fillna(0).tolist() == [2,3,0]
    assert out['a_y'].fillna(0).tolist() == [0,3,4]

def test_when_keep_eqs_true_inner_join_preserves_both_sets_of_keys():
    # test_that("when keep = TRUE, inner_join() preserves both sets of keys (#5581)", {
    # when keys have different names
    df1 = tibble(a = c(2, 3), b = c(1, 2))
    df2 = tibble(x = c(3, 4), y = c(3, 4))
    out = inner_join(df1, df2, by = dict(a = "x"), keep = TRUE)
    assert out.a.tolist() == [3]
    assert out.x.tolist() == [3]

    # when keys have same name
    df1 = tibble(a = c(2, 3), b = c(1, 2))
    df2 = tibble(a = c(3, 4), y = c(3, 4))
    out = inner_join(df1, df2, by = c("a"), keep = TRUE)
    assert out['a_x'].tolist() == [3]
    assert out['a_y'].tolist() == [3]

def test_joins_matches_nas_by_default():
    # test_that("joins matches NAs by default (#892, #2033)", {
    df1 = tibble(x = c(None, 1))
    df2 = tibble(x = c(None, 2))

    assert nrow(inner_join(df1, df2, by=f.x)) == 1
    assert nrow(semi_join(df1, df2, by=f.x)) == 1

# def test_joins_donot_match_na_when_na_matches_is_never():
#     # test_that("joins don't match NA when na_matches = 'never' (#2033)", {
#     df1 = tibble(a = c(1, NA))
#     df2 = tibble(a = c(1, NA), b = [1,2])

#     out = left_join(df1, df2, by = "a", na_matches = "never")
#     assert out.equals(tibble(a=[1,NA], b=[1,NA]))

#     out = inner_join(df1, df2, by = "a", na_matches = "never")
#     assert out.equals(tibble(a=1, b=1))

#     out = semi_join(df1, df2, by = "a", na_matches = "never")
#     assert out.equals(tibble(a=1))

#     out = anti_join(df1, df2, by = "a", na_matches = "never")
#     assert out.equals(tibble(a=None))

#     dat1 = tibble(
#         name = c("a", "c"),
#         var1 = c(1, 2)
#     )
#     dat3 = tibble(
#         name = c("a", None),
#         var3 = c(5, 6)
#     )
#     out = full_join(dat1, dat3, by=f.name, na_matches="never")
#     exp = tibble(
#         name=c("a", "c", NA),
#         var1 = c(1, 2, NA),
#         var3 = c(5, NA, 6)
#     )
#     assert out.equals(exp)

# nest_join ---------------------------------------------------------------

def test_nest_join_returns_list_of_dfs():
    df1 = tibble(x = c(1, 2), y = c(2, 3))
    df2 = tibble(x = c(1, 1), z = c(2, 3))
    out = nest_join(df1, df2, by = "x")

    assert out.columns.tolist() == ["x", "y", "df2"]
    df2list = out.df2.tolist()
    assert isinstance(df2list[0], DataFrame)

def test_nest_join_handles_multiple_matches_in_x():
    # test_that("nest_join handles multiple matches in x (#3642)", {
    df1 = tibble(x = c(1, 1))
    df2 = tibble(x = 1, y = [1,2])
    out = nest_join(df1, df2, by="x")
    assert out.df2.values[0].equals(out.df2.values[1])

def test_y_keys_dropped_by_default():
    df1 = tibble(x = c(1, 2), y = c(2, 3))
    df2 = tibble(x = c(1, 1), z = c(2, 3))
    out = nest_join(df1, df2, by = "x")
    assert out.columns.tolist() == ["x", "y", "df2"]
    assert out.df2.values[0].columns.tolist() == ["z"]

    out = nest_join(df1, df2, by = "x", keep = TRUE)
    assert out.df2.values[0].columns.tolist() == ["x", "z"]

# output type ---------------------------------------------------------------

# We only have DataFrame
# test_that("joins x preserve type of x", {
#   df1 <- data.frame(x = 1)
#   df2 <- tibble(x = 2)

#   expect_s3_class(inner_join(df1, df2, by = "x"), "data.frame", exact = TRUE)
#   expect_s3_class(inner_join(df2, df1, by = "x"), "tbl_df")
# })

def test_joins_preserve_groups():

    gf1 = tibble(a = [1,2,3]) >> group_by(f.a)
    gf2 = tibble(a = rep([1,2,3,4], 2), b = 1) >> group_by(f.b)

    out = inner_join(gf1, gf2, by="a")
    assert group_vars(out) == ["a"]

    out = semi_join(gf1, gf2, by="a")
    assert group_vars(out) == ["a"]

    # See comment in nest_join
    out = nest_join(gf1, gf2, by="a")
    assert group_vars(out) == ["a"]

def test_group_column_names_reflect_renamed_duplicate_columns():
    # test_that("group column names reflect renamed duplicate columns (#2330)", {
    df1 = tibble(x = range(1,6), y = range(1,6)) >> group_by(f.x, f.y)
    df2 = tibble(x = range(1,6), y = range(1,6))

    out = inner_join(df1, df2, by = "x")
    assert group_vars(out) == ["x"]
    # dplyr todo: fix this issue: https://github.com/tidyverse/dplyr/issues/4917
    # expect_equal(group_vars(out), c("x", "y.x"))

def test_rowwise_group_structure_is_updated_after_a_join():
    # test_that("rowwise group structure is updated after a join (#5227)", {
    df1 = rowwise(tibble(x = [1,2]))
    df2 = tibble(x = c([1,2], 2))

    x = left_join(df1, df2, by = "x")
    assert group_rows(x) == [[0],[1],[2]]

def test_join_by_dict_not_keep():
    df1 = tibble(x=[1,2])
    df2 = tibble(y=[1,2])

    out = left_join(df1, df2, by=dict(x="y"))
    assert out.equals(df1)

def test_nest_join_by_multiple():
    df1 = tibble(x=[1,2], y=[3,4])
    df2 = tibble(x=[1,2], y=[3,4], z=[5,6])
    out = nest_join(df1, df2, by=['x', 'y'])
    assert out.df2.values[0].equals(tibble(z=5))
    assert out.df2.values[1].equals(tibble(z=6))

    out = nest_join(df1, df2, copy=True)
    assert out.df2.values[0].equals(tibble(z=5))
    assert out.df2.values[1].equals(tibble(z=6))

def test_join_by_none():
    df1 = tibble(x=[1,2,3], y=[3,4,5])
    df2 = tibble(x=[2,3,4], z=[5,6,7])
    out = inner_join(df1, df2, keep=True)

    assert_frame_equal(out, tibble(
        x_x=[2,3],
        y=[4,5],
        x_y=[2,3],
        z=[5,6]
    ))

    out = inner_join(df1, df2, keep=False)
    assert_frame_equal(out, tibble(
        x=[2,3],
        y=[4,5],
        z=[5,6]
    ))
