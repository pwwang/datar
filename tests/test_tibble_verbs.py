# https://github.com/tidyverse/tibble/blob/master/tests/testthat/test-tibble.R

from datar.dplyr.group_data import group_vars
from datar.core.grouped import DataFrameGroupBy, DataFrameRowwise
from datar.base.verbs import ncol
import pandas
from pandas.testing import assert_frame_equal
from datar.base import as_character, c, levels, rev, seq_along
import pytest

from datar import f
from datar.core.exceptions import ColumnNotExistingError, NameNonUniqueError, DataUnrecyclable
from datar.tibble import *
from datar.base import nrow, rep, dim, sum, diag, NA, letters, LETTERS, NULL, seq, factor, rownames, seq_len
from datar.dplyr import pull, mutate, group_by, rowwise
from datar.datasets import iris, mtcars
from .conftest import assert_iterable_equal

# enframe ------------------------------------------

def test_can_convert_list():
    df = enframe(seq(3,1))
    assert df.equals(tibble(name=seq(1,3), value=seq(3,1)))
    # scalar
    df = enframe(1)
    assert df.equals(tibble(name=1, value=1))

def test_can_convert_dict():
    df = enframe(dict(a=2, b=1))
    assert df.equals(tibble(name=['a','b'], value=[2,1]))
    df = enframe(dict(a=2, b=1), name=None)
    assert df.equals(tibble(value=[2,1]))

def test_can_convert_empty():
    df = enframe([])
    assert df.shape == (0, 2)
    assert df.columns.tolist() == ['name', 'value']

    df = enframe(None)
    assert df.shape == (0, 2)
    assert df.columns.tolist() == ['name', 'value']

def test_can_use_custom_names():
    df = enframe(letters, name="index", value="letter")
    assert df.equals(tibble(index=seq_along(letters), letter=letters))

def test_can_enframe_without_names():
    df = enframe(letters, name=None, value="letter")
    assert df.equals(tibble(letter=letters))

def test_cannot_value_none():
    with pytest.raises(ValueError):
        enframe(letters, value=None)

def test_cannot_pass_with_dimensions():
    with pytest.raises(ValueError):
        enframe(iris)

# deframe -----------------------------------------------------------------

def test_can_deframe_2col_data_frame():
    out = deframe(tibble(name = letters[:3], value = seq(3,1)))
    assert out == {"a": 3, "b": 2, "c":1}

def test_can_deframe_1col_data_frame():
    out = deframe(tibble(value=seq(3,1)))
    assert out.tolist() == [3,2,1]

def test_can_deframe_3col_df_with_warning(caplog):
    out = deframe(tibble(name=letters[:3], value=seq(3,1), oops=[1,2,3]))
    assert out == {"a": 3, "b": 2, "c":1}
    assert "one- or two-column" in caplog.text

# add_row -------------------------------------------------------------

df_all = tibble(
    a=[1,2.5,NA],
    b=[1,2,NA],
    c=[True, False, NA],
    d=["a", "b", NA],
    e=factor(c("a","b", NA))
)
def test_can_add_row():
    df_all_new = add_row(df_all, a=4, b=3)
    assert df_all_new.columns.tolist() == df_all.columns.tolist()
    assert nrow(df_all_new) == nrow(df_all) + 1
    assert_iterable_equal(df_all_new.a, [1.0,2.5,NA, 4])
    assert_iterable_equal(df_all_new.b, [1.0,2.0,NA, 3.0])
    assert_iterable_equal(df_all_new.c, [True,False,NA, NA])

def test_add_empty_row_if_no_arguments():
    iris1 = add_row(iris)
    assert nrow(iris1) == nrow(iris) + 1
    new_iris_row = iris1.iloc[-1, :]
    assert all(pandas.isna(new_iris_row))

def test_error_if_adding_row_with_unknown_variables():
    with pytest.raises(ValueError, match="xxyzy"):
        add_row(tibble(a=3), xxyzy="err")

    with pytest.raises(ValueError):
        add_row(tibble(a=3), b="err", c="oops")

def test_add_rows_to_nondf():
    # with pytest.raises(NotImplementedError):
    out = add_row(1)
    assert hasattr(out, '_pipda_eval')

def test_can_add_multiple_rows():
    df = tibble(a=3)
    df_new = add_row(df, a=[4,5])
    assert df_new.equals(tibble(a=[3,4,5]))

def test_can_recycle_when_adding_rows():
    iris_new = add_row(iris, Sepal_Length=[-1,-2], Species="unknown")
    assert nrow(iris_new) == nrow(iris) + 2
    assert_iterable_equal(iris_new.Sepal_Length, iris.Sepal_Length.tolist() + [-1, -2])
    assert_iterable_equal(iris_new.Species, iris.Species.tolist() + ["unknown"] * 2)

def test_can_add_as_first_row_via_before_1():
    df = tibble(a=3)
    df_new = add_row(df, a=2, _before=1)
    assert df_new.equals(tibble(a=[2,3]))

def test_can_add_as_first_row_via_after_0():
    df = tibble(a=3)
    df_new = add_row(df, a=2, _after=0)
    assert df_new.equals(tibble(a=[2,3]))

def test_can_add_row_inbetween():
    df = tibble(a=[1,2,3])
    df_new = add_row(df, a=[4,5], _after=2)
    assert df_new.equals(tibble(a=[1,2,4,5,3]))

def test_can_safely_add_to_factor_columns_everywhere():
    # test_that("can safely add to factor columns everywhere (#296)", {
    df = tibble(a=factor(letters[:3]))
    out = add_row(df)
    exp = tibble(a=factor(c(letters[:3], NA)))
    assert_frame_equal(out, exp)

    out = add_row(df, _before=1)
    exp = tibble(a = factor(c(NA, letters[:3])))
    assert_frame_equal(out, exp)

    out = add_row(df, _before=2)
    exp = tibble(a = factor(c("a", NA, letters[1:3])))
    assert_frame_equal(out, exp)

    out = add_row(df, a="d")
    exp = tibble(a = letters[:4], _dtypes=object)
    assert_frame_equal(out, exp)

    out = add_row(df, a="d", _before=1)
    exp = tibble(a = c("d", letters[:3]), _dtypes=object)
    assert_frame_equal(out, exp)

    out = add_row(df, a="d", _before=2)
    exp = tibble(a = list("adbc"), _dtypes=object)
    assert_frame_equal(out, exp)

def test_error_if_both_before_and_after_are_given():
    df = tibble(a=seq(1,3))
    with pytest.raises(ValueError):
        add_row(df, a=[4,5], _after=2, _before=3)

def test_missing_row_names_stay_missing_when_adding_row():
    assert not has_rownames(iris)
    assert not has_rownames(add_row(iris, Species="unknown", _after=0))
    assert not has_rownames(add_row(iris, Species="unknown", _after=nrow(iris)))
    assert not has_rownames(add_row(iris, Species="unknown", _after=10))

# test_that("adding to a list column adds a NULL value (#148)", {
#   expect_null(add_row(tibble(a = as.list(1:3)))$a[[4]])
#   expect_null(add_row(tibble(a = as.list(1:3)), .before = 1)$a[[1]])
#   expect_null(add_row(tibble(a = as.list(1:3)), .after = 1)$a[[2]])
#   expect_null(add_row(tibble(a = as.list(1:3), b = 1:3), b = 4:6)$a[[5]])
# })

# test_that("add_row() keeps the class of empty columns", {
#   new_tibble <- add_row(df_empty, to_be_added = 5)
#   expect_equal(sapply(df_empty, class), sapply(new_tibble, class))
# })

def test_add_row_fails_nicely_for_grouped_df():
    gf = group_by(iris, f.Species)
    with pytest.raises(ValueError):
        add_row(gf, Petal_Width=3)

def test_add_row_works_when_add_0row_input():
    # test_that("add_row() works when adding zero row input (#809)", {
    x = tibble(x=1, y=2)
    y = tibble(y=[])

    out = add_row(x, x=[])
    assert_frame_equal(out, x)

    out = add_row(x, y)
    assert_frame_equal(out, x)

    out = add_row(x, NULL)
    assert_frame_equal(out, x)

    out = add_row(x, None)
    assert_frame_equal(out, x)

# add_column ------------------------------------------------------------

def test_can_add_new_column():
    df_all_new = add_column(df_all, j=seq(1,3), k=seq(3,1))
    assert nrow(df_all_new) == nrow(df_all)
    assert_frame_equal(df_all_new.iloc[:, :ncol(df_all)], df_all)
    assert_iterable_equal(df_all_new.j, [1,2,3])
    assert_iterable_equal(df_all_new.k, [3,2,1])

def test_add_column_works_with_0col_tibbles():
    # test_that("add_column() works with 0-col tibbles (#786)", {
    out = add_column(tibble(_rows=1), a=1)
    assert_frame_equal(out, tibble(a=1))

# test_that("add_column() keeps class of object", {
#   iris_new <- add_column(iris, x = 1:150)
#   expect_equal(class(iris), class(iris_new))

#   iris_new <- add_column(as_tibble(iris), x = 1:150)
#   expect_equal(class(as_tibble(iris)), class(iris_new))
# })

# test_that("add_column() keeps class of object when adding in the middle", {
#   iris_new <- add_column(iris, x = 1:150, .after = 3)
#   expect_equal(class(iris), class(iris_new))

#   iris_new <- add_column(as_tibble(iris), x = 1:150)
#   expect_equal(class(as_tibble(iris)), class(iris_new))
# })

# test_that("add_column() keeps class of object when adding in the beginning", {
#   iris_new <- add_column(iris, x = 1:150, .after = 0)
#   expect_equal(class(iris), class(iris_new))

#   iris_new <- add_column(as_tibble(iris), x = 1:150)
#   expect_equal(class(as_tibble(iris)), class(iris_new))
# })

def test_add_column_keeps_unchanged_if_no_arguments():
    assert_frame_equal(iris, add_column(iris))

def test_add_column_can_add_to_empty_tibble_or_df():
    out = add_column(tibble(_rows=3), a=seq(1,3))
    assert_frame_equal(out, tibble(a=seq(1,3)))

def test_error_if_add_existing_columns():
    with pytest.raises(NameNonUniqueError):
        add_column(tibble(a=3), a=5)

def test_error_if_adding_wrong_number_of_rows_with_add_column():
    with pytest.raises(DataUnrecyclable, match="new columns"):
        add_column(tibble(a=3), b=[4,5])

def test_can_add_multiple_columns():
    df = tibble(a=[1,2,3])
    df_new = add_column(df, b=[4,5,6], c=[3,2,1])
    assert_frame_equal(df_new, tibble(a=[1,2,3], b=[4,5,6], c=[3,2,1]))

def test_can_recycle_when_adding_columns():
    df = tibble(a=[1,2,3])
    df_new = add_column(df, b=4, c=[3,2,1])
    assert_frame_equal(df_new, tibble(a=[1,2,3], b=rep(4,3), c=[3,2,1]))

def test_can_recycle_when_adding_a_column_of_len1():
    df = tibble(a=[1,2,3])
    df_new = add_column(df, b=4)
    assert_frame_equal(df_new, tibble(a=[1,2,3], b=rep(4,3)))

def test_can_recycle_when_add_multiple_columns_of_len1():
    df = tibble(a=[1,2,3])
    df_new = add_column(df, b=4, c=5)
    assert_frame_equal(df_new, tibble(a=[1,2,3], b=rep(4,3), c=rep(5,3)))

def test_can_recycle_for_0row_df():
    # test_that("can recyle for zero-row data frame (#167)", {
    df = tibble(a=[1.,2,3]).iloc[[], :]
    df_new = add_column(df, b=4., c=[])
    assert_frame_equal(df_new, tibble(
        a=[], b=[], c=[]
    ))

def test_can_add_as_first_column_via_before_1():
    df = tibble(a=3)
    df_new = add_column(df, b=2, _before=1)
    assert_frame_equal(df_new, tibble(b=2, a=3))

def test_can_add_as_first_column_via_after_0():
    df = tibble(a=3)
    df_new = add_column(df, b=2, _after=0)
    assert_frame_equal(df_new, tibble(b=2, a=3))

def test_can_add_column_inbetween():
    df = tibble(a=[1,2,3], c=[4,5,6])
    df_new = add_column(df, b=seq(-1,1), _after=1)
    assert_frame_equal(df_new, tibble(a=[1,2,3], b=seq(-1,1), c=[4,5,6]))

def test_can_add_column_relative_to_named_column():
    df = tibble(a=seq(1,3), c=seq(4,6))
    df_new = add_column(df, b=seq(-1,1), _before=f.c)
    assert_frame_equal(df_new, tibble(a=[1,2,3], b=seq(-1,1), c=[4,5,6]))

def test_error_if_both_before_after_are_given():
    df = tibble(a=[1,2,3])
    with pytest.raises(ValueError):
        add_column(df, b=[4,5,6], _after=2, _before=3)

def test_error_if_column_named_by_before_or_after_not_found():
    df = tibble(a=[1,2,3])
    with pytest.raises(ColumnNotExistingError):
        add_column(df, b=[4,5,6], _after='x')
    with pytest.raises(ColumnNotExistingError):
        add_column(df, b=[4,5,6], _before='x')

# test_that("deprecated adding columns to non-data-frames", {
#   expect_error(
#     # Two lifecycle warnings, requires testthat > 2.3.2:
#     suppressWarnings(
#       expect_warning(add_column(as.matrix(mtcars), x = 1))
#     )
#   )
# })

def test_missing_row_names_stay_missing_when_adding_column():
    assert not has_rownames(iris)
    assert not has_rownames(add_column(iris, x=seq(1,150), _after=0))
    assert not has_rownames(add_column(iris, x=seq(1,150), _after=ncol(iris)))
    assert not has_rownames(add_column(iris, x=seq(1,150), _before=2))

def test_errors_of_add_row_and_add_column():
    with pytest.raises(ValueError):
        add_row(tibble(), a=1)
    with pytest.raises(ValueError):
        add_row(tibble(), a=1, b=2)
    with pytest.raises(ValueError):
        add_row(tibble(), **dict(zip(letters, letters)))
    with pytest.raises(ValueError):
        add_row(group_by(tibble(a=1), f.a))
    with pytest.raises(ValueError):
        add_row(tibble(a=1), a=2, _before=1, _after=1)

    with pytest.raises(NameNonUniqueError):
        add_column(tibble(a=1), a=1)
    with pytest.raises(NameNonUniqueError):
        add_column(tibble(a=1, b=2), a=1, b=2)
    with pytest.raises(NameNonUniqueError):
        add_column(tibble(dict(zip(letters, letters))), **dict(zip(letters, letters)))
    with pytest.raises(DataUnrecyclable):
        add_column(tibble(a=[2,3]), b=[4,5,6])
    with pytest.raises(ValueError):
        add_column(tibble(a=1), b=1, _before=1, _after=1)

# rownames -------------------------------------------------------

def test_has_rownames_and_remove_rownames():
    assert not has_rownames(iris)
    assert has_rownames(mtcars)
    assert not has_rownames(remove_rownames(mtcars))
    assert not has_rownames(remove_rownames(iris))
    # assert not has_rownames(seq(1,10)) # not supported for other types

# test_that("setting row names on a tibble raises a warning", {
#   mtcars2 <- as_tibble(mtcars)
#   expect_false(has_rownames(mtcars2))

#   expect_warning(
#     rownames(mtcars2) <- rownames(mtcars),
#     "deprecated",
#     fixed = TRUE
#   )
# })

def test_rownames_to_column():
    # test_that("rownames_to_column keeps the tbl classes (#882)", {
    res = rownames_to_column(mtcars)
    assert not has_rownames(res)
    assert_iterable_equal(res.rowname, rownames(mtcars))
    with pytest.raises(ValueError, match="duplicated"):
        rownames_to_column(mtcars, f.wt)

    res1 = rownames_to_column(mtcars, "MakeModel")
    assert not has_rownames(res1)
    assert_iterable_equal(res1.MakeModel, rownames(mtcars))
    with pytest.raises(ValueError, match="duplicated"):
        rownames_to_column(res1, f.wt)

def test_rowid_to_column():
    # test_that("rowid_to_column keeps the tbl classes", {
    res = rowid_to_column(mtcars)
    assert not has_rownames(res)
    assert_iterable_equal(res.rowid, seq_len(nrow(mtcars)))
    with pytest.raises(ValueError, match="duplicated"):
        rowid_to_column(mtcars, f.wt)

    res1 = rowid_to_column(mtcars, "row_id")
    assert not has_rownames(res1)
    assert_iterable_equal(res1.row_id, seq_len(nrow(mtcars)))
    with pytest.raises(ValueError, match="duplicated"):
        rowid_to_column(res1, f.wt)

def test_column_to_rownames(caplog):
    var = "var"
    assert has_rownames(mtcars)
    res0 = rownames_to_column(mtcars, var)
    res = column_to_rownames(res0, var)
    assert caplog.text == ''
    assert has_rownames(res)
    assert rownames(res) == rownames(mtcars)
    assert_frame_equal(res, mtcars)
    # has_name is not a public API
    #   expect_false(has_name(res, var))

    mtcars1 = mtcars.copy()
    mtcars1['num'] = rev(seq_len(nrow(mtcars), base0_=True))
    res0 = rownames_to_column(mtcars1)
    res = column_to_rownames(res0, var="num")
    assert caplog.text == ''
    assert has_rownames(res)
    assert_iterable_equal(rownames(res), as_character(mtcars1.num))
    with pytest.raises(ValueError):
        column_to_rownames(res)
    with pytest.raises(ColumnNotExistingError):
        column_to_rownames(rownames_to_column(mtcars1, var), "num2")

# test_that("converting to data frame does not add row names", {
#   expect_false(has_rownames(as.data.frame(as_tibble(iris))))
# })

# test_that("work around structure() bug (#852)", {
#   expect_false(has_rownames(structure(trees, .drop = FALSE)))
# })

def test_rownames_errors():
    with pytest.raises(ValueError):
        rownames_to_column(mtcars, f.cyl)
    with pytest.raises(ValueError):
        rowid_to_column(iris, "Species")
    with pytest.raises(ValueError):
        column_to_rownames(mtcars, "cyl")
    with pytest.raises(ColumnNotExistingError):
        column_to_rownames(iris, "foo")

# add_row for rowwise df
def test_add_row_for_rowwise_df():
    df = tibble(x=[1,2,3]) >> rowwise()
    df2 = add_row(df, x=4)
    assert isinstance(df2, DataFrameRowwise)
    assert_iterable_equal(df2.x, [1,2,3,4])

# add_column for rowwise df
def test_add_column_for_rowwise_df():
    df = tibble(x=[1,2,3]) >> group_by(f.x)
    df2 = add_column(df, y=[3,2,1])
    assert isinstance(df2, DataFrameGroupBy)
    assert group_vars(df2) == ['x']
