# tests grabbed from:
# https://github.com/tidyverse/tidyr/blob/master/tests/testthat/test-nest.R
import pytest
from pandas.api.types import is_categorical_dtype
from pandas.testing import assert_frame_equal
from datar.all import *
from datar.core.tibble import TibbleGrouped, TibbleRowwise
from ..conftest import assert_iterable_equal

# nest --------------------------------------------------------------------
def test_nest_turns_grouped_values_into_one_list_df():
    df = tibble(x=[1,1,1], y=f[1:4])
    out = nest(df, data=f.y)
    assert len(out.x) == 1
    assert len(out.data) == 1
    assert_frame_equal(out.data.values[0], tibble(y=f[1:4]))

def test_nest_uses_grouping_vars_if_present():
    df = tibble(x=[1,1,1], y=f[1:4])
    out = nest(group_by(df, f.x))
    assert group_vars(out) == ['x']
    assert_frame_equal(out.data.obj.values[0], tibble(y=f[1:4]))

def test_nest_provides_grouping_vars_override_grouped_defaults():
    df = tibble(x=1, y=2, z=3) >> group_by(f.x)
    out = nest(df, data=f.y)
    assert isinstance(out, TibbleGrouped)
    assert out.columns.tolist() == ['x', 'z', 'data']
    assert out.data.obj.values[0].columns.tolist() == ['y']

def test_nest_puts_data_into_correct_row():
    df = tibble(x = f[1:4], y = c("B", "A", "A"))
    out = df >> nest(data = f.x) >> filter(f.y == "B")
    assert len(out.data) == 1
    assert out.data.values[0].x.tolist() == [1]

def test_nest_everyting_returns_a_simple_df():
    df = tibble(x=f[1:4], y=['B', 'A', 'A'])
    out = nest(df, data=c(f.x, f.y))
    assert len(out.data) == 1
    assert_frame_equal(out.data.values[0], df)

def test_nest_preserves_order_of_data():
    df = tibble(x=[1,3,2,3,2], y=f[1:6])
    out = nest(df, data=f.y)
    assert out.x.tolist() == [1,3,2]

def test_nest_can_strip_names():
    df = tibble(x = c(1, 1, 1), ya = f[1:4], yb = f[4:7])
    out = df >> nest(y = starts_with("y"), _names_sep = "")
    assert out.y.values[0].columns.tolist() == ['a', 'b']

def test_nest_names_sep():
    df = tibble(x = c(1, 1, 1), y_a = f[1:4], y_b = f[4:7])
    out = df >> nest(y = starts_with("y"), _names_sep = "_")
    assert out.y.values[0].columns.tolist() == ['a', 'b']

def test_empty_factor_levels_dont_affect_nest():
    df = tibble(
        x = factor(c("z", "a"), levels=letters),
        y = f[1:3]
    )
    out = nest(df, data=f.y)
    assert out.x.eq(df.x).all()

def test_nest_works_with_empty_df():
    df = tibble(x=[], y=[])
    out = nest(df, data=f.x)
    assert out.columns.tolist() == ['y', 'data']
    assert nrow(out) == 0

    out = nest(df, data=c(f.x, f.y))
    assert out.columns.tolist() == ['data']
    assert nrow(out) == 0

# test_that("tibble conversion occurs in the `nest.data.frame()` method", {
#   df <- data.frame(x = 1, y = 1:2)
#   out <- df %>% nest(data = y)
#   expect_s3_class(out, "tbl_df")
#   expect_s3_class(out$data[[1L]], "tbl_df")
# })

def test_can_nest_multiple_columns():
    df = tibble(x = 1, a1 = 1, a2 = 2, b1 = 1, b2 = 2)
    out = df >> nest(a=c(f.a1, f.a2), b=c(f.b1, f.b2))

    assert out.columns.tolist() == ['x', 'a', 'b']
    assert_frame_equal(out.a.values[0], df[['a1', 'a2']])
    assert_frame_equal(out.b.values[0], df[['b1', 'b2']])

def test_nest_no_columns_error():
    # warning for no columns will be changed to error here.
    df = tibble(x = 1, a1 = 1, a2 = 2, b1 = 1, b2 = 2)
    with pytest.raises(ValueError, match="must not be empty"):
        nest(df)

# test_that("nesting no columns nests all inputs", {
#   # included only for backward compatibility
#   df <- tibble(a1 = 1, a2 = 2, b1 = 1, b2 = 2)
#   expect_warning(out <- nest(df), "must not be empty")
#   expect_named(out, "data")
#   expect_equal(out$data[[1]], df)
# })

# unnest ------------------------------------------------------------------

def test_unnest_keep_empty_rows():
    df = tibble(x=f[1:4], y = [NULL, tibble(), tibble(a=1)])
    out1 = df >> unnest(f.y)
    assert nrow(out1) == 1

    out2 = df >> unnest(f.y, keep_empty=True)
    assert nrow(out2) == 3
    assert_iterable_equal(out2.a, [NA, NA, 1])

## problem with NAs (numpy.nan), which is a float type
# test_that("empty rows still affect output type", {
#   df <- tibble(
#     x = 1:2,
#     data = list(
#       tibble(y = character(0)),
#       tibble(z = integer(0))
#     )
#   )
#   out <- unnest(df, data)
#   expect_equal(out, tibble(x = integer(), y = character(), z = integer()))
# })

def test_unnest_bad_inputs_error():
    df = tibble(x=1, y=[mean])
    out = unnest(df, f.y)
    ## able to do it
    assert nrow(out) == 1
    # with pytest.raises(ValueError):
    #   unnest(df, f.y)

def test_unnest_combines_augmented_vectors():
    df = tibble(x=[factor(letters[:3])])
    out = unnest(df, f.x)
    assert is_categorical_dtype(out.x)
    assert_iterable_equal(out.x, letters[:3])

def test_unnest_vector_unnest_preserves_names():
    df = tibble(x=[1, [2,3]], y=["a", ["b", "c"]])
    out = unnest(df, f.x)
    assert out.columns.tolist() == ['x', 'y']

def test_unnest_rows_and_cols_of_nested_dfs_are_expanded():
    df = tibble(x = f[1:3], y = [tibble(a = 1), tibble(b = f[1:3])])
    out = df >> unnest(f.y)

    assert out.columns.tolist() == ['x', 'a', 'b']
    assert nrow(out) == 3

def test_unnest_nested_lists():
    df = tibble(x=f[1:3], y=[[["a"]], [["b"]]])
    rs = unnest(df, f.y)
    assert_frame_equal(rs, tibble(x=f[1:3], y=[["a"], ["b"]]))

def test_can_unnest_mixture_of_named_and_unnamed():
    df = tibble(
        x="a",
        y=[tibble(y=f[1:3])],
        z=[[1,2]]
    )
    out = unnest(df, c(f.y, f.z))
    assert_frame_equal(out, tibble(x=["a","a"], y=f[1:3], z=f[1:3]))

def test_can_unnest_lists():
    df = tibble(x=f[1:3], y=[seq(1,3), seq(4,9)])
    out = unnest(df, f.y)
    assert_frame_equal(out, tibble(x=rep([1,2], [3,6]), y=f[1:10]))

def test_unnest_can_combine_null_with_vectors_or_dfs():
    df1 = tibble(x=f[1:3], y=[NULL, tibble(z=1)])
    out = unnest(df1, f.y)
    assert out.columns.tolist() == ['x', 'z']
    assert_iterable_equal(out.z, [1])

    df2 = tibble(x=f[1:3], y=[NULL, 1])
    out = unnest(df2, f.y)
    assert out.columns.tolist() == ['x', 'y']
    assert_iterable_equal(out.y, [1])

def test_unnest_vectors_become_columns():
    df = tibble(x=f[1:3], y=[1, [1,2]])
    out = unnest(df, f.y)
    assert_iterable_equal(out.y, [1,1,2])

def test_unnest_multiple_columns_must_be_same_length():
    df = tibble(x=[[1,2]], y=[[1,2,3]])
    with pytest.raises(ValueError, match="Incompatible lengths: 2, 3"):
        unnest(df, c(f.x, f.y))

    df = tibble(x=[[1,2]], y=[tibble(y=f[1:4])])
    with pytest.raises(ValueError, match="Incompatible lengths: 2, 3"):
        unnest(df, c(f.x, f.y))

def test_unnest_using_non_syntactic_names():
    out = tibble(foo_bar=[[1,2], 3])
    out.columns = ['foo bar']
    out = out >> unnest(f['foo bar'])
    assert out.columns.to_list() == ['foo bar']

def test_unnest_no_cols_error():
    with pytest.raises(ValueError):
        tibble(x=[]) >> unnest()

def test_unnest_list_of_empty_dfs():
    df = tibble(x=[1,2], y=[tibble(a=[]), tibble(b=[])])
    out = df >> unnest(f.y)
    assert dim(out) == (0, 3)
    assert out.columns.tolist() == ['x', 'a', 'b']

# other methods -----------------------------------------------------------------

def test_unnest_rowwise_df_becomes_grouped_df():
    df = tibble(g=1, x=[[1,2,3]]) >> rowwise(f.g)
    rs = df >> unnest(f.x)
    assert isinstance(rs, TibbleGrouped)
    assert not isinstance(rs, TibbleRowwise)
    assert group_vars(rs) == ['g']

    # but without grouping vars, it's a tibble
    df = tibble(g=1, x=[[1,2,3]]) >> rowwise()
    rs = df >> unnest(f.x)
    assert not isinstance(rs, TibbleGrouped)

def test_unnest_grouping_preserved():
    df = tibble(g=1, x=[[1,2,3]]) >> group_by(f.g)
    rs = df >> unnest(f.x)
    assert isinstance(rs, TibbleGrouped)
    assert not isinstance(rs, TibbleRowwise)
    assert group_vars(rs) == ['g']

# Empty inputs ------------------------------------------------------------

def test_unnest_empty_data_frame():
    df = tibble(x=[], y=[], _dtypes={'x': int})
    out = unnest(df, f.y)
    assert dim(out) == (0, 2)


## unable to do it due to NAs being float
# test_that("unnest() preserves dtypes", {
#   tbl <- tibble(x = integer(), y = list_of(dtypes = tibble(a = integer())))
#   res <- unnest(tbl, y)
#   expect_equal(res, tibble(x = integer(), a = integer()))
# })

## empty columns ([]) can be unnested
# test_that("errors on bad inputs", {
#   df <- tibble(x = integer(), y = list())
#   expect_error(unnest(df, x), "list of vectors")
# })

def test_unnest_keeps_list_cols():
    df = tibble(x=f[1:3], y=[[3], [4]], z=[5, [6,7]])
    out = df >> unnest(f.y)
    assert out.columns.tolist() == ['x', 'y', 'z']

# # Deprecated behaviours ---------------------------------------------------

# test_that("warn about old style interface", {
#   df <- tibble(x = c(1, 1, 1), y = 1:3)
#   expect_warning(out <- nest(df, y), "data = c(y)", fixed = TRUE)
#   expect_named(out, c("x", "data"))
# })

# test_that("can control output column name", {
#   df <- tibble(x = c(1, 1, 1), y = 1:3)
#   expect_warning(out <- nest(df, y, .key = "y"), "y = c(y)", fixed = TRUE)
#   expect_named(out, c("x", "y"))
# })

# test_that("can control output column name when nested", {
#   df <- dplyr::group_by(tibble(x = c(1, 1, 1), y = 1:3), x)
#   expect_warning(out <- nest(df, .key = "y"), "`.key`", fixed = TRUE)
#   expect_named(out, c("x", "y"))
# })

# test_that(".key gets warning with new interface", {
#   df <- tibble(x = c(1, 1, 1), y = 1:3)
#   expect_warning(out <- nest(df, y = y, .key = "y"), ".key", fixed = TRUE)
#   expect_named(df, c("x", "y"))
# })

# test_that("cols must go in cols", {
#   df <- tibble(x = list(3, 4), y = list("a", "b"))
#   expect_warning(unnest(df, x, y), "c(x, y)", fixed = TRUE)
# })

# test_that("need supply column names", {
#   df <- tibble(x = 1:2, y = list("a", "b"))
#   expect_warning(unnest(df), "c(y)", fixed = TRUE)
# })

# test_that("sep combines column names", {
#   df <- tibble(x = list(tibble(x = 1)), y = list(tibble(x = 1)))
#   out <- expect_warning(df %>% unnest(c(x, y), .sep = "_"), "names_sep")
#   expect_named(out, c("x_x", "y_x"))
# })

# test_that("unnest has mutate semantics", {
#   df <- tibble(x = 1:3, y = list(1, 2:3, 4))
#   out <- expect_warning(df %>% unnest(z = map(y, `+`, 1)), "mutate")
#   expect_equal(out$z, 2:5)
# })

# test_that(".drop and .preserve are deprecated", {
#   df <- tibble(x = list(3, 4), y = list("a", "b"))
#   expect_warning(df %>% unnest(x, .preserve = y), ".preserve")

#   df <- tibble(x = list(3, 4), y = list("a", "b"))
#   expect_warning(df %>% unnest(x, .drop = FALSE), ".drop")
# })

# test_that(".id creates vector of names for vector unnest", {
#   df <- tibble(x = 1:2, y = list(a = 1, b = 1:2))
#   out <- expect_warning(unnest(df, y, .id = "name"), "names")

#   expect_equal(out$name, c("a", "b", "b"))
# })
