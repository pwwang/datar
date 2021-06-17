# tests grabbed from:
# https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-mutate.r
from pipda.function import register_func
import pytest
from pandas.testing import assert_frame_equal
from pandas.core.frame import DataFrame
from datar.all import *
from datar.core.grouped import DataFrameGroupBy, DataFrameRowwise
from datar.core.exceptions import ColumnNotExistingError, DataUnrecyclable
from datar.datasets import mtcars, iris

def test_empty_mutate_returns_input():
    df = tibble(x=1)
    gf = group_by(df, f.x)

    out = mutate(df)
    assert out.equals(df)

    out = mutate(gf)
    assert out.equals(gf)
    assert isinstance(gf, DataFrameGroupBy)
    assert group_vars(out) == ['x']

def test_rownames_preserved():
    df = DataFrame(dict(x=[1,2]), index=['a', 'b'])
    df = mutate(df, y = 2)
    assert df.index.tolist() == ['a', 'b']

def test_applied_progressively():
    df = tibble(x=1)
    out = df >> mutate(y = f.x + 1, z = f.y + 1)
    assert out.equals(tibble(x=1, y=2, z=3))

    out = df >> mutate(y = f.x + 1, x = f.y + 1)
    assert out.equals(tibble(x=3, y=2))

    out = df >> mutate(x = 2, y = f.x)
    assert out.equals(tibble(x=2, y=2))

    df = tibble(x=1, y=2)
    out1 = df >> mutate(x2 = f.x, x3 = f.x2 + 1)
    out2 = df >> mutate(x2 = f.x + 0, x3 = f.x2 + 1)
    assert out1.equals(out2)

def test_length1_vectors_are_recycled():
    df = tibble(x=range(1,5))
    out = mutate(df, y=1)
    assert out.y.tolist() == [1,1,1,1]

    with pytest.raises(DataUnrecyclable, match="recycle"):
        mutate(df, y=[1,2])

def test_removes_vars_with_null():
    df = tibble(x=range(1,4), y=range(1,4))
    gf = group_by(df, f.x)

    out = df >> mutate(y=NULL)
    assert out.columns.tolist() == ['x']

    out = gf >> mutate(y=NULL)
    assert out.columns.tolist() == ['x']
    assert isinstance(out, DataFrameGroupBy)
    assert group_vars(out) == ['x']
    assert group_rows(out) == [[0], [1], [2]]

    # even if it doesn't exist
    out = df >> mutate(z=NULL)
    assert out.equals(df)

    out = df >> mutate({'z': 1}, z=NULL)
    assert out.equals(df)

    df = tibble(x = 1, y = 1)
    out = mutate(df, z=1, x=NULL, y=NULL)
    assert out.equals(tibble(z=1))

# unquoted values: not supported by python

# assignments don't overwrite variables: for sure, since we use f.x

def test_can_mutate_0col_dfs():
    df = tibble(_rows=2)
    out = mutate(df, x=1)
    assert out.equals(tibble(x=[1,1]))

# ...

def test_preserves_names():
    df = tibble(a=range(1,4))
    # note it's treated as data frame
    out1 = df >> mutate(b=dict(zip(letters[:3], [0,1,2])))
    out2 = df >> mutate(b=dict(zip(letters[:3], [[0],[1],[2]])))

    x = out1 >> pull(f.b)
    assert x.columns.tolist() == list('abc')
    x = out2 >> pull(f.b)
    assert x.columns.tolist() == list('abc')

def test_handles_data_frame_columns():
    df = tibble(a = c(1, 2, 3), b = c(2, 3, 4), base_col = c(3, 4, 5))
    res = mutate(df, new_col=tibble(x=[1,2,3]))
    out = res >> pull(f.new_col)
    assert out.equals(tibble(x=[1,2,3]))

    tibble_func = register_func(None)(tibble)

    res = mutate(group_by(df, f.a), new_col=tibble_func(x=f.a))
    out = res >> pull(f.new_col)
    assert out.equals(tibble(x=[1,2,3]))

    rf = rowwise(df, f.a)
    res = mutate(rf, new_col=fibble(x=f.a))
    out = res >> pull(f.new_col)
    assert out.equals(tibble(x=[1,2,3]))

def test_unnamed_data_frames_are_automatically_unspliced():
    out = tibble(a=1) >> mutate(tibble(b=2))
    assert out.equals(tibble(a=1, b=2))

    out = tibble(a=1) >> mutate(tibble(b=2), tibble(b=3))
    assert out.equals(tibble(a=1, b=3))

    out = tibble(a=1) >> mutate(tibble(b=2), c=f.b)
    assert out.equals(tibble(a=1, b=2, c=2))

def test_named_data_frames_are_packed():
    df = tibble(x=1)
    out = df >> mutate(y=tibble(a=df.x))
    exp = tibble(x=1, y=tibble(a=1))
    assert out.equals(exp)

# ...

# output types ------------------------------------------------------------
def test_preserves_grouping():
    gf = group_by(tibble(x = [1,2], y = 2), f.x)
    out = mutate(gf, x=1)
    assert group_vars(out) == ['x']
    assert nrow(group_data(out)) == 1

    out = mutate(gf, z=1)
    assert group_data(out).equals(group_data(gf))


def test_works_on_0row_grouped_data_frame():
    dat = tibble(a=[], b=[])
    res = dat >> group_by(f.b, _drop = FALSE) >> mutate(a2 = f.a * 2)
    assert isinstance(res, DataFrameGroupBy)
    assert res.a2.tolist() == []

def test_works_on_0row_rowwise_df():
    dat = tibble(a=[])
    res = dat >> rowwise() >> mutate(a2=f.a*2)

    assert isinstance(res, DataFrameRowwise)
    assert res.a2.tolist() == []

def test_works_on_empty_data_frames():
    df = tibble()
    res = df >> mutate()
    assert nrow(res) == 0
    assert len(res) == 0

    res = df >> mutate(x=[])
    assert res.columns.tolist() == ['x']
    assert nrow(res) == 0
    assert ncol(res) == 1

def test_handles_0row_rowwise():
    res = tibble(y=[]) >> rowwise() >> mutate(z=1)
    assert dim(res) == (0, 2)

def test_rowwise_mutate_as_expected():
    res = tibble(x=[1,2,3]) >> rowwise() >> mutate(y=if_else(f.x<2, NA, f.x))
    assert res.y.fillna(0).tolist() == [0, 2, 3]

# grouped mutate does not drop grouping attributes

def test_rowwise_list_data():
    test = rowwise(tibble(x=[1,2]))
    out = test >> mutate(a=[[1]]) >> mutate(b=[[f.a[cur_group_id()][0] + 1]])
    exp = test >> mutate(a=[[1]], b=[[f.a[cur_group_id()][0] + 1]])

    assert out.equals(exp)

# .before, .after, .keep ------------------------------------------------------
def test_keep_unused_keeps_variables_explicitly_mentioned():
    df = tibble(x=1, y=2)
    out = mutate(df, x1=f.x+1, y=f.y, _keep="unused")
    assert out.columns.tolist() == ["y", "x1"]

def test_keep_used_not_affected_by_across():
    df = tibble(x = 1, y = 2, z = 3, a = "a", b = "b", c = "c")
    out = df >> mutate(across(where(is_numeric), identity), _keep = "unused")
    assert out.columns.tolist() == df.columns.tolist()

def test_keep_used_keeps_variables_used_in_expressions():
    df = tibble(a = 1, b = 2, c = 3, x = 1, y = 2)
    out = mutate(df, xy = f.x + f.y, _keep = "used")
    assert out.columns.tolist() == ["x", "y", "xy"]

def test_keep_none_only_keeps_grouping_variables():
    df = tibble(x = 1, y = 2)
    gf = group_by(df, f.x)

    out = mutate(df, z = 1, _keep = "none")
    assert out.columns.tolist() == ['z']
    out = mutate(gf, z = 1, _keep = "none")
    assert out.columns.tolist() == ['x', 'z']

def test_keep_none_prefers_new_order():
    df = tibble(x = 1, y = 2)
    out = df >> mutate(y=1, x=2, _keep="none")
    assert out.columns.tolist() == ['y', 'x']

def test_can_use_before_and_after_to_control_column_position():
    df = tibble(x = 1, y = 2)
    # out = mutate(df, z=1)
    # assert out.columns.tolist() == ['x', 'y', 'z']
    # out = mutate(df, z=1, _before=1)
    # assert out.columns.tolist() == ['z', 'x', 'y']
    out = mutate(df, z=1, _after=1)
    assert out.columns.tolist() == ['x', 'z', 'y']

    df = tibble(x = 1, y = 2)
    out = mutate(df, x=1, _after=f.y)
    assert out.columns.tolist() == ['x', 'y']

def test_keep_always_retains_grouping_variables():
    df = tibble(x = 1, y = 2, z = 3) >> group_by(f.z)
    out = df >> mutate(a=f.x+1, _keep="none")
    exp = tibble(z=3, a=2) >> group_by(f.z)
    assert out.equals(exp)
    assert group_vars(out) == group_vars(exp)

    out = df >> mutate(a=f.x+1, _keep="all")
    exp = tibble(x=1, y=2, z=3, a=2) >> group_by(f.z)
    assert out.equals(exp)
    assert group_vars(out) == group_vars(exp)

    out = df >> mutate(a=f.x+1, _keep="used")
    exp = tibble(x=1, z=3, a=2) >> group_by(f.z)
    assert out.equals(exp)
    assert group_vars(out) == group_vars(exp)

    out = df >> mutate(a=f.x+1, _keep="unused")
    exp = tibble(y=2, z=3, a=2) >> group_by(f.z)
    assert out.equals(exp)
    assert group_vars(out) == group_vars(exp)

def test_deals_with_0_groups():
    df = tibble(x = []) >> group_by(f.x)
    out = mutate(df, y=f.x+1)
    exp = tibble(x=[], y=[]) >> group_by(f.x)
    assert out.equals(exp)
    assert group_vars(out) == group_vars(exp)

    out = mutate(df, y = max(f.x))
    assert out.shape == (0, 2)
    assert group_vars(out) == ['x']

def test_mutate_null_preserves_correct_all_vars():
    df = tibble(x = 1, y = 2) >> mutate(x = NULL, vars = cur_data_all()) >> pull(f.vars)
    exp = tibble(y=2)
    assert df.equals(exp)

# TODO:
# def test_mutate_casts_data_frame_results_to_common_type():
#     df = tibble(x = [1,2], g = [1,2]) >> group_by(f.g)
#     res = df >> mutate(if_else(f.g == 1, tibble(y=1), tibble(y=1, z=2)))
#     assert res.z.fillna(0.).tolist() == [0., 2.]

# TODO: test warning catching

def test_rowwise_empty_list_columns():
    res = tibble(a=[[]]) >> rowwise() >> mutate(n=lengths(f.a))
    # Different!
    # since [] is an element in row#0
    assert res.n.tolist() == [0]

# Error messages ----------------------------------------------------------
def test_errors():
    tbl = tibble(x = [1,2], y = [1,2])

    with pytest.raises(ColumnNotExistingError, match="y"):
        tbl >> mutate(y = NULL, a = sum(f.y))
    with pytest.raises(ColumnNotExistingError, match="y"):
        tbl >> group_by(f.x) >> mutate(y = NULL, a = sum(f.y))

    # Valid: function can be an object in pandas dataframe.
    tibble(x = 1) >> mutate(y = mean)

    # incompatible size
    with pytest.raises(DataUnrecyclable, match="size 4"):
        tibble(x = c(2, 2, 3, 3)) >> mutate(i = range(1,6))
    with pytest.raises(DataUnrecyclable, match="size 2"):
        tibble(x = c(2, 2, 3, 3)) >> group_by(f.x) >> mutate(i = range(1,6))
    with pytest.raises(DataUnrecyclable, match="size 1"):
        tibble(x = c(2, 3, 3)) >> group_by(f.x) >> mutate(i = range(1,6))
    with pytest.raises(DataUnrecyclable, match="size 1"):
        tibble(x = c(2, 2, 3, 3)) >> rowwise() >> mutate(i = range(1,6))
    with pytest.raises(DataUnrecyclable, match="size 10"):
        tibble(x = range(1,11)) >> mutate(y=range(11,21), z=[1,2,3])

# transmute -------------------------------------------------------------

def test_non_syntactic_grouping_variable_is_preserved():
    # test_that("non-syntactic grouping variable is preserved (#1138)", {
    df = tibble(**{'a b': 1}) >> group_by('a b') >> transmute()
    assert df.columns.tolist() == ['a b']

def test_transmutate_preserves_grouping():
    gf = group_by(tibble(x=[1,2], y=2), f.x)

    out = transmute(gf, x=1)
    assert group_vars(out) == ['x']
    assert nrow(group_data(out)) == 1

    out = transmute(gf, z=1)
    assert group_data(out).equals(group_data(gf))

# Empty transmutes -------------------------------------------------

def test_transmute_without_args_returns_grouping_vars():
    df = tibble(x=1, y=2)
    gf = group_by(df, f.x)

    out = df >> transmute()
    assert dim(out) == (1, 0)

    out = gf >> transmute()
    assert out.equals(tibble(x=1))

# transmute variables -----------------------------------------------

# test_that("transmute succeeds in presence of raw columns (#1803)", {
#   df <- tibble(a = 1:3, b = as.raw(1:3))
#   expect_identical(transmute(df, a), df["a"])
#   expect_identical(transmute(df, b), df["b"])
# })

def test_transmute_dont_match_vars_transmute_args():
    df = tibble(a=1)
    out = transmute(df, var=f.a)
    assert out.equals(tibble(var=1))
    out = transmute(df, exclude=f.a)
    assert out.equals(tibble(exclude=1))
    out = transmute(df, include=f.a)
    assert out.equals(tibble(include=1))

def test_can_transmute_with_data_pronoun():
    out = transmute(mtcars, mtcars.cyl)
    exp = transmute(mtcars, f.cyl)
    assert out.equals(exp)

def test_transmute_doesnot_warn_when_var_removed(caplog):
    df = tibble(x=1)
    transmute(df, z=f.x*2, x=NULL)
    assert caplog.text == ''

def test_transmute_can_handle_auto_splicing():
    out = iris >> transmute(fibble(f.Sepal_Length, f.Sepal_Width))
    exp = iris >> select(f.Sepal_Length, f.Sepal_Width)
    assert out.equals(exp)

def test_transmute_errors():
    with pytest.raises(TypeError):
        transmute(mtcars, cyl2=f.cyl, _keep='all')
    # transmute(mtcars, cyl2=f.cyl, _before=f.disp)
    # transmute(mtcars, cyl2=f.cyl, _after=f.disp)

def test_dup_keyword_args():
    df = tibble(a=1)
    out = df >> mutate(b_=f.a+1, b=f.b*2)
    assert_frame_equal(out, tibble(a=1, b=4))
    # order doesn't matter
    out = df >> mutate(b=f.a+1, b_=f.b*2)
    assert_frame_equal(out, tibble(a=1, b=4))
    # support >= 2 dups
    out = df >> mutate(b__=f.a+1, b_=f.b*2, b=f.b/4.)
    assert_frame_equal(out, tibble(a=1, b=1.))
    # has to be consective
    out = df >> mutate(b__=f.a+1, b_=f.b*2, b=f.b/4.)
    assert_frame_equal(out, tibble(a=1, b=1.))
    out = df >> mutate(b__=f.a+1, b_=f.b_*2)
    assert_frame_equal(out, tibble(a=1, b_=4))
    out = df >> mutate(b_=f.a+1)
    assert_frame_equal(out, tibble(a=1, b_=2))
