# tests grabbed from:
# https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-group_data.R
# https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-group_map.R
# https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-group_split.R
# https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-group_trim.R
# https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-groups-with.R
from datar.core.grouped import DataFrameGroupBy
from pandas.core import groupby
import pytest
from pandas import DataFrame
from pandas.testing import assert_frame_equal
from datar.all import *
from datar.datasets import mtcars, iris

# group_data --------------------------------------------------------------
def test_group_data_dataframe_returns_df():
    df = tibble(x=[1,2,3])
    gd = group_data(df)
    assert isinstance(gd, DataFrame)
    assert gd._rows.tolist() == [[0,1,2]]

def test_group_data_dataframegroupby_returns_df():
    df = tibble(x=c(1,1,2))
    gf = group_by(df, f.x)
    gd = group_data(gf)

    assert isinstance(gd, DataFrame)
    assert gd._rows.tolist() == [[0,1], [2]]

def test_group_data_dataframerowwise_returns_df():
    df = tibble(x=[1,2,3])
    rf = rowwise(df)
    gd = group_data(rf)

    assert isinstance(gd, DataFrame)
    assert gd._rows.tolist() == [[0],[1],[2]]

# group_rows() and group_keys() -------------------------------------------
def test_group_rows_group_keys_partition_group_data():
    df = tibble(x=[1,2], y=[1,2])
    rows = group_rows(df)
    assert rows == [[0,1]]

    gf = group_by(df, f.x, f.y)
    gd = group_data(gf)

    assert group_keys(gf).equals(gd.iloc[:, [0,1]])
    assert pull(gd, to='list') == group_rows(gf)

def test_group_keys():
    df = tibble(x=1, y=2)
    with pytest.raises(TypeError):
        # not even supported for group_keys additionsl arguments
        df >> group_keys(f.x)

# group_indices() ---------------------------------------------------------
def test_no_arg_group_indices():
    df = tibble(x=1)
    # verb works as function, need the data argument
    out = summarise(df, id=group_indices(df))
    assert out.equals(tibble(id=0))

def test_group_indices_additional_args_not_supported():
    # test_that("group_indices(...) is deprecated", {
    df = tibble(x = 1, y = 2)
    with pytest.raises(TypeError):
        df >> group_indices(f.x)

def test_group_indices():
    df = tibble(x=1, y=2)
    out = df >> group_indices()
    assert out == [0]

    df = tibble(x=c("b", "a", "b"))
    gf = group_by(df, f.x)

    assert group_indices(df) == [0, 0, 0]
    assert group_indices(gf) == [0, 1, 0]

def test_group_indices_handles_0row_df():
    df = tibble(x=[], y=[]) >> group_by(f.x)
    assert group_indices(df) == []

# group_size --------------------------------------------------------------
def test_ungrouped_data_has_1group_with_group_size_nrow():
    df = tibble(x=rep([1,2,3], each=10), y=rep(range(1,7), each=5))
    assert n_groups(df) == 1
    assert group_size(df) == [30]

def test_rowwise_data_has_1group_for_each_group():
    rw = rowwise(mtcars)
    assert n_groups(rw) == 32
    assert group_size(rw) == [1] * 32

def test_group_size_correct_for_grouped_data():
    df = tibble(
        x = rep([1,2,3], each=10),
        y = rep(range(1,7), each=5)
    ) >> group_by(f.x)
    assert n_groups(df) == 3
    assert group_size(df) == [10] * 3

# group_map
# -----------------------------------------------------------------
def test_group_map_respects_empty_groups():
    res = group_by(mtcars, f.cyl) >> group_map(lambda df: head(df, 2))
    assert len(list(res)) == 3

    res = iris >> group_by(f.Species) >> filter(
        f.Species == "setosa") >> group_map(tally)
    assert len(list(res)) == 1

    res = iris >> group_by(f.Species, _drop=False) >> filter(
        f.Species == "setosa") >> group_map.list(tally)
    assert len(res) == 3

def test_group_map_can_return_arbitrary_objects():
    out = group_by(mtcars, f.cyl) >> group_map.list(lambda df: 10)
    assert out == [10] * 3
    out = group_by(mtcars, f.cyl) >> group_walk(lambda df: 10)
    assert out is None

def test_group_map_works_on_ungrouped_df():
    out = group_map(mtcars, lambda df: head(df, 2))
    out = list(out)[0]
    assert_frame_equal(out, head(mtcars, 2))

def test_group_modify_makes_a_grouped_df():
    res = group_by(mtcars, f.cyl) >> group_modify(
        lambda df: head(df, 2)
    )
    assert nrow(res) == 6
    assert group_rows(res) == [[0,1], [2,3], [4,5]]

    res = iris >> group_by(f.Species) >> filter(
        f.Species == "setosa") >> group_modify(lambda df: tally(df))
    assert nrow(res) == 1
    assert group_rows(res) == [[0]]

    res = iris >> group_by(f.Species, _drop=False) >> filter(
        f.Species == "setosa") >> group_modify(lambda df: tally(df))
    assert nrow(res) == 3
    assert group_rows(res) == [[0],[1],[2]]

def test_group_modify_map_want_functions_with_at_least_1_arg():
    head1 = lambda df: head(df, 1)
    g = iris >> group_by(f.Species)
    assert nrow(group_modify(g, head1)) == 3
    assert len(list(group_map(g, head1))) == 3

    head_err = lambda: 1
    with pytest.raises(TypeError):
        group_modify(g, head_err)
    with pytest.raises(TypeError):
        group_map.list(g, head_err) # force function to execute

def test_group_modify_works_on_ungrouped_df():
    out = group_modify(mtcars, lambda df: head(df, 2))
    assert out.equals(head(mtcars, 2))

# test_that("group_map() uses ptype on empty splits (#4421)", {
#   res <- mtcars %>%
#     group_by(cyl) %>%
#     filter(hp > 1000) %>%
#     group_map(~.x)
#   expect_equal(res, list(), ignore_attr = TRUE)
#   ptype <- attr(res, "ptype")
#   expect_equal(names(ptype), setdiff(names(mtcars), "cyl"))
#   expect_equal(nrow(ptype), 0L)
#   expect_s3_class(ptype, "data.frame")
# })

# test_that("group_modify() uses ptype on empty splits (#4421)", {
#   res <- mtcars %>%
#     group_by(cyl) %>%
#     filter(hp > 1000) %>%
#     group_modify(~.x)
#   expect_equal(res, group_by(mtcars[integer(0L), names(res)], cyl))
# })


# group_split ---------------------------------------------------------

def test_group_modify_works_with_additional_arguments():
    def myfun(x, y, foo):
        x = x.copy()
        x[foo] = 1
        return x

    srcdata = tibble(
        A=rep([1,2], each=3)
    ) >> group_by(f.A)
    targetdata = srcdata.copy()
    targetdata['bar'] = 1

    out = group_modify(srcdata, _f=myfun, foo="bar")
    assert out.equals(targetdata)

def test_group_map_doesnot_warn_about_keep_for_rowwise_df(caplog):
    tibble(x=1) >> rowwise() >> group_map(lambda df: None)
    assert caplog.text == ''

def test_group_map_errors():
    # head1 = lambda df: head(df, 1)

    # group_modify()
    with pytest.raises(ValueError, match="grouping variables"):
        mtcars >> group_by(f.cyl) >> group_modify(lambda df: tibble(cyl=19))
    with pytest.raises(ValueError, match="should be a data frame"):
        mtcars >> group_by(f.cyl) >> group_modify(lambda df: 10)

    # dplyr requires function to have at least 2 arguments
    # but we dont, only one is good.
    #
    # expect_snapshot(error = TRUE, iris %>% group_by(Species) %>% group_modify(head1))
    # expect_snapshot(error = TRUE, iris %>% group_by(Species) %>% group_map(head1))

def test_group_split_keeps_group_variables_by_default():
    tbl = tibble(x=[1,2,3,4], g=factor(rep(["a", "b"], each=2)))
    out = group_split(tbl, f.g)
    res = list(out)

    assert len(res) == 2
    assert res[0].equals(tbl.iloc[[0,1], :])
    assert res[1].equals(tbl.iloc[[2,3], :])

def test_group_split_can_discard_grouping_vars_by__keep_eqs_false():
    tbl = tibble(x=[1,2,3,4], g=factor(rep(["a", "b"], each=2)))
    res = group_split.list(tbl, f.g, _keep=False)

    assert res[0].equals(tbl.iloc[:2, [0]])
    assert res[1].equals(tbl.iloc[[2,3], [0]])

def test_group_list_respects_empty_groups():
    tbl = tibble(x=[1,2,3,4], g=factor(rep(["a", "b"], each=2), levels=["a", "b", "c"]))

    res = group_split.list(tbl, f.g)

    assert res[0].equals(tbl.iloc[:2, :])
    assert res[1].equals(tbl.iloc[[2,3], :])

    res = group_split.list(tbl, f.g, _drop=False)
    assert res[0].equals(tbl.iloc[:2, :])
    assert res[1].equals(tbl.iloc[[2,3], :])
    assert res[2].equals(tbl.iloc[[], :])

def test_group_split_grouped_df_warns_about_args_kwargs(caplog):
    # test_that("group_split.grouped_df() warns about ...", {
    group_split(group_by(mtcars, f.cyl), f.cyl)
    assert '`*args` and `**kwargs` are ignored in `group_split(<DataFrameGroupBy)`' in caplog.text
    # expect_warning(group_split(group_by(mtcars, cyl), cyl))

def test_group_split_rowwise_df_warns_about_args_kwargs(caplog):
    # test_that("group_split.rowwise_df() warns about ...", {
    group_split(rowwise(mtcars), f.cyl, _keep=True)
    assert '`*args` and `**kwargs` is ignored in `group_split(<DataFrameRowwise>)`' in caplog.text
    # expect_warning(group_split(rowwise(mtcars), cyl))

    # warns about _keep
    assert "`_keep` is ignored" in caplog.text

def test_group_split_grouped_df_works():
    out = iris >> group_by(f.Species) >> group_split()
    exp = iris >> group_split(f.Species)

    for o, e in zip(out, exp):
        assert o.equals(e)

def test_group_split_bind_rows_round_trip():
    iris['Species'] = iris['Species'].astype('category')
    setosa = iris >> filter(f.Species == "setosa")

    chunks = setosa >> group_split.list(f.Species)
    assert len(chunks) == 1
    assert bind_rows(chunks).equals(setosa)

    chunks = setosa >> group_split.list(f.Species, _drop=False)
    assert len(chunks) == 3
    assert bind_rows(chunks).equals(setosa)

def test_group_split_works_if_no_grouping_column():
    out = group_split.list(iris)
    assert len(out) == 1
    assert out[0].equals(iris)

def test_group_split_keep_false_does_not_try_to_remove_virtual_grouping_columns():
    # test_that("group_split(keep=FALSE) does not try to remove virtual grouping columns (#4045)", {
    iris3 = iris.head(4).copy()
    rows = [[0,2], [1,3]]
    gdata = tibble(_bootstrap=[0,1], rows=rows) >> rename(_rows='rows')
    iris3['_bootstrap'] = [0,1,0,1]
    df = DataFrameGroupBy(
        iris3,
        _group_vars=['_bootstrap'],
        _group_data=gdata
    )
    res = group_split.list(df, _keep=False)
    iris3 = select(iris3, ~f._bootstrap)
    assert len(res) == 2
    assert_frame_equal(res[0], iris3.iloc[rows[0], :])
    assert_frame_equal(res[1], iris3.iloc[rows[1], :])


def test_group_split_respects__drop():
    # test_that("group_split() respects .drop", {
    chunks = tibble(f=factor(["b"], levels=list("abc"))) >> group_split.list(
        f.f, _drop=True
    )
    assert len(chunks) == 1

# test_that("group_split() on a bare data frame returns bare tibbles", {
#   df <- data.frame(x = 1:2)
#   tib <- as_tibble(df)
#   expect <- list_of(vec_slice(tib, 1), vec_slice(tib, 2))
#   expect_identical(group_split(df, x), expect)
# })

def test_group_split_on_a_grouped_df_returns_a_list_of_tibbles():
    df = tibble(x=[1,2])
    gdf = group_by(df, f.x)
    out = group_split.list(gdf)
    assert len(out) == 2
    assert out[0].equals(df.iloc[[0], :])
    assert out[1].equals(df.iloc[[1], :])

def test_group_split_on_a_rowwise_df_returns_a_list_of_tibbles():
    df = tibble(x=[1,2])
    rdf = rowwise(df)
    out = group_split.list(rdf)
    assert len(out) == 2
    assert out[0].equals(df.iloc[[0], :])
    assert out[1].equals(df.iloc[[1], :])

def test_group_split_works_with_subclasses_implementing_group_by_ungroup():
    # test_that("group_split() works with subclasses implementing group_by() / ungroup()", {
    class DataFrameGroupBy1(DataFrameGroupBy):
        ...

    df = DataFrameGroupBy1(dict(x=[1,2,2]), _group_vars=["x"])
    out = group_split.list(df, f.x)
    assert len(out) == 2
    assert out[0].equals(df.iloc[[0], :])
    assert out[1].equals(df.iloc[[1,2], :])

# test_that("group_split() internally uses dplyr_row_slice()", {
#   local_foo_df()

#   df <- list(x = c(1, 2, 2))
#   df <- new_tibble(df, nrow = 3L, class = "foo_df")

#   local_methods(
#     dplyr_row_slice.foo_df = function(x, i, ...) {
#       abort(class = "dplyr_row_slice_called")
#     }
#   )

#   expect_error(group_split(df, x), class = "dplyr_row_slice_called")
# })


# group_trim ---------------------------------------------------------

def test_group_trim_is_identity_on_nongrouped_data():
    # test_that("group_trim() is identity on non grouped data", {
    assert group_trim(iris).equals(iris)

def test_group_trim_always_regroups_even_if_no_factors():
    res = mtcars >> \
        group_by(f.cyl) >> \
        filter(f.cyl == 6, _preserve=True) >> \
        group_trim()
    assert n_groups(res) == 1

def test_group_trim_drops_factor_levels_in_data_and_grouping_structure():
    res = iris >> \
        mutate(Species=as_factor(f.Species)) >> \
        group_by(f.Species) >> \
        filter(f.Species == "setosa") >> \
        group_trim()

    assert n_groups(res) == 1
    assert levels(res.Species) == ["setosa"]
    # expect_equal(levels(attr(res, "groups")$Species), "setosa")

# with_groups -----------------------------------------------------

def test_with_groups_restores_original_class():
    df = tibble(x=[1,2])
    gf = group_by(df, f.x)

    out = with_groups(df, f.x, mutate)
    assert isinstance(out, DataFrame)

    out = with_groups(gf, f.x, mutate)
    assert isinstance(out, DataFrameGroupBy)

def test_with_groups__groups_eq_null_ungroups():
    ".groups = NULL ungroups"
    gf = group_by(tibble(x=[1.0,2.0]), f.x)
    out = gf >> with_groups(NULL, mutate, y=mean(f.x))
    assert out.y.tolist() == [1.5, 1.5]

def test__groups_is_defused_with_context():
    # test_that(".groups is defused with context", {
    local_fn = identity
    out = with_groups(mtcars, local_fn(2), mutate, disp=f.disp/sd(f.disp))
    exp = with_groups(mtcars, 2, mutate, disp=f.disp/sd(f.disp))
    assert out.equals(exp)
    assert group_vars(out) == group_vars(exp)
