# tests grabbed from:
# https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-slice.r
from pipda.context import ContextError
import pytest
from datar.all import *
from datar.datasets import mtcars
from datar.dplyr.slice import _n_from_prop

def test_empty_slice_returns_input():
    df = tibble(x=[1,2,3])
    assert slice(df).equals(df)

def test_slice_handles_numeric_input():
    g = mtcars >> arrange(f.cyl) >> group_by(f.cyl)
    res = g >> slice(1)
    assert nrow(res) == 3
    exp = g >> filter(row_number() == 2)
    assert res.equals(exp)

    res1 = mtcars >> slice(1)
    res2 = mtcars >> filter(row_number() == 2)
    assert res1.equals(res2)

def test_slice_silently_ignores_out_of_range_values():
    res1 = slice(mtcars, c(2, 100))
    res2 = slice(mtcars, 2)
    assert res1.equals(res2)

    g = group_by(mtcars, f.cyl)
    res1 = slice(g, c(2, 100))
    res2 = slice(g, 2)
    assert res1.equals(res2)


def test_slice_works_with_negative_indices():
    res = slice(mtcars, ~f[:2])
    # wait for tail
    # exp = tail(mtcars, -2) # tail with negative?
    # exp = tail(mtcars, nrow(mtcars)-2)
    # assert res.equals(exp)

def test_slice_works_with_grouped_data():
    g = mtcars >> arrange(f.cyl) >> group_by(f.cyl)

    res = slice(g, f[:2])
    exp = filter(g, row_number() < 3)
    assert res.equals(exp)

    res = slice(g, ~f[:2])
    exp = filter(g, row_number() >= 3)
    assert res.equals(exp)

    g = group_by(tibble(x=c(1,1,2,2,2)), f.x)
    out = group_keys(slice(g, 2, _preserve=True)) >> pull(f.x, to='list')
    assert out == [1,2]
    out = group_keys(slice(g, 2, _preserve=False)) >> pull(f.x, to='list')
    assert out == [2]

def test_slice_gives_correct_rows():
    a = tibble(value=[f"row{i}" for i in range(1,11)])
    out = slice(a, [0, 1,2]) >> pull(f.value, to='list')
    assert out == ['row1', 'row2', 'row3']

    out = slice(a, [3,5,8]) >> pull(f.value, to='list')
    assert out == ['row4', 'row6', 'row9']

    a = tibble(
        value=[f"row{i}" for i in range(1,11)],
        group=rep([1,2], each=5)
    ) >> group_by(f.group)

    out = slice(a, [0,1,2]) >> pull(f.value, to='list')
    assert out == [f'row{i}' for i in [1,2,3, 6,7,8]]

    out = slice(a, c(1,3)) >> pull(f.value, to='list')
    assert out == [f'row{i}' for i in [2,4,7,9]]

def test_slice_handles_na():
    df = tibble(x=[1,2,3])
    assert nrow(slice(df, NA)) == 0
    assert nrow(slice(df, c(0, NA))) == 1
    out = df >> slice(c(~c(0), NA)) >> nrow()
    assert out == 2

    df = tibble(x=[1,2,3,4], g=rep([1,2], 2)) >> group_by(f.g)
    assert nrow(slice(df, c(0, NA))) == 2
    out = df >> slice(c(~c(0), NA)) >> nrow()
    assert out == 2

def test_slice_handles_logical_NA():
    df = tibble(x=[1,2,3])
    assert nrow(slice(df, NA)) == 0

def test_slice_handles_empty_df():
    df = tibble(x=[])
    res = df >> slice(f[:3])
    assert nrow(res) == 0
    assert names(res) == ["x"]

def test_slice_works_fine_if_n_gt_nrow():
    by_slice = mtcars >> arrange(f.cyl) >> group_by(f.cyl)
    slice_res = by_slice >> slice(7)
    filter_res = by_slice >> group_by(f.cyl) >> filter(row_number() == 8)
    assert slice_res.equals(filter_res)

def test_slice_strips_grouped_indices():
    res = mtcars >> group_by(f.cyl) >> slice(0) >> mutate(mpgplus=f.mpg+1)
    assert nrow(res) == 3
    assert group_rows(res) == [[0], [1], [2]]

def test_slice_works_with_0col_dfs():
    out = tibble(a=[1,2,3]) >> select(~f.a) >> slice(0) >> nrow()
    assert out == 1

def test_slice_correctly_computes_positive_indices_from_negative_indices():
    x = tibble(y=range(1,11))
    # negative in dplyr meaning exclusive
    assert slice(x, ~f[9:32]).equals(tibble(y=range(1,10)))

def test_slice_accepts_star_args():
    out1 = slice(mtcars, 1, 2)
    out2 = slice(mtcars, [1,2])
    assert out1.equals(out2)

    out3 = slice(mtcars, 1, n())
    out4 = slice(mtcars, c(1, nrow(mtcars)))
    assert out3.equals(out4)

    g = mtcars >> group_by(f.cyl)
    out5 = slice(g, 1, n())
    out6 = slice(g, c(1, n()))
    assert out5.equals(out6)

def test_slice_does_not_evaluate_the_expression_in_empty_groups():
    res = mtcars >> \
        group_by(f.cyl) >> \
        filter(f.cyl==6) >> \
        slice(f[:2])
    assert nrow(res) == 2

    # sample_n is Superseded in favor of slice_sample
    # res = mtcars >> \
    #     group_by(f.cyl) >> \
    #     filter(f.cyl==6) >> \
    #     sample_n(size=3)
    # assert nrow(res) == 3

def test_slice_handles_df_columns():
    df = tibble(x=[1,2], y=tibble(a=[1,2], b=[3,4]), z=tibble(A=[1,2], B=[3,4]))
    out = slice(df, 0)
    assert out.equals(df.iloc[[0], :])

    gdf = group_by(df, f.x)
    assert slice(gdf, 0).equals(gdf)
    # TODO: group_by a stacked df is not supported yet
    gdf = group_by(df, f['y$a'], f['y$b'])
    assert slice(gdf, 0).equals(gdf)
    gdf = group_by(df, f['z$A'], f['z$B'])
    assert slice(gdf, 0).equals(gdf)

# # Slice variants ----------------------------------------------------------

def test_functions_silently_truncate_results():
    df = tibble(x=range(1,6))
    # out = df >> slice_head(n=6) >> nrow()
    # assert out == 5
    out = df >> slice_tail(n=6) >> nrow()
    assert out == 5
    out = df >> slice_sample(n=6) >> nrow()
    assert out == 5
    out = df >> slice_min(f.x, n=6) >> nrow()
    assert out == 5
    out = df >> slice_max(f.x, n=6) >> nrow()
    assert out == 5

def test_proportion_computed_correctly():
    df = tibble(x=range(1,11))

    out = df >> slice_head(prop=.11) >> nrow()
    assert out == 1
    out = df >> slice_tail(prop=.11) >> nrow()
    assert out == 1
    out = df >> slice_sample(prop=.11) >> nrow()
    assert out == 1
    out = df >> slice_min(f.x, prop=.11) >> nrow()
    assert out == 1
    out = df >> slice_max(f.x, prop=.11) >> nrow()
    assert out == 1
    out = df >> slice_max(f.x, prop=.11, with_ties=False) >> nrow()
    assert out == 1
    out = df >> slice_min(f.x, prop=.11, with_ties=False) >> nrow()
    assert out == 1

def test_min_and_max_return_ties_by_default():
    df = tibble(x=c(1,1,1,2,2))

    out = df >> slice_min(f.x) >> nrow()
    assert out == 3
    out = df >> slice_max(f.x) >> nrow()
    assert out == 2

    out = df >> slice_min(f.x, with_ties=False) >> nrow()
    assert out == 1
    out = df >> slice_max(f.x, with_ties=False) >> nrow()
    assert out == 1

def test_min_and_max_reorder_results():
    df = tibble(id=range(1,5), x=c(2,3,1,2))
    out = df >> slice_min(f.x, n=2) >> pull(f.id, to='list')
    assert out == [3,1,4]
    out = df >> slice_min(f.x, n=2, with_ties=False) >> pull(f.id, to='list')
    assert out == [3,1]
    out = df >> slice_max(f.x, n=2) >> pull(f.id, to='list')
    assert out == [2,1,4]
    out = df >> slice_max(f.x, n=2, with_ties=False) >> pull(f.id, to='list')
    assert out == [2,1]

def test_min_and_max_ignore_nas():
    df = tibble(
        id=range(1,5),
        x=c(2,NA,1,2),
        y=[NA]*4
    )
    out = df >> slice_min(f.x, n=2) >> pull(f.id, to='list')
    assert out == [3,1,4]
    out = df >> slice_min(f.y, n=2) >> nrow()
    assert out == 0
    out = df >> slice_max(f.x, n=2) >> pull(f.id, to='list')
    assert out == [1,4]
    out = df >> slice_max(f.y, n=2) >> nrow()
    assert out == 0

def test_arguments_to_sample_are_passed_along():
    df = tibble(x=range(1,101), wt=c(1, rep(0, 99)))
    out = df >> slice_sample(n=1, weight_by=f.wt) >> pull(f.x, to='list')
    assert out == [1]

    out = df >> slice_sample(n=2, weight_by=f.wt, replace=True) >> pull(f.x, to='list')
    assert out == [1,1]

def test_slice_any_checks_for_empty_args_kwargs():
    df = tibble(x=range(1,11))
    # python recognize n=5
    # with pytest.raises(ValueError):
    #     slice_head(df, 5)
    # with pytest.raises(ValueError):
    #     slice_tail(df, 5)
    with pytest.raises(TypeError):
        slice_min(df, n=5)
    with pytest.raises(TypeError):
        slice_max(df, n=5)
    # with pytest.raises(ValueError):
    #     slice_sample(df, 5)

def test_slice_any_checks_for_constant_n_and_prop():
    df = tibble(x=range(1,11))
    with pytest.raises(ContextError):
        slice_head(df, n=f.x) # ok with n()
    with pytest.raises(ContextError):
        slice_head(df, prop=f.x)

    with pytest.raises(ContextError):
        slice_tail(df, n=f.x)
    with pytest.raises(ContextError):
        slice_tail(df, prop=f.x)

    with pytest.raises(ContextError):
        slice_min(df, f.x, n=f.x)
    with pytest.raises(ContextError):
        slice_min(df, f.x, prop=f.x)

    with pytest.raises(ContextError):
        slice_max(df, f.x, n=f.x)
    with pytest.raises(ContextError):
        slice_max(df, f.x, prop=f.x)

    with pytest.raises(ContextError):
        slice_sample(df, n=f.x)
    with pytest.raises(ContextError):
        slice_sample(df, prop=f.x)

def test_slice_sample_dose_not_error_on_0rows():
    df = tibble(dummy=[], weight=[])
    res = slice_sample(df, prop=.5, weight_by=f.weight)
    assert nrow(res) == 0


# # Errors ------------------------------------------------------------------
def test_rename_errors_with_invalid_grouped_df():
    df = tibble(x=[1,2,3])

    # Incompatible type
    with pytest.raises(TypeError):
        slice(df, object())
    with pytest.raises(TypeError):
        slice(df, {'a': 1})

    # Mix of positive and negative integers
    with pytest.raises(ValueError):
        mtcars >> slice(c(~c(1), 2))
    with pytest.raises(ValueError):
        mtcars >> slice(c(f[2:4], ~c(1)))

    # n and prop are carefully validated
    # with pytest.raises(ValueError):
    #     _n_from_prop(10, n=1, prop=1)
    with pytest.raises(TypeError):
        _n_from_prop(10, n="a")
    with pytest.raises(TypeError):
        _n_from_prop(10, prop="a")
    with pytest.raises(ValueError):
        _n_from_prop(10, n=-1)
    with pytest.raises(ValueError):
        _n_from_prop(10, prop=-1)
    with pytest.raises(TypeError):
        _n_from_prop(10, n=n())
    with pytest.raises(TypeError):
        _n_from_prop(10, prop=n())

## tests for datar
def test_mixed_rows():
    df = tibble(x=range(5))

    # order kept
    out = slice(df, c(-c(1,3), 3)) >> pull(f.x, to='list')
    assert out == [4, 2, 3]

    out = slice(df, c(-f[:2], 3)) >> pull(f.x, to='list')
    assert out == [3, 4]

    out = slice(df, c(~c(1,3), ~c(0))) >> pull(f.x, to='list')
    assert out == [2, 4]

    out = slice(df, c(~f[3:], ~c(0))) >> pull(f.x, to='list')
    assert out == [1, 2]

def test_slice_sample_n_defaults_to_1():
    df = tibble(
        g = rep([1,2], each=3),
        x = seq(1,6)
    )
    out = df >> slice_sample(n=None)
    assert dim(out) == (1, 2)

def test_slicex_on_grouped_data():
    gf = tibble(
        g = rep([1,2], each=3),
        x = seq(1,6)
    ) >> group_by(f.g)

    out = gf >> slice_min(f.x)
    assert out.equals(tibble(g=[1,2], x=[1,4]))
    out = gf >> slice_max(f.x)
    assert out.equals(tibble(g=[1,2], x=[3,6]))
    out = gf >> slice_sample()
    assert dim(out) == (2, 2)
