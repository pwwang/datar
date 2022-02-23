# tests grabbed from:
# https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-filter.r
import numpy
import pytest
from datar import f
from datar.datasets import iris, mtcars
from datar2.tibble import tibble
from datar2.dplyr import (
    group_by,
    filter,
    rowwise,
    ungroup,
    mutate,
    group_rows,
    group_keys,
    group_vars,
    starts_with,
)
from datar2.base import c, rep, nrow, NA, min
from pandas.testing import assert_frame_equal
from pipda import register_func


def test_handles_passing_args():
    df = tibble(x=range(1, 5))

    def ff(*args):
        x1 = 4
        f1 = lambda y: y
        return df >> filter(*args, f1(x1) > f.x)

    def g():
        x2 = 2
        return ff(f.x > x2)

    res = g()
    assert res.x.tolist() == [3]

    df >>= group_by(f.x)
    res = g()
    assert res.x.obj.tolist() == [3]


def test_handles_simple_symbols():
    df = tibble(x=range(1, 5), test=rep(c(True, False), each=2))
    res = filter(df, f.test)

    gdf = group_by(df, f.x)
    res = filter(gdf, f.test)

    def h(data):
        test2 = c(True, True, False, False)
        return filter(data, test2)

    out = h(df)
    assert out.equals(df.iloc[:2, :])

    def ff(data, *args):
        one = 1
        return filter(data, f.test, f.x > one, *args)

    def g(data, *args):
        four = 4
        return ff(data, f.x < four, *args)

    res = g(df)
    assert res.x.tolist() == [2]
    assert res.test.tolist() == [True]

    res = g(gdf)
    assert res.x.obj.tolist() == [2]
    assert res.test.obj.tolist() == [True]


def test_handles_scalar_results():
    df1 = mtcars >> filter(min(f.mpg) > 0)
    assert df1.equals(mtcars)

    # df2 = mtcars >> group_by(f.cyl) >> filter(min(f.mpg)>0) >> arrange(f.cyl, f.mpg)
    # # See DataFrameGroupBy's Known issues
    # df3 = mtcars >> group_by(f.cyl) >> arrange(f.cyl, f.mpg)
    # assert_frame_equal(df2, df3)


def test_discards_na():
    temp = tibble(i=range(1, 6), x=c(NA, 1, 1, 0, 0))
    res = filter(temp, f.x == 1)
    rows = nrow(res)
    assert rows == 2


def test_returns_input_with_no_args():
    df = filter(mtcars)
    assert df.equals(mtcars)


def test_complex_vec():
    d = tibble(x=range(1, 11), y=[i + 2j for i in range(1, 11)])
    out = d >> filter(f.x < 4)
    assert out.y.tolist() == [i + 2j for i in range(1, 4)]

    # out = d >> filter(re(f.y) < 4)
    # assert out.y.tolist() == [i+2j for i in range(1,4)]


def test_contains():
    df = tibble(a=c("a", "b", "ab"), g=c(1, 1, 2))

    # res = df >> filter(is_element(f.a, letters))
    # rows = nrow(res)
    # assert rows == 2

    # res = df >> group_by(f.g) >> filter(is_element(f.a, letters))
    # rows = nrow(res)
    # assert rows == 2


def test_row_number():
    z = tibble(a=[1, 2, 3])
    b = "a"
    # res = z >> filter(row_number() == 4)
    # rows = nrow(res)
    # assert rows == 0


# def test_row_number_0col():
#     out = tibble() >> mutate(a=row_number())
#     assert nrow(out) == 0
#     assert out.columns.tolist() == ['a']


def test_mixed_orig_df():
    df = tibble(x=range(1, 11), g=rep(range(1, 6), 2))
    res = df >> group_by(f.g) >> filter(f.x > min(df.x))
    assert nrow(res) == 9


def test_empty_df():
    res = tibble() >> filter(False)
    assert nrow(res) == 0
    assert len(res.columns) == 0


def test_true_true():
    df = tibble(x=range(1, 6))
    res = df >> filter(True, True)
    assert res.equals(df)


# def test_rowwise():
#     @register_func(None)
#     def grepl(a, b):
#         return numpy.array([x in y for x, y in zip(a, b)])

#     df = tibble(
#         First=c("string1", "string2"),
#         Second=c("Sentence with string1", "something"),
#     )
#     res = df >> rowwise() >> filter(grepl(f.First, f.Second))
#     assert nrow(res) == 1

#     df1 = df >> slice(1)
#     df2 = res >> ungroup()
#     assert df1.equals(df2)


def test_grouped_filter_handles_indices():
    res = iris >> group_by(f.Species) >> filter(f.Sepal_Length > 5)
    res2 = res >> mutate(Petal=f.Petal_Width * f.Petal_Length)

    assert nrow(res) == nrow(res2)
    grows1 = group_rows(res)
    grows2 = group_rows(res2)
    assert grows1 == grows2
    assert all(group_keys(res) == group_keys(res2))


def test_filter_false_handles_indices():

    # out = mtcars >> group_by(f.cyl) >> filter(False, _preserve=True)
    # out = group_rows(out)
    # assert out == [[], [], []]

    out = mtcars >> group_by(f.cyl) >> filter(False, _preserve=False)
    out = group_rows(out)
    assert out == []

    # # not documented, but _drop=False keeps groups
    # out = mtcars >> group_by(f.cyl, _drop=False) >> filter(False)
    # out = group_rows(out)
    # assert out == [[], [], []]


# def test_hybrid_lag_and_default_value_for_string_cols():


# def test_handles_tuple_columns():
#     res = (
#         tibble(a=[1, 2], x=[tuple(range(1, 11)), tuple(range(1, 6))])
#         >> filter(f.a == 1)
#         >> pull(f.x, to="list")
#     )
#     assert res == [tuple(range(1, 11))]

#     res = (
#         tibble(a=[1, 2], x=[tuple(range(1, 11)), tuple(range(1, 6))])
#         >> group_by(f.a)
#         >> filter(f.a == 1)
#         >> pull(f.x, to="list")
#     )
#     assert res == [tuple(range(1, 11))]


# def test_row_number_no_warning(caplog):
#     mtcars >> filter(row_number() > 1, row_number() < 5)
#     assert caplog.text == ""


# def test_preserve_order_across_groups():
#     df = tibble(g=c(1, 2, 1, 2, 1), time=[5, 4, 3, 2, 1], x=f.time)
#     res1 = (
#         df
#         >> group_by(f.g)
#         >> filter(f.x <= 4)
#         >> ungroup()
#         >> arrange(f.g, f.time)
#     )

#     res2 = (
#         df
#         >> arrange(f.g)
#         >> group_by(f.g)
#         >> filter(f.x <= 4)
#         >> ungroup()
#         >> arrange(f.g, f.time)
#     )

#     res3 = (
#         df
#         >> filter(f.x <= 4)
#         >> group_by(f.g)
#         >> ungroup()
#         >> arrange(f.g, f.time)
#     )
#     res1.reset_index(drop=True, inplace=True)
#     res2.reset_index(drop=True, inplace=True)
#     res3.reset_index(drop=True, inplace=True)
#     assert res1.equals(res2)
#     assert res1.equals(res3)
#     # res1$time, res2$time, res3$time unsorted?


def test_two_conds_not_freeze():
    df1 = iris >> filter(f.Sepal_Length > 7, f.Petal_Length < 6)
    df2 = iris >> filter((f.Sepal_Length > 7) & (f.Petal_Length < 6))
    assert df1.equals(df2)


# def test_handles_df_cols():
#     df = tibble(x=[1, 2], z=tibble(A=[1, 2], B=[3, 4]))
#     expect = df >> slice(1)

#     out = df >> filter(f.x == 1)
#     assert out.equals(expect)
#     out = df >> filter(f["z$A"] == 1)
#     assert out.equals(expect)

#     gdf = df >> group_by(f.x)

#     out = gdf >> filter(f["z$A"] == 1)
#     assert out.equals(expect)
#     out = gdf >> filter(f["z$A"] == 1)
#     assert out.equals(expect)


# def test_handles_named_logical():
#     tbl = tibble(a={'a': True})
#     out = tbl >> filter(f.a)
#     assert out.equals(tbl)


def test_errors():
    # wrong type
    with pytest.raises(ValueError):
        iris >> group_by(f.Species) >> filter(range(1, 10))
    with pytest.raises(ValueError):
        iris >> filter(range(1, 10))

    # wrong size
    with pytest.raises(ValueError):
        iris >> group_by(f.Species) >> filter([True, False])
    with pytest.raises(ValueError):
        iris >> rowwise(f.Species) >> filter([True, False])
    with pytest.raises(ValueError):
        iris >> filter([True, False])

    # wrong size in column
    with pytest.raises(ValueError):
        iris >> group_by(f.Species) >> filter(tibble([True, False]))
    with pytest.raises(ValueError):
        iris >> rowwise() >> filter(tibble([True, False]))
    with pytest.raises(ValueError):
        iris >> filter(tibble([True, False]))
    with pytest.raises(ValueError):
        tibble(x=1) >> filter([True, False])

    # named inputs
    with pytest.raises(TypeError):
        mtcars >> filter(x=1)
    with pytest.raises(TypeError):
        mtcars >> filter(f.y > 2, z=3)
    with pytest.raises(TypeError):
        mtcars >> filter(True, x=1)

    # across() in filter() does not warn yet
    # tibble(x=1, y=2) >> filter(across(everything(), lambda x: x>0))


def test_preserves_grouping():
    gf = tibble(g=[1, 1, 1, 2, 2], x=[1, 2, 3, 4, 5]) >> group_by(f.g)
    # out = gf >> filter(is_element(f.x, [3, 4]))
    # assert group_vars(out) == ["g"]
    # assert group_rows(out) == [[0], [1]]

    out = gf >> filter(f.x < 3)
    assert group_vars(out) == ["g"]
    assert group_rows(out) == [[0, 1]]


# def test_works_with_if_any_if_all():
#     df = tibble(x1=range(1, 11), x2=c(range(1, 6), 10, 9, 8, 7, 6))
#     df1 = df >> filter(if_all(starts_with("x"), lambda x: x > 6))
#     df2 = df >> filter((f.x1 > 6) & (f.x2 > 6))
#     assert df1.equals(df2)

#     df1 = df >> filter(if_any(starts_with("x"), lambda x: x > 6))
#     df2 = df >> filter((f.x1 > 6) | (f.x2 > 6))
#     assert df1.equals(df2)


# GH69
# def test_filter_restructures_group_data_correctly():
#     df = (
#         mtcars
#         >> arrange(f.gear)
#         >> group_by(f.cyl)
#         >> mutate(cum=f.drat.cumsum())
#         >> filter(f.cum >= 5)
#         >> mutate(ranking=f.cum.rank())
#     )
#     assert nrow(df) == 29
