# https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-group-by.r
# https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-rowwise.r

import pytest

import numpy
from datar import f
from datar.datasets import mtcars, iris
from datar2.core.tibble import TibbleGroupby, TibbleRowwise
from datar2.dplyr import group_by, group_vars
from datar2.tibble import tibble
from datar2.base import rep
from ..conftest import assert_iterable_equal


@pytest.fixture
def df():
    return tibble(x=rep([1, 2, 3], each=10), y=rep(range(1, 7), each=5))


def test_add(df):
    tbl = df >> group_by(f.x, f.y, _add=True)
    gvars = group_vars(tbl)
    assert gvars == ["x", "y"]

    tbl = df >> group_by(f.x, _add=True) >> group_by(f.y, _add=True)
    gvars = group_vars(tbl)
    assert gvars == ["x", "y"]


# def test_join_preserve_grouping(df):
#     g = df >> group_by(f.x)

#     tbl = g >> inner_join(g, by=["x", "y"])
#     gvars = tbl >> group_vars()
#     assert gvars == ["x"]

#     tbl = g >> left_join(g, by=["x", "y"])
#     gvars = tbl >> group_vars()
#     assert gvars == ["x"]

#     tbl = g >> semi_join(g, by=["x", "y"])
#     gvars = tbl >> group_vars()
#     assert gvars == ["x"]

#     tbl = g >> anti_join(g, by=["x", "y"])
#     gvars = tbl >> group_vars()
#     assert gvars == ["x"]


def test_tibble_lose_grouping(df):
    g = df >> group_by(f.x)
    tbl = tibble(g)
    # with pytest.raises(NotImplementedError):
    assert group_vars(tbl) == []


# group by a string is also referring to the column


# def test_mutate_does_not_loose_variables():
#     df = tibble(
#         a=rep([1, 2, 3, 4], 2), b=rep([1, 2, 3, 4], each=2), x=runif(8)
#     )
#     by_ab = df >> group_by(f.a, f.b)
#     by_a = by_ab >> summarise(x=sum(f.x), _groups="drop_last")
#     by_a_quantile = by_a >> group_by(quantile=ntile(f.x, 4))

#     assert by_a_quantile.columns.tolist() == ["a", "b", "x", "quantile"]


# def test_orders_by_groups():
#     df = tibble(a=sample(range(1, 11), 3000, replace=TRUE)) >> group_by(f.a)
#     out = df >> count()
#     assert_iterable_equal(out.a, range(1, 11))

#     df = tibble(a=sample(letters[:10], 3000, replace=TRUE)) >> group_by(f.a)
#     out = df >> count()
#     assert_iterable_equal(out.a, letters[:10])

#     df = tibble(a=sample(sqrt(range(1, 11)), 3000, replace=TRUE)) >> group_by(
#         f.a
#     )
#     out = df >> count()
#     expect = list(sqrt(range(1, 11)))
#     assert_iterable_equal(out.a, expect)


def test_by_tuple_values():
    df = (
        tibble(
            x=[1, 2, 3],
            # https://github.com/pandas-dev/pandas/issues/21340
            # y=[(1,2), (1,2,3), (1,2)],
            y=[1, 2, 1],
            _dtypes={"y": object},
        )
        >> group_by(f.y)
    )
    out = df >> count()
    # assert out.y.tolist() == [(1,2), (1,2,3)]
    assert out.y.tolist() == [1, 2]
    assert out.n.tolist() == [2, 1]


def test_select_add_group_vars():
    res = mtcars >> group_by(f.vs) >> select(f.mpg)
    assert res.columns.tolist() == ["vs", "mpg"]


def test_one_group_for_NA():
    x = c(NA, NA, NA, range(10, 0, -1), range(10, 0, -1))
    w = numpy.array(c(20, 30, 40, range(1, 11), range(1, 11))) * 10

    assert n_distinct(x) == 11
    res = tibble(x=x, w=w) >> group_by(f.x) >> summarise(n=n())
    # assert nrow(res) == 11
    # See Known Issues of core.grouped.TibbleGroupby
    assert nrow(res) == 10


def test_zero_row_dfs():
    df = tibble(a=[], b=[], g=[])
    dfg = group_by(df, f.g, _drop=False)
    assert dim(dfg) == (0, 3)
    assert group_vars(dfg) == ["g"]
    assert group_size(dfg) == []

    x = summarise(dfg, n=n())
    assert dim(x) == (0, 2)
    assert group_vars(x) == []

    x = mutate(dfg, c=f.b + 1)
    assert dim(x) == (0, 4)
    assert group_vars(x) == ["g"]
    assert group_size(x) == []

    x = filter(dfg, f.a == 100)
    assert dim(x) == (0, 3)
    assert group_vars(x) == ["g"]
    assert group_size(x) == []

    x = arrange(dfg, f.a, f.g)
    assert dim(x) == (0, 3)
    assert group_vars(x) == ["g"]
    assert group_size(x) == []

    x = select(dfg, f.a)
    assert dim(x) == (0, 2)
    assert group_vars(x) == ["g"]
    assert group_size(x) == []


def test_does_not_affect_input_data():
    df = tibble(x=1)
    dfg = df >> group_by(f.x)
    assert df.x.tolist() == [1]


def test_0_groups():
    df = tibble(x=1).loc[[], :] >> group_by(f.x)
    res = df >> mutate(y=mean(f.x), z=+mean(f.x), n=n())
    assert res.columns.tolist() == ["x", "y", "z", "n"]
    rows = res >> nrow()
    assert rows == 0


def test_0_groups_filter():
    df = tibble(x=1).loc[[], :] >> group_by(f.x)
    res = df >> filter(f.x > 3)
    d1 = df >> dim()
    d2 = res >> dim()
    assert d1 == d2
    assert df.columns.tolist() == res.columns.tolist()


def test_0_groups_select():
    df = tibble(x=1).loc[[], :] >> group_by(f.x)
    res = df >> select(f.x)
    d1 = df >> dim()
    d2 = res >> dim()
    assert d1 == d2
    assert df.columns.tolist() == res.columns.tolist()


def test_0_groups_arrange():
    df = tibble(x=1).loc[[], :] >> group_by(f.x)
    res = df >> arrange(f.x)
    d1 = df >> dim()
    d2 = res >> dim()
    assert d1 == d2
    assert df.columns.tolist() == res.columns.tolist()


def test_0_vars(df):
    gdata = group_data(group_by(iris))
    assert names(gdata) == ["_rows"]
    out = gdata >> pull(to="list")
    assert out == [list(range(nrow(iris)))]

    gdata = group_data(group_by(iris, **{}))
    assert names(gdata) == ["_rows"]
    out = gdata >> pull(to="list")
    assert out == [list(range(nrow(iris)))]


def test_drop():
    res = (
        iris
        >> filter(f.Species == "setosa")
        >> group_by(f.Species, _drop=TRUE)
    )
    out = res >> count() >> nrow()
    assert out == 1


def test_remember_drop_true():
    res = iris >> group_by(f.Species, _drop=True)
    assert group_by_drop_default(res)

    res2 = res >> filter(f.Sepal_Length > 5)
    assert group_by_drop_default(res2)

    res3 = res >> filter(f.Sepal_Length > 5, _preserve=FALSE)
    assert group_by_drop_default(res3)

    res4 = res3 >> group_by(f.Species)
    assert group_by_drop_default(res4)

    # group_data to be implemented


def test_remember_drop_false():
    res = (
        iris
        >> filter(f.Species == "setosa")
        >> group_by(f.Species, _drop=FALSE)
    )
    assert not group_by_drop_default(res)

    res2 = res >> group_by(f.Species)
    assert not group_by_drop_default(res2)


# todo
# def test_drop_false_preserve_ordered_factors():
#     ...


def test_summarise_maintains_drop():
    df = tibble(
        f1=factor("a", levels=c("a", "b", "c")),
        f2=factor("d", levels=c("d", "e", "f", "g")),
        x=42,
    )
    res = df >> group_by(f.f1, f.f2, _drop=TRUE)
    ng = n_groups(res)
    assert ng == 1
    assert group_by_drop_default(res)

    # DataFrame.groupby(..., observed=False) doesn't support multiple categoricals
    # res1 = df >> group_by(f.f1, f.f2, _drop=False)
    # ng = n_groups(res1)
    # assert ng == 12

    res1 = df >> group_by(f.f1, _drop=TRUE)
    ng = n_groups(res1)
    assert ng == 1

    res1 = df >> group_by(f.f1, _drop=FALSE)
    ng = n_groups(res1)
    assert ng == 3

    res1 = df >> group_by(f.f2, _drop=FALSE)
    ng = n_groups(res1)
    assert ng == 4

    res2 = res >> summarise(x=sum(f.x), _groups="drop_last")
    ng = n_groups(res2)
    assert ng == 1
    assert group_by_drop_default(res2)


def test_joins_maintains__drop():
    df1 = group_by(
        tibble(f1=factor(c("a", "b"), levels=c("a", "b", "c")), x=[42, 43]),
        f.f1,
        _drop=TRUE,
    )

    df2 = group_by(
        tibble(f1=factor(c("a"), levels=c("a", "b", "c")), y=1),
        f.f1,
        _drop=TRUE,
    )

    res = left_join(df1, df2, by="f1")
    assert n_groups(res) == 2

    df2 = group_by(
        tibble(f1=factor(c("a", "c"), levels=c("a", "b", "c")), y=[1, 2]),
        f.f1,
        _drop=TRUE,
    )
    res = full_join(df1, df2, by="f1")
    assert n_groups(res) == 3


def test_add_passes_drop():
    d = tibble(
        f1=factor("b", levels=c("a", "b", "c")),
        f2=factor("g", levels=c("e", "f", "g")),
        x=48,
    )
    res = group_by(group_by(d, f.f1, _drop=TRUE), f.f2, _add=TRUE)
    ng = n_groups(res)
    assert ng == 1
    assert group_by_drop_default(res)


# NA in groupvars to get group data is not supported
# See: TibbleGroupby's Known Issues
#
# def test_na_last():

#     res = tibble(x = c("apple", NA, "banana"), y = range(1,4)) >> \
#         group_by(f.x) >> \
#         group_data()

#     x = res.x.fillna("")
#     assert x.tolist() == ["apple", "banana", ""]

#     out = res >> pull(to='list')
#     assert out == [[0], [2], [1]]


def test_auto_splicing():
    df1 = iris >> group_by(f.Species)
    df2 = iris >> group_by(tibble(Species=iris.Species))
    assert df1.equals(df2)

    df1 = iris >> group_by(f.Species)
    df2 = iris >> group_by(across(f.Species))
    assert df1.equals(df2)

    df1 = (
        iris
        >> mutate(across(starts_with("Sepal"), round))
        >> group_by(f.Sepal_Length, f.Sepal_Width)
    )
    df2 = iris >> group_by(across(starts_with("Sepal"), round))
    assert df1.equals(df2)

    # across(character()), across(NULL) not supported

    df1 = (
        iris
        >> mutate(across(starts_with("Sepal"), round))
        >> group_by(f.Sepal_Length, f.Sepal_Width, f.Species)
    )
    df2 = iris >> group_by(across(starts_with("Sepal"), round), f.Species)
    assert df1.equals(df2)

    df1 = (
        iris
        >> mutate(across(starts_with("Sepal"), round))
        >> group_by(f.Species, f.Sepal_Length, f.Sepal_Width)
    )
    df2 = iris >> group_by(f.Species, across(starts_with("Sepal"), round))
    assert df1.equals(df2)


def test_mutate_semantics():
    df1 = tibble(a=1, b=2) >> group_by(c=f.a * f.b, d=f.c + 1)
    df2 = (
        tibble(a=1, b=2)
        >> mutate(c=f.a * f.b, d=f.c + 1)
        >> group_by(f.c, f.d)
    )
    assert df1.equals(df2)


def test_implicit_mutate_operates_on_ungrouped_data():
    vars = tibble(x=c(1, 2), y=c(3, 4), z=c(5, 6)) >> group_by(f.y)
    vars >>= group_by(across(any_of(c("y", "z"))))
    gv = group_vars(vars)
    assert gv == ["y", "z"]


def test_errors():
    df = tibble(x=1, y=2)

    with pytest.raises(KeyError):
        df >> group_by(f.unknown)

    with pytest.raises(ValueError):
        df >> ungroup(f.x)

    with pytest.raises(KeyError):
        df >> group_by(f.x, f.y) >> ungroup(f.z)

    with pytest.raises(ValueError):
        df >> group_by(z=f.a + 1)


# rowwise --------------------------------------------


def test_rowwise_preserved_by_major_verbs():
    rf = rowwise(tibble(x=range(1, 6), y=range(5, 0, -1)), f.x)

    out = arrange(rf, f.y)
    assert isinstance(out, TibbleRowwise)
    assert group_vars(out) == ["x"]

    out = filter(rf, f.x < 3)
    assert isinstance(out, TibbleRowwise)
    assert group_vars(out) == ["x"]

    out = mutate(rf, x=f.x + 1)
    assert isinstance(out, TibbleRowwise)
    assert group_vars(out) == ["x"]

    out = rename(rf, X=f.x)
    assert isinstance(out, TibbleRowwise)
    assert group_vars(out) == ["X"]

    out = select(rf, "x")
    assert isinstance(out, TibbleRowwise)
    assert group_vars(out) == ["x"]

    out = slice(rf, c(1, 1))
    assert isinstance(out, TibbleRowwise)
    assert group_vars(out) == ["x"]

    # Except for summarise
    out = summarise(rf, z=mean([f.x, f.y]))
    assert isinstance(out, TibbleGroupby)
    assert group_vars(out) == ["x"]


def test_rowwise_preserved_by_subsetting():
    rf = rowwise(tibble(x=range(1, 6), y=range(5, 0, -1)), f.x)

    out = get(rf, [1])
    assert isinstance(out, TibbleRowwise)
    assert group_vars(out) == ["x"]

    out = mutate(rf, z=f.y)
    assert isinstance(out, TibbleRowwise)
    assert group_vars(out) == ["x"]

    out = set_names(rf, [name.upper() for name in names(rf)])
    assert isinstance(out, TibbleRowwise)
    assert group_vars(out) == ["X"]


def test_rowwise_captures_group_vars():
    df = group_by(tibble(g=[1, 2], x=[1, 2]), f.g)
    rw = rowwise(df)

    assert group_vars(rw) == ["g"]

    with pytest.raises(ValueError):
        rowwise(df, f.x)


def test_can_re_rowwise():
    rf1 = rowwise(tibble(x=range(1, 6), y=range(1, 6)), "x")
    rf2 = rowwise(rf1, f.y)
    assert group_vars(rf2) == ["y"]

    rf3 = rowwise(rf2)
    assert group_vars(rf3) == []


def test_compound_ungroup():
    df = tibble(x=1, y=2) >> group_by(f.x, f.y)
    out = ungroup(df)
    assert group_vars(out) == []

    out = ungroup(df, f.x)
    assert group_vars(out) == ["y"]

    out = ungroup(df, f.y)
    assert group_vars(out) == ["x"]

    out = group_by(df, f.y, _add=True)
    assert group_vars(out) == ["x", "y"]

    rf = df >> rowwise()
    with pytest.raises(ValueError):
        ungroup(rf, f.x)

    with pytest.raises(KeyError):
        group_by(df, f.w)


# GH63
def test_group_by_keeps_the_right_order_of_subdfs():
    df = (
        tibble(
            g1=["a", "b", "c", "a", "b", "c", "a", "b", "c"],
            g2=["a", "b", "c", "a", "b", "c", "a", "b", "b"],
        )
        >> mutate(x=range(9))
    )
    out = df >> group_by(f.g1, f.g2) >> mutate(x=f.x)
    assert_iterable_equal(out.x, range(9))
