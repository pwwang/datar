# tests grabbed from:
# https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-mutate-windowed.R
import pytest  # noqa
from datar.all import *
from datar.datasets import mtcars
from ..conftest import assert_iterable_equal


def test_desc_correctly_handled_by_window_functions():
    df = tibble(
        x=range(1, 11),
        y=seq(1, 10, by=1),
        g=rep([1, 2], each=5),
        s=c(letters[:3], LETTERS[:5], letters[[3, 4]]),
    )
    out = mutate(df, rank=min_rank(desc(f.x))) >> pull(to="list")
    assert out == list(range(10, 0, -1))
    out = mutate(group_by(df, f.g), rank=min_rank(desc(f.x))) >> pull(
        to="list"
    )
    assert out == rep(range(5, 0, -1), 2).tolist()

    out = df >> mutate(rank=row_number(desc(f.x))) >> pull()
    assert_iterable_equal(out, range(10, 0, -1))  # 0-based
    out = group_by(df, f.g) >> mutate(rank=row_number(desc(f.x))) >> pull()
    assert_iterable_equal(out, rep(range(5, 0, -1), 2))


def test_row_number_gives_correct_results():
    tmp = tibble(
        id=rep([1, 2], each=5),
        value=c(1, 1, 2, 5, 0, 6, 4, 0, 0, 2),
        x=c(letters[:2], LETTERS[:4], letters[1:5]),
    )

    res = group_by(tmp, f.id) >> mutate(var=row_number(f.value)) >> pull()
    assert_iterable_equal(res, c(2, 3, 4, 5, 1, 5, 4, 1, 2, 3))


def test_row_number_works_with_0_arguments():
    g = group_by(mtcars, f.cyl)
    out = mutate(g, rn=row_number())
    exp = mutate(g, rn=seq(1, n()))
    assert out.equals(exp)


def test_cum_sum_min_max_works():
    # test_that("cum(sum,min,max) works", {
    df = tibble(x=range(1, 11), y=seq(1, 10, by=1), g=rep([1, 2], each=5))
    res = mutate(
        df,
        csumx=cumsum(f.x),
        csumy=cumsum(f.y),
        cminx=cummin(f.x),
        cminy=cummin(f.y),
        cmaxx=cummax(f.x),
        cmaxy=cummax(f.y),
    )
    assert res.csumx.tolist() == cumsum(df.x).tolist()
    assert res.csumy.tolist() == cumsum(df.y).tolist()
    assert res.cminx.tolist() == cummin(df.x).tolist()
    assert res.cminy.tolist() == cummin(df.y).tolist()
    assert res.cmaxx.tolist() == cummax(df.x).tolist()
    assert res.cmaxy.tolist() == cummax(df.y).tolist()

    res = mutate(
        group_by(df, f.g),
        csumx=cumsum(f.x),
        csumy=cumsum(f.y),
        cminx=cummin(f.x),
        cminy=cummin(f.y),
        cmaxx=cummax(f.x),
        cmaxy=cummax(f.y),
    )
    assert_iterable_equal(
        res.csumx, c(list(cumsum(df.x[:5])), list(cumsum(df.x[5:])))
    )
    assert_iterable_equal(
        res.csumy, c(list(cumsum(df.x[:5])), list(cumsum(df.x[5:])))
    )
    assert_iterable_equal(
        res.cminx, c(list(cummin(df.x[:5])), list(cummin(df.x[5:])))
    )
    assert_iterable_equal(
        res.cminy, c(list(cummin(df.x[:5])), list(cummin(df.x[5:])))
    )
    assert_iterable_equal(
        res.cmaxx, c(list(cummax(df.x[:5])), list(cummax(df.x[5:])))
    )
    assert_iterable_equal(
        res.cmaxy, c(list(cummax(df.x[:5])), list(cummax(df.x[5:])))
    )

    df.loc[2, "x"] = NA
    df.loc[3, "y"] = NA
    res = mutate(
        df,
        csumx=cumsum(f.x),
        csumy=cumsum(f.y),
        cminx=cummin(f.x),
        cminy=cummin(f.y),
        cmaxx=cummax(f.x),
        cmaxy=cummax(f.y),
    )
    assert all(is_na(res.csumx[2:]))
    assert all(is_na(res.csumy[3:]))

    assert all(is_na(res.cminx[2:]))
    assert all(is_na(res.cminy[3:]))

    assert all(is_na(res.cmaxx[2:]))
    assert all(is_na(res.cmaxy[3:]))


def test_lead_lag_simple_hybrid_version_gives_correct_results():
    # test_that("lead and lag simple hybrid version gives correct results (#133)", {
    res = (
        group_by(mtcars, f.cyl)
        >> mutate(disp_lag_2=lag(f.disp, 2), disp_lead_2=lead(f.disp, 2))
        >> summarise(
            lag1=all(is_na(head(f.disp_lag_2, 2))),
            lag2=all(~is_na(tail(f.disp_lag_2, -2))),
            lead1=all(is_na(tail(f.disp_lead_2, 2))),
            lead2=all(~is_na(head(f.disp_lead_2, -2))),
        )
    )

    assert all(res.lag1)
    assert all(res.lag2)

    assert all(res.lead1)
    assert all(res.lead2)


def test_min_rank_handles_columns_full_of_nas():
    # test_that("min_rank handles columns full of NaN (#726)", {
    test = tibble(Name=list("abcde"), ID=[1] * 5, expression=[NA] * 5)
    data = group_by(test, f.ID) >> mutate(rank=min_rank(f.expression))
    assert all(is_na(data["rank"]))


def test_ntile_works_with_one_argument():
    # test_that("ntile works with one argument (#3418)", {
    df = tibble(x=seq(1, 42))
    out = df >> mutate(nt=ntile(n=9))
    exp = df >> mutate(nt=ntile(row_number(), n=9))
    assert out.equals(exp)

    # with pytest.raises(TypeError):
    out = df >> mutate(nt=ntile(row_number(), 9))
    assert out.equals(exp)

    df = group_by(tibble(x=range(1, 43), g=rep(range(1, 8), each=6)), f.g)
    out = df >> mutate(nt=ntile(n=4))
    exp = df >> mutate(nt=ntile(row_number(), n=4))
    assert out.equals(exp)


def test_rank_functions_deal_correctly_with_na():
    # test_that("rank functions deal correctly with NA (#774)", {
    data = tibble(x=c(1, 2, NA, 1, 0, NA))
    res = data >> mutate(
        min_rank=min_rank(f.x),
        percent_rank=percent_rank(f.x),
        dense_rank=dense_rank(f.x),
        cume_dist=cume_dist(f.x),
        ntile=ntile(f.x, 2),
        row_number=row_number(f.x),
    )
    assert all(is_na(res.min_rank[[2, 5]]))
    assert all(is_na(res.dense_rank[[2, 5]]))
    assert all(is_na(res.percent_rank[[2, 5]]))
    assert all(is_na(res.cume_dist[[2, 5]]))
    assert all(is_na(res.ntile[[2, 5]]))
    assert all(is_na(res.row_number[[2, 5]]))

    assert res.percent_rank[[0, 1, 3, 4]].tolist() == c(
        1.0 / 3.0, 1.0, 1.0 / 3.0, 0.0
    )
    assert res.min_rank[[0, 1, 3, 4]].tolist() == c(2, 4, 2, 1)
    assert res.dense_rank[[0, 1, 3, 4]].tolist() == c(2, 3, 2, 1)
    assert res.cume_dist[[0, 1, 3, 4]].tolist() == c(0.75, 1.0, 0.75, 0.25)
    assert res.ntile[[0, 1, 3, 4]].tolist() == c(0, 1, 0, 0)
    assert res.row_number[[0, 1, 3, 4]].tolist() == c(2, 4, 3, 1)

    data = tibble(x=rep(c(1, 2, NA, 1, 0, NA), 2), g=rep([1, 2], each=6))

    res = (
        data
        >> group_by(f.g)
        >> mutate(
            min_rank=min_rank(f.x),
            percent_rank=percent_rank(f.x),
            dense_rank=dense_rank(f.x),
            cume_dist=cume_dist(f.x),
            ntile=ntile(f.x, 2),
            row_number=row_number(f.x),
        )
    )

    assert all(is_na(res.min_rank[[2, 5, 8, 11]]))
    assert all(is_na(res.dense_rank[[2, 5, 8, 11]]))
    assert all(is_na(res.percent_rank[[2, 5, 8, 11]]))
    assert all(is_na(res.cume_dist[[2, 5, 8, 11]]))
    assert all(is_na(res.ntile[[2, 5, 8, 11]]))
    assert all(is_na(res.row_number[[2, 5, 8, 11]]))

    assert (
        res.percent_rank[[0, 1, 3, 4, 6, 7, 9, 10]].tolist()
        == rep(c(1.0 / 3, 1.0, 1.0 / 3, 0.0), 2).tolist()
    )
    assert (
        res.min_rank[[0, 1, 3, 4, 6, 7, 9, 10]].tolist()
        == rep(c(2, 4, 2, 1), 2).tolist()
    )
    assert (
        res.dense_rank[[0, 1, 3, 4, 6, 7, 9, 10]].tolist()
        == rep(c(2, 3, 2, 1), 2).tolist()
    )
    assert (
        res.cume_dist[[0, 1, 3, 4, 6, 7, 9, 10]].tolist()
        == rep(c(0.75, 1, 0.75, 0.25), 2).tolist()
    )
    assert (
        res.ntile[[0, 1, 3, 4, 6, 7, 9, 10]].tolist()
        == rep(c(0, 1, 0, 0), 2).tolist()
    )
    assert (
        res.row_number[[0, 1, 3, 4, 6, 7, 9, 10]].tolist()
        == rep(c(2, 4, 3, 1), 2).tolist()
    )


def test_lag_lead_work_on_factors_inside_mutate():
    # test_that("lag and lead work on factors inside mutate (#955)", {
    test_factor = factor(rep(list("ABC"), each=3))
    exp_lag = test_factor != lag(test_factor)
    exp_lead = test_factor != lead(test_factor)

    test_df = tibble(test=test_factor)
    res = test_df >> mutate(
        is_diff_lag=f.test != lag(f.test),
        is_diff_lead=f.test != lead(f.test),
    )
    assert_iterable_equal(exp_lag, res.is_diff_lag)
    assert_iterable_equal(exp_lead, res.is_diff_lead)


def test_lag_handles_default_argument_in_mutate():
    # test_that("lag handles default argument in mutate (#915)", {
    blah = tibble(x1=c(5, 10, 20, 27, 35, 58, 5, 6), y=range(8, 0, -1))
    blah = mutate(
        blah,
        x2=f.x1 - lag(f.x1, n=1, default=0),
        x3=f.x1 - lead(f.x1, n=1, default=0),
        x4=lag(f.x1, n=1, order_by=f.y),
        x5=lead(f.x1, n=1, order_by=f.y),
    )
    assert_iterable_equal(blah.x2, blah.x1 - lag(blah.x1, n=1, default=0))
    assert_iterable_equal(blah.x3, blah.x1 - lead(blah.x1, n=1, default=0))

    assert_iterable_equal(blah.x4, lag(blah.x1, n=1, order_by=blah.y))
    assert_iterable_equal(blah.x5, lead(blah.x1, n=1, order_by=blah.y))


# # dplyr fIXME: this should only fail if strict checking is on.
# # test_that("window functions fail if db doesn't support windowing", {
# #   df_sqlite <- temp_load(temp_srcs("sqlite"), df)$sql %>% group_by(g)
# #   ok <- collect(df_sqlite %>% mutate(x > 4))
# #   expect_equal(nrow(ok), 10)
# #
# #   expect_error(df_sqlite %>% mutate(x > mean(x)), "does not support")
# #   expect_error(df_sqlite %>% mutate(r = row_number()), "does not support")
# # })

def test_mutate_handles_matrix_columns():
    df = tibble(a=rep([1,2,3], each=2), b=range(1,7))

    df_regular = mutate(df, b=scale(f.b))
    df_grouped = mutate(group_by(df, f.a), b=scale(f.b))
    df_rowwise = mutate(rowwise(df), b=scale(f.b))

    assert dim(df_regular >> pull(f.b)) == (6,1)
    assert dim(df_grouped >> pull(f.b)) == (6,1)
    assert dim(df_rowwise >> pull(f.b)) == (6,1)
