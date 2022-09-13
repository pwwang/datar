# tests grabbed from:
# https://github.com/tidyverse/tidyr/blob/HEAD/tests/testthat/test-expand.R
import pytest  # noqa
from datar import f
from datar.base import (
    c,
    nrow,
    factor,
    NA,
    NULL,
    levels,
    dim,
    letters,
    LETTERS,
    rep,
    seq,
)
from datar.tibble import tibble, tribble
from datar.dplyr import pull, group_by, rowwise
from datar.tidyr import (
    expand,
    nesting,
    crossing,
    nest,
    expand_grid,
)
from datar.core.backends.pandas.testing import assert_frame_equal
from ..conftest import assert_iterable_equal

# expand ----------------------------------------------------------------
def test_expand_completes_all_values():
    df = tibble(x=c[1:3], y=c[1:3])
    out = expand(df, f.x, f.y)
    assert_frame_equal(
        out,
        tibble(
            x=[1, 1, 2, 2],
            y=[1, 2, 1, 2],
        ),
    )


def test_multiple_variables_in_one_arg_doesnot_expand():
    df = tibble(x=c[1:3], y=c[1:3])
    out = expand(df, c(f.x, f.y))
    assert nrow(out) == 2


def test_nesting_doesnot_expand_values():
    df = tibble(x=c[1:3], y=c[1:3])
    out = expand(df, nesting(f.x, f.y))
    assert_frame_equal(out, df)


def test_unnamed_dfs_are_flattened():
    df = tibble(x=c[1:3], y=c[1:3])
    out = expand(df, nesting(f.x, f.y))
    assert_iterable_equal(out.x, df.x)

    out = crossing(df)
    assert_iterable_equal(out.x, df.x)

    df = tibble(name=c[1:3], y=c[1:3])
    out = expand(df, nesting(f.name, f.y))
    assert_iterable_equal(out.name, df.name)


def test_named_dfs_are_not_flattened():
    df = tibble(x=c[1:3], y=c[1:3])
    out = expand(df, x=nesting(f.x, f.y)) >> pull(f.x)
    assert_frame_equal(out, df)

    out = crossing(x=df) >> pull(f.x)
    assert_frame_equal(out, df)


def test_expand_works_with_non_standard_colnames():
    df = tribble(f[" x "], f["/y"], 1, 1, 2, 2)
    out = expand(df, f[" x "], f["/y"])
    assert nrow(out) == 4


def test_expand_accepts_expressions():
    df = expand(tibble(), x=[1, 2, 3], y=[3, 2, 1])
    out = crossing(x=[1, 2, 3], y=[3, 2, 1])
    assert_frame_equal(df, out)


def test_expand_respects_groups():
    df = tibble(a=[1, 1, 2], b=[1, 2, 1], c=[2, 1, 1])
    out = df >> group_by(f.a) >> expand(f.b, f.c) >> nest(data=c(f.b, f.c))
    assert_frame_equal(out.data.obj.values[0], crossing(b=[1, 2], c=[1, 2]))
    assert_frame_equal(
        out.data.obj.values[1].reset_index(drop=True), tibble(b=1, c=1)
    )


def test_presevers_ordered_factors():
    df = tibble(a=factor("a", ordered=True))
    out = expand(df, f.a)
    assert out.a.values.ordered


def test_preserves_nas():
    x = c("A", "B", NA)
    out = crossing(x)
    assert_iterable_equal(out.iloc[:, 0], x)


def test_crossing_preserves_factor_levels():
    # NA can't be levels for pandas.Categorical object
    x_na_lev_extra = factor(["a", NA], levels=["a", "b"], exclude=NULL)
    out = crossing(x=x_na_lev_extra)
    assert_iterable_equal(levels(out.x), ["a", "b"])


def test_null_inputs():
    tb = tibble(x=c[1:6])
    out = expand(tb, f.x, y=NULL)
    assert_frame_equal(out, tb)
    out = nesting(x=tb.x, y=NULL)
    assert_frame_equal(out, tb)
    out = crossing(NULL, x=tb.x, y=NULL)
    assert_frame_equal(out, tb)


def test_0len_input_gives_0len_output():
    tb = tibble(x=[])
    assert_frame_equal(tb >> expand(f.x), tb)
    assert_frame_equal(tb >> expand(x=f.x), tb)
    assert_frame_equal(expand(tb, y=NULL), tibble())

    assert_frame_equal(expand_grid(x=[], y=[1, 2, 3]), tibble(x=[], y=[]))


def test_expand_crossing_expand_missing_factor_levels_nesting_doesnot():
    tb = tibble(x=c[1:4], f=factor("a", levels=c("a", "b")))
    expanded = expand(tb, f.x, f.f)
    assert nrow(expanded) == 6
    assert nrow(crossing(x=tb.x, f=tb.f)) == 6
    assert nrow(nesting(x=tb.x, f=tb.f)) == 3


# test_that("expand() reconstructs input dots is empty", {
#   expect_s3_class(expand(mtcars), "data.frame")
#   expect_s3_class(expand(as_tibble(mtcars)), "tbl_df")
# })

# test_that("crossing checks for bad inputs", {
#   expect_error(
#     crossing(x = 1:10, y = quote(a)),
#     class = "vctrs_error_scalar_type"
#   )
# })


def test_crossing_handles_list_columns():
    x = [1, 2]
    y = [[1], [1, 2]]
    out = crossing(x, y)

    assert nrow(out) == 4
    assert_iterable_equal(out.iloc[:, 0], rep(x, each=2))
    assert out.iloc[:, 1].to_list() == [[1], [1, 2]] * 2


def test_expand_grid_can_control_name_repair():
    x = [1, 2]

    out = expand_grid(**{"x.1": x, "x.2": x}, _name_repair="universal")
    assert out.columns.tolist() == ["x__0", "x__1"]


## vars with the same name will get overriden
# test_that("expand_grid can control name_repair", {
#   x <- 1:2

#   if (packageVersion("tibble") > "2.99") {
#     expect_error(expand_grid(x, x), class = "rlang_error")
#   } else {
#     expect_error(expand_grid(x, x), "must not be duplicated")
#   }

#   expect_message(out <- expand_grid(x, x, .name_repair = "unique"), "New names:")
#   expect_named(out, c("x...1", "x...2"))

#   out <- expand_grid(x, x, .name_repair = "minimal")
#   expect_named(out, c("x", "x"))
# })


def test_crossing_nesting_expand_respect_name_repair():
    x = [1, 2]
    out = crossing(**{"x.1": x, "x.2": x}, _name_repair="universal")
    assert out.columns.tolist() == ["x__0", "x__1"]

    out = nesting(**{"x.1": x, "x.2": x}, _name_repair="universal")
    assert out.columns.tolist() == ["x__0", "x__1"]

    df = tibble(x)
    out = df >> expand(**{"x.1": x, "x.2": x}, _name_repair="universal")
    assert out.columns.tolist() == ["x__0", "x__1"]


# # dots_cols supports lazy evaluation --------------------------------------

# test_that("dots_cols evaluates each expression in turn", {
#   out <- dots_cols(x = seq(-2, 2), y = x)
#   expect_equal(out$x, out$y)
# })

# expand_grid ----------------------------------
def test_expand_grid():
    out = expand_grid(x=seq(1, 3), y=[1, 2])
    assert_frame_equal(out, tibble(x=[1, 1, 2, 2, 3, 3], y=[1, 2, 1, 2, 1, 2]))

    out = expand_grid(l1=letters, l2=LETTERS)
    assert dim(out) == (676, 2)

    out = expand_grid(df=tibble(x=c[1:3], y=[2, 1]), z=[1, 2, 3])
    assert_frame_equal(
        out,
        tibble(
            df=tibble(x=[1, 1, 1, 2, 2, 2], y=[2, 2, 2, 1, 1, 1]),
            z=[1, 2, 3, 1, 2, 3],
        ),
    )

    out = expand_grid(
        x1=tibble(a=[1, 2], b=[3, 4]), x2=tibble(a=[5, 6], b=[7, 8])
    )
    assert_frame_equal(
        out,
        tibble(
            x1=tibble(a=[1, 1, 2, 2], b=[3, 3, 4, 4]),
            x2=tibble(a=[5, 6, 5, 6], b=[7, 8, 7, 8]),
        ),
    )


def test_expand_rowwise_df_drops_rowwise():
    df = tibble(x=c[1:3], y=c[1:3])
    rf = rowwise(df)
    out1 = df >> expand(f.x, f.y)
    out2 = rf >> expand(f.x, f.y)
    assert_frame_equal(out1, out2)


def test_flatten_at_0len_val():
    from datar.tidyr.expand import _flatten_at

    out = _flatten_at({"x": [], "y": [1, 2, 3]}, {"x": True, "y": False})
    assert out == {"y": [1, 2, 3]}
