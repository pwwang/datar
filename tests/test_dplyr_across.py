"""Grabbed from
https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-across.R"""
import numpy
import pytest

from datar.all import *

def test_on_one_column():
    df = tibble(x=1)
    out = df >> mutate(across())
    assert out.equals(df)

def test_not_selecting_grouping_var():
    df = tibble(g = 1, x = 1)
    out = df >> group_by(f.g) >> summarise(x = across(everything())) >> pull()
    expected = tibble(x=1)
    assert out.to_frame().equals(expected)

def test_names_output():
    gf = tibble(x = 1, y = 2, z = 3, s = "") >> group_by(f.x)

    out = gf >> summarise(across())
    assert out.columns.tolist() == c("x", "y", "z", "s")

    out = gf >> summarise(across(_names = "id_{_col}"))
    assert out.columns.tolist() == c("x", "id_y", "id_z", "id_s")

    out = gf >> summarise(across(where(is_numeric), mean))
    assert out.columns.tolist() == c("x", "y", "z")

    out = gf >> summarise(across(where(is_numeric), mean, _names="mean_{_col}"))
    assert out.columns.tolist() == c("x", "mean_y", "mean_z")

    out = gf >> summarise(across(
        where(is_numeric),
        {'mean': mean, 'sum': sum}
    ))
    assert out.columns.tolist() == c("x", "y_mean", "y_sum", "z_mean", "z_sum")

    # Different from R's list
    out = gf >> summarise(across(
        where(is_numeric),
        {'mean': mean, 1: sum}
    ))
    assert out.columns.tolist() == c("x", "y_mean", "y_1", "z_mean", "z_1")

    # Different from R's list
    out = gf >> summarise(across(
        where(is_numeric),
        {0: mean, 'sum': sum}
    ))
    assert out.columns.tolist() == c("x", "y_0", "y_sum", "z_0", "z_sum")

    out = gf >> summarise(across(
        where(is_numeric),
        [mean, sum]
    ))
    assert out.columns.tolist() == c("x", "y_1", "y_2", "z_1", "z_2")

    out = gf >> summarise(across(
        where(is_numeric),
        [mean, sum],
        _names='{_col}_{_fn0}'
    ))
    assert out.columns.tolist() == c("x", "y_0", "y_1", "z_0", "z_1")

    out = gf >> summarise(across(
        where(is_numeric),
        {'mean': mean, 'sum': sum},
        _names="{_fn}_{_col}"
    ))
    assert out.columns.tolist() == c("x", "mean_y", "sum_y", "mean_z", "sum_z")

def test_result_locations_aligned_with_column_names():
    df = tibble(x=[1,2], y=['a', 'b'])
    expect = tibble(
        x_cls=numpy.int64,
        x_type=TRUE,
        y_cls=object,
        y_type=FALSE
    )
    x = df >> summarise(across(
        everything(),
        {'cls': lambda x: x.dtype, 'type': is_numeric}
    ))
    assert x.equals(expect)

def test_to_functions():
    df = tibble(x = c(1, NA)) # -> float

    out = df >> summarise(across(everything(), mean, na_rm = TRUE))
    expect = tibble(x = 1.0)
    assert out.equals(expect)

    out = df >> summarise(across(
        everything(),
        dict(mean=mean, median=median),
        na_rm = TRUE
    ))
    expect = tibble(x_mean=1.0, x_median=1.0)
    assert out.equals(expect)

# unnamed arguments not supported

def test_kwargs():
    df = tibble(x = c(1, 2))

    tail_n = lambda d, n: d >> tail(n)
    out = df >> summarise(across(f.x, tail_n, n=1)) >> drop_index()
    expect = tibble(x=2)
    assert out.equals(expect)

def test_works_sequentially():
    from pipda import register_func, Context
    n_col = register_func(
        lambda data, acr: len(acr.evaluate(Context.EVAL, data))
    )

    df = tibble(a = 1)
    out = df >> mutate(
        x = n_col(across(where(is_numeric))),
        y = n_col(across(where(is_numeric)))
    )
    expect = tibble(a=1, x=1, y=2)
    assert out.equals(expect)

    out = df >> mutate(
        a = "x",
        y = n_col(across(where(is_numeric)))
    )
    expect = tibble(a="x", y=0)
    assert out.equals(expect)

def test_original_ordering():
    df = tibble(a=1, b=2)
    out = df >> mutate(a=2, x=across())
    assert out.columns.tolist() == ['a', 'b', 'x$a', 'x$b']
