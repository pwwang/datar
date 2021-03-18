"""Grabbed from
https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-across.R"""
import numpy
from pipda import register_func, Context
import pytest
from varname.helpers import register

from datar.all import *

@register_func
def n_col(data, acr):
    """ncol of an across"""
    return acr.evaluate(data, Context.EVAL) >> ncol()

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
    assert out.columns.tolist() == ["x", "y", "z", "s"]

    out = gf >> summarise(across(_names = "id_{_col}"))
    assert out.columns.tolist() == ["x", "id_y", "id_z", "id_s"]

    out = gf >> summarise(across(where(is_numeric), mean))
    assert out.columns.tolist() == ["x", "y", "z"]

    out = gf >> summarise(across(where(is_numeric), mean, _names="mean_{_col}"))
    assert out.columns.tolist() == ["x", "mean_y", "mean_z"]

    out = gf >> summarise(across(
        where(is_numeric),
        {'mean': mean, 'sum': sum}
    ))
    assert out.columns.tolist() == ["x", "y_mean", "y_sum", "z_mean", "z_sum"]

    # Different from R's list
    out = gf >> summarise(across(
        where(is_numeric),
        {'mean': mean, 1: sum}
    ))
    assert out.columns.tolist() == ["x", "y_mean", "y_1", "z_mean", "z_1"]

    # Different from R's list
    out = gf >> summarise(across(
        where(is_numeric),
        {0: mean, 'sum': sum}
    ))
    assert out.columns.tolist() == ["x", "y_0", "y_sum", "z_0", "z_sum"]

    out = gf >> summarise(across(
        where(is_numeric),
        [mean, sum]
    ))
    assert out.columns.tolist() == ["x", "y_1", "y_2", "z_1", "z_2"]

    out = gf >> summarise(across(
        where(is_numeric),
        [mean, sum],
        _names='{_col}_{_fn0}'
    ))
    assert out.columns.tolist() == ["x", "y_0", "y_1", "z_0", "z_1"]

    out = gf >> summarise(across(
        where(is_numeric),
        {'mean': mean, 'sum': sum},
        _names="{_fn}_{_col}"
    ))
    assert out.columns.tolist() == ["x", "mean_y", "sum_y", "mean_z", "sum_z"]

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

    df = tibble(a = 1)
    # out = df >> mutate(
    #     x = n_col(across(where(is_numeric))),
    #     y = n_col(across(where(is_numeric)))
    # )
    # expect = tibble(a=1, x=1, y=2)
    # assert out.equals(expect)

    out = df >> mutate(
        a = "x",
        y = n_col(across(where(is_numeric)))
    )
    expect = tibble(a="x", y=0)
    print(out)
    assert out.equals(expect)

def test_original_ordering():
    df = tibble(a=1, b=2)
    out = df >> mutate(a=2, x=across())
    assert out.columns.tolist() == ['a', 'b', 'x$a', 'x$b']

def test_error_messages():
    with pytest.raises(ValueError, match='Argument `_fns` of across must be'):
        tibble(x = 1) >> summarise(res=across(where(is_numeric), 42))
    with pytest.raises(TypeError):
        across()
    with pytest.raises(TypeError):
        c_across()

def test_used_twice():
    df = tibble(a = 1, b = 2)
    out = df >> mutate(x = n_col(across(where(is_numeric))) + n_col(across(f.a)))
    expect = tibble(a=1, b=2, x=3)
    assert out.equals(expect)

def test_used_separately():
    df = tibble(a = 1, b = 2)
    out = df >> mutate(x=n_col(across(where(is_numeric))), y=n_col(across(f.a)))
    expect = tibble(a=1, b=2, x=2, y=1)
    assert out.equals(expect)

def test_with_group_id():
    df = tibble(g=[1,2], a=[1,2], b=[3,4]) >> group_by(f.g)

    @register_func(context=None)
    def switcher(data, group_id, across_a, across_b):
        across_a = across_a.evaluate(data, Context.EVAL)
        across_b = across_b.evaluate(data, Context.EVAL)
        return across_a.a if group_id == 0 else across_b.b

    expect = df >> ungroup() >> mutate(x=[1,4])
    out = df >> mutate(x=switcher(cur_group_id(), across(f.a), across(f.b)))
    assert out.obj.equals(expect)

def test_cache_key():
    df = tibble(g=rep([1,2], each=2), a=range(1,5)) >> group_by(f.g)
    tibble2 = register_func(None)(tibble)

    @register_func
    def across_col(data, acr, col):
        return acr.evaluate(data, Context.EVAL)[col]

    out = df >> mutate(
        tibble2(
            x = across_col(across(where(is_numeric), mean), 'a'),
            y = across_col(across(where(is_numeric), max), 'a')
        )
    )
    expect = df >> mutate(x = mean(f.a), y = max(f.a))
    assert out.obj.equals(expect.obj)

def test_reject_non_vectors():
    with pytest.raises(ValueError, match='Argument `_fns` of across must be'):
        tibble(x = 1) >> summarise(across(where(is_numeric), object()))

def test_recycling():
    df = tibble(x=1, y=2)
    out = df >> summarise(across(everything(), lambda col: rep(42, col)))
    expect = tibble(x=rep(42,2), y=rep(42,2))
    assert out.equals(expect)

    df = tibble(x=2, y=3)
    with pytest.raises(ValueError):
        df >> summarise(across(everything(), lambda col: rep(42, col)))

def test_return_one_row():
    # not actually one row, but returns a corresponding series
    df = tibble(x=range(1,43))
    out = df >> mutate(across(c(), as_factor))
    assert out.equals(df)

    out = df >> mutate(y=across(c(), as_factor))
    # empty column in pandas will be NAs
    assert out.y.isna().all()

def test_use_env_var():
    # not a problem, since we use f.y
    df = tibble(x = 1.0, y = 2.4)
    y = "x"
    out = df >> summarise(across(all_of(y), mean))
    expect = tibble(x=1.0)
    assert out.equals(expect)

    out = df >> mutate(across(all_of(y), mean))
    assert out.equals(df)

    out = df >> filter(if_all(all_of(y), lambda col: col < 2))
    assert out.equals(df)

def test_empty_df():
    df = tibble()
    out = df >> mutate(across())
    assert out.equals(df)

def test_mutate_cols_inside_func():
    df = tibble(x = 2, y = 4, z = 8)

    @register_func(None, context=None)
    def data_frame(**kwargs):
        return tibble(**kwargs)

    out = df >> mutate(data_frame(x=f.x/f.y, y=f.y/f.y, z=f.z/f.y))
    # df.y does not work on grouped data
    expect = df >> mutate(across(everything(), lambda col: col/df.y))
    assert out.equals(expect)

def test_summarise_cols_inside_func():
    df = tibble(x = 2, y = 4, z = 8)
    @register_func(None, context=None)
    def data_frame(**kwargs):
        return tibble(**kwargs)

    out = df >> summarise(data_frame(x=f.x/f.y, y=f.y/f.y, z=f.z/f.y))
    expect = df >> summarise(across(everything(), lambda col: col/df.y))
    assert out.equals(expect)

def test_cols_in_lambda():
    df = tibble(x=1.0, y=2.0)
    out = df >> mutate(across('x', lambda x: x/df.y)) >> pull(f.x, to='list')
    assert out == [.5]

def test_if_any_all_enforce_bool():
    d = tibble(x=10, y=10)
    with pytest.raises(TypeError):
        d >> filter(if_all(f[f.x:f.y], identity))

    with pytest.raises(TypeError):
        d >> filter(if_any(f[f.x:f.y], identity))

    with pytest.raises(TypeError):
        d >> mutate(ok=if_all(f[f.x:f.y], identity))
    with pytest.raises(TypeError):
        d >> mutate(ok=if_any(f[f.x:f.y], identity))

def test_if_any_all_in_mutate():
    d = tibble(x = c(1, 5, 10, 10), y = c(0, 0, 0, 10), z = c(10, 5, 1, 10))
    res = d >> mutate(
      any = if_any(f[f.x:f.z], lambda x: x > 8),
      all = if_all(f[f.x:f.z], lambda x: x > 8)
    )
    assert res['any'].eq([True, False, True, True]).all()
    assert res['all'].eq([False, False, False, True]).all()

def test_caching_not_confused():
    @register_func(context=None)
    def add_ifacross(data, acr1, acr2):
        return (
            acr1.evaluate(data, Context.EVAL) |
            acr2.evaluate(data, Context.EVAL)
        )

    df = tibble(x=[1,2,3])
    res = df >> mutate(
        any = add_ifacross(
            if_any(f.x, lambda x: x >= 2),
            if_any(f.x, lambda x: x >= 3)
        ),
        all = add_ifacross(
            if_all(f.x, lambda x: x >= 2),
            if_all(f.x, lambda x: x >= 3)
        ),
    )
    assert res['any'].eq([False, True, True]).all()
    assert res['all'].eq([False, True, True]).all()

def test_if_any_all_na_handling():
    df = expandgrid(x = c(TRUE, FALSE, NA), y = c(TRUE, FALSE, NA))

    out = df >> filter(if_all(c(f.x,f.y), identity))
    expect = df >> filter(f.x & f.y)
    assert out.equals(expect)

    out = df >> filter(if_any(c(f.x,f.y), identity))
    expect = df >> filter(f.x | f.y)
    # Note that out has an extra row:
    #      x     y
    # 6    NaN   True
    # This is because df.fillna(False).any()
    # is not the same as df.x | df.y
    # see: https://pandas.pydata.org/pandas-docs/stable/user_guide/boolean.html
    assert out.iloc[:4].equals(expect)

# reset columns not supported

def test_c_across():
    df = tibble(x=[1,2], y=[3,4])
    # 1. c_across has to be used with rowwise()
    #    otherwise it is the save as across
    # 2. c_across accepts a function, instead of
    #    sum(c_across(x:y)), one should use
    #    c_across(f[f.x:f.y], sum) instead
    out = df >> rowwise() >> summarise(z=c_across([f.x, f.y], list)) >> pull(f.z)
    assert out[0] == [1,3]
    assert out[1] == [2,4]