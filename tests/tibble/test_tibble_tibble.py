# https://github.com/tidyverse/tibble/blob/master/tests/testthat/test-tibble.R

from pandas import DataFrame
from pandas.testing import assert_frame_equal
import pytest

from datar import f
from datar.core.exceptions import DataUnrecyclable
from datar.tibble import *
from datar.base import c, nrow, rep, dim, sum, diag, NA, letters, LETTERS, NULL, seq
from datar.dplyr import pull, mutate
from datar.datasets import iris
from .conftest import assert_iterable_equal


def test_mixed_numbering():
    df = tibble(a=f[1:5], b=seq(5), c=c(1,2,3,[4,5]), d=c(f[1:3], c(4, 5)))
    exp = tibble(a=seq(1,5), b=f.a, c=f.a, d=f.a)
    assert_frame_equal(df, exp)


def test_correct_rows():
    out = tibble(value=range(1,11)) >> nrow()
    assert out == 10
    out = tibble(value=range(1,11), name="recycle_me") >> nrow()
    assert out == 10
    out = tibble(name="recycle_me", value=range(1,11)) >> nrow()
    assert out == 10
    out = tibble(name="recycle_me", value=range(1,11), value2=range(11,21)) >> nrow()
    assert out == 10
    out = tibble(value=range(1,11), name="recycle_me", value2=range(11,21)) >> nrow()
    assert out == 10

def test_null_none_ignored():
    out = tibble(a=None)
    expect = tibble()
    assert_frame_equal(out, expect)

    out = tibble(a_=None, a=1)
    expect = tibble(a=1)
    assert_frame_equal(out, expect)

    out = tibble(a=None, b=1, c=[2,3])
    expect = tibble(b=1, c=[2,3])
    assert_frame_equal(out, expect)

    out = tibble(None, b=1, c=[2,3])
    expect = tibble(b=1, c=[2,3])
    assert_frame_equal(out, expect)

def test_recycle_scalar_or_len1_vec():
    out = tibble(value=range(1,11)) >> nrow()
    assert out == 10
    out = tibble(value=range(1,11), y=1) >> nrow()
    assert out == 10
    out = tibble(value=range(1,11), y=[1]) >> nrow()
    assert out == 10
    with pytest.raises(DataUnrecyclable):
        tibble(value=range(1,11), y=[1,2,3])

def test_recycle_nrow1_df():
    out = tibble(x=range(1,11), y=tibble(z=1))
    expect = tibble(x=range(1,11), y=tibble(z=rep(1,10)))
    assert_frame_equal(out, expect)

    out = tibble(y=tibble(z=1), x=range(1,11))
    expect = tibble(y=tibble(z=rep(1,10)), x=range(1,11))
    assert_frame_equal(out, expect)

    out = tibble(x=1, y=tibble(z=range(1,11)))
    expect = tibble(x=rep(1,10), y=tibble(z=range(1,11)))
    assert_frame_equal(out, expect)

    out = tibble(y=tibble(z=range(1,11)), x=1)
    expect = tibble(y=tibble(z=range(1,11)), x=rep(1,10))
    assert_frame_equal(out, expect)

def test_missing_names():
    x = range(1,11)
    df = tibble(x, y=x)
    assert df.columns.tolist() == ['x', 'y']

def test_empty():
    zero = tibble()
    d = zero >> dim()
    assert d == (0, 0)
    assert zero.columns.tolist() == []

def test_tibble_with_series():
    df = tibble(x=1)
    df2 = tibble(df.x)
    assert_frame_equal(df, df2)

def test_hierachical_names():
    foo = tibble(x=tibble(y=1,z=2))
    assert foo.columns.tolist() == ['x$y', 'x$z']
    pulled = foo >> pull(f.x)
    assert pulled.columns.tolist() == ['y', 'z']

    foo = tibble(x=dict(y=1,z=2))
    assert foo.columns.tolist() == ['x$y', 'x$z']
    pulled = foo >> pull(f.x)
    assert pulled.columns.tolist() == ['y', 'z']


def test_meta_attrs_preserved():
    foo = tibble(x=1)
    foo.attrs['a'] = 1
    bar = tibble(foo)
    assert bar.attrs['a'] == 1

def test_f_pronoun():
    foo = tibble(a=1, b=f.a)
    bar = tibble(a=1, b=1)
    assert foo.equals(bar)

def test_nest_df():
    out = tibble(a=1, b=tibble(x=2))
    assert_iterable_equal(out.columns, ['a', 'b$x'])

def test_mutate_semantics():
    foo = tibble(a=[1,2], b=1, c=f.b / sum(f.b))
    bar = tibble(a=[1,2], b=[1,1], c=[.5,.5])
    assert foo.equals(bar)

    foo = tibble(b=1, a=[1,2], c=f.b / sum(f.b))
    bar = tibble(b=[1,1], a=[1,2], c=[.5,.5])
    assert foo.equals(bar)

    foo = tibble(b=1.0, c=f.b / sum(f.b), a=[1,2])
    bar = tibble(b=[1.0,1.0], c=[1.0,1.0], a=[1,2])
    assert foo.equals(bar)

    x = 1
    df = tibble(x, f.x*2)
    assert_frame_equal(df, tibble(x=1, **{'f.x*2': 2}))

# TODO: units preseved when recycled

def test_auto_splicing_anonymous_tibbles():
    df = tibble(a=1, b=2)
    out = tibble(df)
    assert out.equals(df)

    out = tibble(df, c=f.b)
    expect = tibble(a=1,b=2,c=2)
    assert_frame_equal(out, expect)

def test_coerce_dict_of_df():
    df = tibble(x=range(1,11))
    out = tibble(dict(x=df)) >> nrow()
    assert out == 10

    out = tibble(dict(x=diag(5))) >> nrow()
    assert out == 5

def test_subsetting_correct_nrow():
    df = tibble(x=range(1,11))
    out = tibble(x=df).loc[:4,:]
    expect = tibble(x=df.loc[:4,:])
    assert_frame_equal(out, expect)

def test_one_row_retains_column():
    out = tibble(y=diag(5)).loc[0, :]
    expect = tibble(y=diag(5).loc[0, :].values)
    assert (out.values.flatten() == expect.values.flatten()).all()

# tribble

def test_tribble():
    out = tribble(
        f.colA, f.colB,
        "a",    1,
        "b",    2
    )
    expect = tibble(colA=["a", "b"], colB=[1,2])
    assert_frame_equal(out, expect)

    out = tribble(
        f.colA, f.colB, f.colC, f.colD,
        1,2,3,4,
        5,6,7,8
    )
    expect = tibble(
        colA=[1,5],
        colB=[2,6],
        colC=[3,7],
        colD=[4,8],
    )
    assert_frame_equal(out, expect)

    out = tribble(
        f.colA, f.colB,
        1,6,
        2,7,
        3,8,
        4,9,
        5,10
    )
    expect = tibble(
        colA=[1,2,3,4,5],
        colB=[6,7,8,9,10]
    )
    assert_frame_equal(out, expect)

# trailing comma is a python feature
def test_trailing_comma():
    out = tribble(
        f.colA, f.colB,
        "a",    1,
        "b",    2, # <--
    )
    expect = tibble(colA=["a", "b"], colB=[1,2])
    assert_frame_equal(out, expect)

# todo: handle column as class

def test_non_atomic_value():
    out = tribble(f.a, f.b, NA, "A", letters, LETTERS[1:])
    expect = tibble(a=[NA, letters], b=["A", LETTERS[1:]])
    assert_frame_equal(out, expect)

    out = tribble(f.a, f.b, NA, NULL, 1, 2)
    expect = tibble(a=[NA, 1], b=[NULL, 2])
    assert_frame_equal(out, expect)

def test_errors():
    with pytest.raises(ValueError):
        tribble(1, 2, 3)
    with pytest.raises(ValueError):
        tribble("a", "b", 1, 2)

    with pytest.raises(ValueError):
        tribble(f.a, f.b, f.c, 1,2,3,4,5)

def test_dict_value():
    out = tribble(f.x, f.y, 1, dict(a=1), 2, dict(b=2))
    assert out.x.values.tolist() == [1,2]
    assert out.y.values.tolist() == [dict(a=1), dict(b=2)]

def test_empty_df():
    out = tribble(f.x, f.y)
    expect = tibble(x=[], y=[])
    assert out.columns.tolist() == ['x', 'y']
    assert out.shape == (0, 2)
    assert expect.columns.tolist() == ['x', 'y']
    assert expect.shape == (0, 2)

def test_0x0():
    df = tibble()
    expect = tibble()
    assert df.equals(expect)

def test_names_not_stripped():
    # different from R
    df = tribble(f.x, dict(a=1))
    out = df >> pull(f.x, to='list')
    assert out == [dict(a=1)]

def test_dup_cols():
    df = tribble(f.x, f.x, 1, 2)
    # assert df.columns.tolist() == ['x', 'x']

    x = 1
    df = tibble(x, x, _name_repair='minimal')
    assert df.columns.tolist() == ['x', 'x']

# fibble -------------------------------------------
def test_fibble():
    df = tibble(x=[1,2]) >> mutate(tibble(y=f.x))
    assert df.equals(tibble(x=[1,2], y=[1,2]))

# tibble_row ---------------------------------------
def test_tibble_row():
    # df = tibble_row(a=1, b=[[2,3]])
    # assert_frame_equal(df, tibble(a=1, b=[[2,3]]))

    df = tibble_row(iris.iloc[[0], :])
    assert_frame_equal(df, tibble(iris.iloc[[0], :]))

    with pytest.raises(ValueError):
        tibble_row(a=1, b=[2,3])

    with pytest.raises(ValueError):
        tibble_row(iris.iloc[1:3, :])

    df = tibble_row()
    assert df.shape == (1, 0)


# zibble ------------------------------------------------
def test_zibble():
    # plain
    df = zibble(['a', 'b'], [1,2])
    assert_frame_equal(df, DataFrame([[1,2]], columns=['a', 'b']))

    # recycle early variable
    df = zibble(['a', 'b'], [1, f.a + 1])
    assert_frame_equal(df, DataFrame([[1,2]], columns=['a', 'b']))

    # lengths not match
    with pytest.raises(ValueError, match="not the same"):
        zibble(['a'], [1,2])

    # no name
    df = zibble([None], [1])
    assert_frame_equal(df, DataFrame([1], columns=['_Var0']))

    # no name df gets expanded
    df0 = DataFrame({'a': [1]})
    df = zibble([None], [df0])
    assert_frame_equal(df, df0)

    # nest df
    df = zibble(['a'], [df0])
    assert_iterable_equal(df.columns, ['a$a'])

    # dict
    df = zibble([None], [{'a': 1, 'b': 2}])
    assert_frame_equal(df, DataFrame([[1,2]], columns=['a', 'b']))

    df = zibble(
        list('abcd'),
        [f[1:5], seq(5), c(1,2,3,[4,5]), c(f[1:3], c(4,5))]
    )
    assert_frame_equal(df, DataFrame(
        [[1]*4, [2]*4, [3]*4, [4]*4, [5]*4],
        columns=list('abcd')
    ))
