# https://github.com/tidyverse/tibble/blob/master/tests/testthat/test-tibble.R
from datar.base.funcs import seq_along
import pytest

from datar import f
from datar.tibble import *
from datar.base import nrow, rep, dim, sum, diag, NA, letters, LETTERS, NULL, seq
from datar.dplyr import pull, mutate
from datar.datasets import iris

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
    assert out.equals(expect)

    out = tibble(a_=None, a=1)
    expect = tibble(a=1)
    assert out.equals(expect)

    out = tibble(a=None, b=1, c=[2,3])
    expect = tibble(b=1, c=[2,3])
    assert out.equals(expect)

    out = tibble(None, b=1, c=[2,3])
    expect = tibble(b=1, c=[2,3])
    assert out.equals(expect)

def test_recycle_scalar_or_len1_vec():
    out = tibble(value=range(1,11)) >> nrow()
    assert out == 10
    out = tibble(value=range(1,11), y=1) >> nrow()
    assert out == 10
    out = tibble(value=range(1,11), y=[1]) >> nrow()
    assert out == 10
    with pytest.raises(ValueError):
        tibble(value=range(1,11), y=[1,2,3])

def test_recycle_nrow1_df():
    out = tibble(x=range(1,11), y=tibble(z=1))
    expect = tibble(x=range(1,11), y=tibble(z=rep(1,10)))
    assert out.equals(expect)

    out = tibble(y=tibble(z=1), x=range(1,11))
    expect = tibble(y=tibble(z=rep(1,10)), x=range(1,11))
    assert out.equals(expect)

    out = tibble(x=1, y=tibble(z=range(1,11)))
    expect = tibble(x=rep(1,10), y=tibble(z=range(1,11)))
    assert out.equals(expect)

    out = tibble(y=tibble(z=range(1,11)), x=1)
    expect = tibble(y=tibble(z=range(1,11)), x=rep(1,10))
    assert out.equals(expect)

def test_missing_names():
    x = range(1,11)
    df = tibble(x, y=x)
    assert df.columns.tolist() == ['x', 'y']

def test_empty():
    zero = tibble()
    d = zero >> dim()
    assert d == (0, 0)
    assert zero.columns.tolist() == []

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

# TODO: units preseved when recycled

def test_auto_splicing_anonymous_tibbles():
    df = tibble(a=1, b=2)
    out = tibble(df)
    assert out.equals(df)

    out = tibble(df, c=f.b)
    expect = tibble(a=1,b=2,c=2)
    assert out.equals(expect)

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
    assert out.equals(expect)

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
    assert out.equals(expect)

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
    assert out.equals(expect)

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
    assert out.equals(expect)

# trailing comma is a python feature
def test_trailing_comma():
    out = tribble(
        f.colA, f.colB,
        "a",    1,
        "b",    2, # <--
    )
    expect = tibble(colA=["a", "b"], colB=[1,2])
    assert out.equals(expect)

# todo: handle column as class

def test_non_atomic_value():
    out = tribble(f.a, f.b, NA, "A", letters, LETTERS[1:])
    expect = tibble(a=[NA, letters], b=["A", LETTERS[1:]])
    assert out.equals(expect)

    out = tribble(f.a, f.b, NA, NULL, 1, 2)
    expect = tibble(a=[NA, 1], b=[NULL, 2])
    assert out.equals(expect)

def test_errors():
    with pytest.raises(ValueError):
        tribble(1, 2, 3)
    with pytest.raises(ValueError):
        tribble("a", "b", 1, 2)

    out = tribble(f.a, f.b, f.c, 1,2,3,4,5)
    # missing values filled with NA, unlike R
    expect = tibble(a=[1,4], b=[2,5], c=[3,NA])
    assert out.fillna(0).equals(expect.fillna(0))

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
    assert df.columns.tolist() == ['x', 'x']

    x = 1
    df = tibble(x, x, _name_repair='minimal')
    assert df.columns.tolist() == ['x', 'x']

# fibble -------------------------------------------
def test_fibble():
    df = tibble(x=[1,2]) >> mutate(fibble(y=f.x))
    assert df.equals(tibble(x=[1,2], y=[1,2]))

# enframe ------------------------------------------

def test_can_convert_list():
    df = enframe(seq(3,1))
    assert df.equals(tibble(name=seq(1,3), value=seq(3,1)))

def test_can_convert_dict():
    df = enframe(dict(a=2, b=1))
    assert df.equals(tibble(name=['a','b'], value=[2,1]))

def test_can_convert_empty():
    df = enframe([])
    assert df.shape == (0, 2)
    assert df.columns.tolist() == ['name', 'value']

    df = enframe(None)
    assert df.shape == (0, 2)
    assert df.columns.tolist() == ['name', 'value']

def test_can_use_custom_names():
    df = enframe(letters, name="index", value="letter")
    assert df.equals(tibble(index=seq_along(letters), letter=letters))

def test_can_enframe_without_names():
    df = enframe(letters, name=None, value="letter")
    assert df.equals(tibble(letter=letters))

def test_cannot_value_none():
    with pytest.raises(ValueError):
        enframe(letters, value=None)

def test_cannot_pass_with_dimensions():
    with pytest.raises(ValueError):
        enframe(iris)

# deframe -----------------------------------------------------------------

def test_can_deframe_2col_data_frame():
    out = deframe(tibble(name = letters[:3], value = seq(3,1)))
    assert out == {"a": 3, "b": 2, "c":1}

def test_can_deframe_1col_data_frame():
    out = deframe(tibble(value=seq(3,1)))
    assert out.tolist() == [3,2,1]

def test_can_deframe_3col_df_with_warning(caplog):
    out = deframe(tibble(name=letters[:3], value=seq(3,1), oops=[1,2,3]))
    assert out == {"a": 3, "b": 2, "c":1}
    assert "one- or two-column" in caplog.text
