from datar.base.verbs import complete_cases, proportions
import pytest
from pandas import DataFrame
from datar2.base import *
from datar2.tibble import tibble

from ..conftest import assert_iterable_equal

def test_rowcolnames():
    df = DataFrame(dict(x=[1,2,3]))
    assert colnames(df) == ['x']
    assert rownames(df).tolist() == [0, 1, 2]
    df = DataFrame([1,2,3], index=['a', 'b', 'c'])
    assert colnames(df) == [0]
    assert rownames(df).tolist() == ['a', 'b', 'c']

    df = colnames(df, ['y'])
    assert_iterable_equal(df.columns, ['y'])

    df = colnames(df, ['y'], nested=False)
    assert_iterable_equal(df.columns, ['y'])

    assert_iterable_equal(colnames(df, nested=False), ['y'])

    df = rownames(df, ['a', 'b', 'c'])
    assert_iterable_equal(df.index, ['a', 'b', 'c'])

    df = tibble(a=tibble(x=1, y=1), b=tibble(u=2, v=3), z=2)
    df = df >> colnames(['c', 'd', 'w'], nested=True)
    assert_iterable_equal(df.columns, ['c$x', 'c$y', 'd$u', 'd$v', 'w'])


def test_diag():
    out = dim(3 >> diag())
    assert out == (3,3)
    out = dim(10 >> diag(3, 4))
    assert out == (3,4)
    x = c(1j,2j) >> diag()
    assert x.iloc[0,0] == 0+1j
    assert x.iloc[0,1] == 0+0j
    assert x.iloc[1,0] == 0+0j
    assert x.iloc[1,1] == 0+2j
    x = TRUE >> diag(3)
    assert sum(x.values.flatten()) == 3
    x = c(2,1) >> diag(4)
    assert_iterable_equal(x >> diag(), [2,1,2,1])

    with pytest.raises(ValueError):
        x >> diag(3, 3)

    x = 1 >> diag(4)
    assert_iterable_equal(x >> diag(3) >> diag(), [3,3,3,3])

def test_ncol():
    df = tibble(x=tibble(a=1, b=2))
    assert ncol(df) == 1
    assert ncol(df, nested=False) == 2

def test_t():
    df = tibble(x=1, y=2)
    out = t(df)
    assert out.shape == (2, 1)
    assert_iterable_equal(out.index, ['x', 'y'])

def test_names():
    assert_iterable_equal(names(tibble(x=1)), ['x'])
    # assert_iterable_equal(names({'x': 1}), ['x'])
    # assert names({'x':1}, ['y']) == {'y': 1}

def test_setdiff():
    assert_iterable_equal(setdiff(1,2), [1])
    assert_iterable_equal(setdiff([1,2], [2]), [1])


def test_intersect():
    assert_iterable_equal(intersect(1,2), [])
    assert_iterable_equal(intersect([1,2], [2]), [2])

def test_union():
    assert_iterable_equal(union(1,2), [1,2])
    assert_iterable_equal(union([1,2], [2]), [1,2])

def test_setequal():
    assert setequal([1,2], [2,1])
    assert setequal(1, 1)

def test_duplicated():
    assert_iterable_equal(
        duplicated([1,1,-1,-1,2,2], incomparables=[-1]),
        [False, True, False, False, False, True]
    )
    assert_iterable_equal(
        duplicated([1,1,2,2], from_last=True),
        [True, False, True, False]
    )
    df = tibble(x=[1,1,2,2])
    assert_iterable_equal(duplicated(df), [False, True, False, True])

def test_max_col():
    df = tibble(
        a = [1,7,4],
        b = [8,5,3],
        c = [6,2,9],
        d = [8,7,9]
    )
    assert_iterable_equal(
        max_col(df[["a", "b", "c"]], "random"),
        [1,0,2]
    )
    out = max_col(df, "random")
    assert out[0] in [1,3]
    assert out[1] in [0,3]
    assert out[2] in [2,3]
    assert_iterable_equal(
        max_col(df, "first"),
        [1,0,2]
    )
    assert_iterable_equal(
        max_col(df, "last"),
        [3,3,3]
    )

def test_complete_cases():
    df = tibble(
        a = [NA, 1, 2],
        b = [4, NA, 6],
        c = [7, 8, 9],
    )
    out = complete_cases(df)
    assert_iterable_equal(out, [False, False, True])

def test_append():
    out = append([1], 2)
    assert_iterable_equal(out, [1,2])
    out = append([1,2,3], 4, after=None)
    assert_iterable_equal(out, [4, 1,2,3])

def test_proportions():
    out = proportions([1,2,3,4])
    assert_iterable_equal(out, [.1,.2,.3,.4])

    df = tibble(a=[1,2], b=[3,4])
    proportions(df).equals(tibble(a=[.1,.2], b=[.3,.4]))
    proportions(df, 1).equals(tibble(a=[1./4, 2./6], b=[3./4, 4./6]))
    proportions(df, 2).equals(tibble(a=[1./3, 2./3], b=[3./7, 4./7]))
    proportions(df, 3).equals(tibble(a=[1,1], b=[1,1]))
