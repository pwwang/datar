from pandas.core.groupby.generic import DataFrameGroupBy
from datar.core.exceptions import ColumnNameInvalidError
import pytest

from pipda import Symbolic
from datar.dplyr import *
from datar.tibble import tibble

from .conftest import assert_equal

def test_arrange():
    f = Symbolic()
    df = tibble(x=list('abc'), y=range(3))
    df = df >> arrange(desc(f.y))
    assert_equal(df.values.flatten(), ['c', 2, 'b', 1, 'a', 0])
    df = df >> arrange(f.y)
    assert_equal(df.values.flatten(), ['a', 0, 'b', 1, 'c', 2])

    df = df >> arrange(across(f.y, desc))
    assert_equal(df.values.flatten(), ['c', 2, 'b', 1, 'a', 0])

    df = df >> arrange(across(f.y))
    assert_equal(df.values.flatten(), ['a', 0, 'b', 1, 'c', 2])

    df = df >> arrange(desc(f.y))

    df = df.groupby('x')
    df = df >> arrange(desc(f.y))

    assert_equal(df.obj.values.flatten(), ['c', 2, 'b', 1, 'a', 0])

    df = df >> arrange(desc(f.y), _by_group=True)
    assert_equal(df.obj.values.flatten(), ['a', 0, 'b', 1, 'c', 2])

def test_mutate():
    f = Symbolic()
    df = tibble(x=list('abc'), y=range(3))
    df = df >> mutate(z=1, _before=0)
    assert_equal(df.values.flatten(), [1, 'a', 0, 1, 'b', 1, 1, 'c', 2])
    df = df >> mutate(z=None)
    assert_equal(df.values.flatten(), ['a', 0, 'b', 1, 'c', 2])
    df = df >> mutate(z=1, _after=-1)
    assert_equal(df.values.flatten(), ['a', 0, 1, 'b', 1, 1, 'c', 2, 1])
    df = df >> mutate({'z': 2})
    assert_equal(df.values.flatten(), ['a', 0, 2, 'b', 1, 2, 'c', 2, 2])

    df = df >> rowwise() >> mutate(z=f.y+f.z)
    assert_equal(df.values.flatten(), ['a', 0, 2, 'b', 1, 3, 'c', 2, 4])

    # df is still rowwise
    df = df >> mutate(z=c_across([f.y, f.z], lambda row: str(row.y+row.z)))
    assert_equal(df.values.flatten(), ['a', 0, '2', 'b', 1, '4', 'c', 2, '6'])

    df = df >> mutate(z=None)
    df = df >> mutate(z=2*f.y, _keep="unused")
    assert_equal(df.values.flatten(), ['a', 0, 'b', 2, 'c', 4])

    df = df >> mutate(y=f.z//2, _keep="used")
    assert_equal(df.values.flatten(), [0, 0, 2, 1, 4, 2])

    df1 = df >> mutate(w=f.z//2, _keep="none")
    assert_equal(df1.values.flatten(), [0, 1, 2])

    df2 = df >> transmutate(w=f.z//2)
    assert_equal(df2.values.flatten(), [0, 1, 2])


def test_relocate():
    f = Symbolic()
    df = tibble(x=list('abc'), y=range(3))
    with pytest.raises(ColumnNameInvalidError):
        df >> relocate(f.x, _before=0, _after=1)

    df = df >> relocate(f.y)
    assert_equal(df.values.flatten(), [0, 'a', 1, 'b', 2, 'c'])

    df = df.groupby('x')
    df = df >> relocate(f.x)
    assert isinstance(df, DataFrameGroupBy)

def test_select():
    f = Symbolic()
    df = tibble(x=list('abc'), y=range(3), z=[3,8,9])

    df = df >> select(f.x, f.y)
    assert_equal(df.values.flatten(), ['a', 0, 'b', 1, 'c', 2])

    df = df >> select(f.x, z=f.y)
    assert_equal(df.columns.tolist(), ['x', 'z'])

def test_group():
    f = Symbolic()
    df = tibble(x=list('abc'), y=range(3))
    df = df >> group_by(f.x)


