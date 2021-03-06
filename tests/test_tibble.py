import pytest

from pandas import DataFrame
from pipda import Symbolic
from datar.tibble import *

from .conftest import assert_equal

def test_tibble():
    f = Symbolic()

    df1 = tibble(x=1, y=2)
    df2 = DataFrame(dict(x=[1], y=[2]))
    assert df1.equals(df2)
    assert_equal(df1.columns.tolist(), df2.columns.tolist())

    df1 = tibble(x=1, y=2, _name_repair=str.upper)
    assert_equal(df1.columns.tolist(), ['X', 'Y'])

    df1 = tibble(x=1, y=2, _name_repair=['X', 'Y'])
    assert_equal(df1.columns.tolist(), ['X', 'Y'])


    x = 1
    df = tibble(x, x, x, _name_repair="unique")
    assert_equal(df.columns.tolist(), ["x_1", "x_2", "x_3"])

    df = tibble(x, f.x*2)
    assert_equal(df.values.flatten(), [1,2])
    df = tibble(x, y=f.x*2)
    assert_equal(df.values.flatten(), [1,2])

    df = tibble(x, df, _name_repair="unique")
    assert_equal(df.values.flatten(), [1,1,2])
    df = tibble(x, z=df, _name_repair="unique")
    assert_equal(df.values.flatten(), [1,1,1,2])


    def name_repair(name, raw_names, new_names):
        if name not in new_names:
            return name
        return f'{name}_1'

    df = tibble(x, x, _name_repair=name_repair)
    assert_equal(df.columns.tolist(), ["x", "x_1"])

    with pytest.raises(ValueError, match="duplicated"):
        tibble(x, x)

    x = {'a': 1}
    df = tibble(x['a'], x['a'], x['a'], _name_repair="universal")
    assert_equal(df.columns.tolist(), ["x_a_1", "x_a_2", "x_a_3"])

    with pytest.raises(ValueError, match='_name_repair'):
        tibble(x['a'], _name_repair=True)

def test_tribble():
    f = Symbolic()
    df = tribble(
        f.colA, f.colB,
        "a",    1,
        "b",    2,
        "c",    3,
    )
    assert_equal(df.columns.tolist(), ['colA', 'colB'])
    assert_equal(df.values.flatten(), ['a', 1, 'b', 2, 'c', 3])
