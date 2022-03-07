from pandas import Series
import pytest
import numpy as np
from datar.core import f
from datar.base import dim
from datar.tibble import tibble
from datar.dplyr import pull
from datar.testing import assert_tibble_equal


def test_pull_series_with_name():
    df = tibble(x=1)
    out = df >> pull(f.x, to="frame", name="a")
    assert dim(out) == (1, 1)
    assert out.columns.tolist() == ["a"]

    with pytest.raises(ValueError):
        df >> pull(f.x, to="frame", name=["a", "b"])

    # pull array
    out = df >> pull(f.x, to="array")
    assert isinstance(out, np.ndarray)


def test_pull_series_when_to_equals_series():
    df = tibble(**{"x$a": 1})
    out = df >> pull(f.x, to="series")
    assert len(out) == 1
    assert out.values.tolist() == [1]

    # with name
    out = df >> pull(f.x, to="series", name="a")
    assert out.name == "a"


def test_pull_df():
    df = tibble(**{"x$a": 1, "x$b": 2})
    out = df >> pull(f.x, to="series")
    assert len(out) == 2
    assert out["a"].values.tolist() == [1]
    assert out["b"].values.tolist() == [2]

    with pytest.raises(ValueError):
        df >> pull(f.x, to="series", name=["a"])

    out = df >> pull(f.x, to="series", name=["c", "d"])
    assert len(out) == 2
    assert out["c"].values.tolist() == [1]
    assert out["d"].values.tolist() == [2]


def test_pull_a_flat_dict():
    df = tibble(x=[1, 2], y=[3, 4])
    out = df >> pull(f.y, f.x)
    assert out == {1: 3, 2: 4}

    with pytest.raises(ValueError):
        # length mismatches
        df >> pull(f.y, name=[3, 4, 5], to="dict")


def test_pull_nest_df_col():
    df = tibble(x=1, y=tibble(a=2))
    out = pull(df, 1, to="list")
    assert out == [[2]]

    out = pull(df, 1)
    assert_tibble_equal(out, tibble(a=2))


def test_pull_grouped():
    df = tibble(x=1, y=2).group_by('x')
    out = pull(df, f.y)
    assert isinstance(out, Series)
    assert out.tolist() == [2]
