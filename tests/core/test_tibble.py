import numpy as np
import pytest
from pandas import DataFrame
from pandas.api.types import is_float_dtype, is_integer_dtype
from pandas.core.groupby import SeriesGroupBy
from pandas.testing import assert_frame_equal
from datar import f
from datar.core.exceptions import NameNonUniqueError
from datar.core.tibble import Tibble, TibbleRowwise, TibbleGrouped
from datar.testing import assert_tibble_equal

from ..conftest import assert_iterable_equal, is_installed


def test_tibble():
    df = Tibble({"a": [1]}, meta={"x": 2})
    assert df._datar["x"] == 2
    assert_frame_equal(df, DataFrame({"a": [1]}))


def test_tibble_from_pairs():
    with pytest.raises(NameNonUniqueError):
        df = Tibble.from_pairs(
            ["a", "a"],
            [1, 1],
        )
    with pytest.raises(ValueError):
        df = Tibble.from_pairs(
            ["a", "a"],
            [1, ],
        )

    df = Tibble.from_pairs(
        ["a", "a"],
        [1, f[:3]],
        _name_repair="universal",
    )
    assert_frame_equal(df, DataFrame({"a__0": [1, 1, 1], "a__1": [0, 1, 2]}))

    df = Tibble.from_pairs(["a"], [1.1], _dtypes=True)
    assert is_float_dtype(df.a)

    df = Tibble.from_pairs(["a"], [1.1], _dtypes={"a": int})
    assert is_integer_dtype(df.a)

    df = Tibble.from_pairs(["a"], [1.1], _dtypes=int)
    assert is_integer_dtype(df.a)

    df = Tibble.from_pairs(["a"], [df], _dtypes={"a": int})
    assert is_integer_dtype(df["a$a"])

    # __getitem__
    assert_frame_equal(df["a"], DataFrame({"a": [1]}))

    with pytest.raises(KeyError):
        df["b"]


def test_tibble_from_args():
    df = Tibble.from_args(_rows=3)
    assert list(df.index) == [0, 1, 2]

    df = Tibble.from_args(a=1)
    assert df.shape == (1, 1)
    assert df.a.tolist() == [1]
    assert df.group_vars == []


def test_tibble_setitem():
    df = Tibble.from_args(a=[1, 2, 3])
    # gf = Tibble.from_args(b=[4, 5, 6]).group_by(["b"])
    # df["b"] = gf._datar["grouped"]
    df2 = df.copy()
    df["b"] = df2
    assert_frame_equal(df["b"], DataFrame(df2))

    # 0-col df with NA
    df["c"] = Tibble.from_args()
    assert_iterable_equal(df.c, [np.nan] * 3)


def test_tibble_group_by():
    df = Tibble.from_args(b=[4, 5, 6]).group_by(["b"])
    assert df.group_vars == ["b"]

    df2 = TibbleGrouped.from_groupby(df._datar["grouped"])
    assert df2.group_vars == ["b"]

    assert isinstance(df2.b, SeriesGroupBy)

    df3 = TibbleGrouped.from_groupby(df2.b)
    assert_tibble_equal(df2, df3)

    # __setitem__
    df3["c"] = 1
    assert df3._datar["grouped"].c.obj.tolist() == [1, 1, 1]

    df3["d"] = Tibble.from_args(d=df2.b)
    assert df3._datar["grouped"].obj["d$d"].tolist() == [4, 5, 6]

    # copy
    df4 = df3.copy()
    assert "c" in df4._datar["grouped"].obj

    df = Tibble.from_args(a=[1, 2, 3], b=[4, 5, 6])
    df.index = ["a", "b", "c"]
    df = df.group_by("a")
    # reindex
    df5 = df.reindex(["a", "a", "b"])
    assert df5.b.obj.tolist() == [4, 4, 5]

    # # take
    # df6 = df.take([0, 0, 1])
    # assert df6.b.obj.tolist() == [4, 4, 5]

    # group_by
    df7 = df.group_by("b", add=True)
    assert df7.group_vars == ["a", "b"]

    df8 = df7.convert_dtypes()
    assert_iterable_equal(df7.values.flatten(), df8.values.flatten())


def test_tibble_rowwise():
    df = Tibble.from_args(a=[1, 2, 3]).rowwise([])
    assert isinstance(df, TibbleRowwise)
    assert len(df.group_vars) == 0

    assert isinstance(df.a, SeriesGroupBy)
    assert df.a.is_rowwise

    # reindex
    df2 = df.reindex([0, 0, 1, 1, 2, 2])
    assert isinstance(df2, TibbleRowwise)
    assert df2._datar["grouped"].grouper.size().tolist() == [1] * 6

    # # take
    # df3 = df2.take([0, 2, 4])
    # assert_tibble_equal(df3, df)

    df3 = df2.copy()
    assert_tibble_equal(df2, df3)


@pytest.mark.skipif(
    not is_installed("pdtypes"), reason="'pdtypes' is not installed"
)
def test_footer():
    df = Tibble.from_args(x=[1, 2, 3]).group_by('x')
    assert "n=3" in str(df)
    assert "n=3" in df._repr_html_()


def test_regroup_nohard():
    df = Tibble.from_args(x=[1, 2, 3]).group_by('x')
    out = df.regroup(hard=False, inplace=False)
    assert out is not df
    assert out._datar["grouped"] is not df._datar["grouped"]

    out = df.regroup(hard=True, inplace=True)
    assert out is df
    assert out._datar["grouped"] is df._datar["grouped"]
