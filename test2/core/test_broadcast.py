import pytest
from pandas import Index, Series
from datar import f
from datar2.testing import assert_tibble_equal
from datar2.tibble import tibble
from datar2.core.broadcast import (
    _broadcast_base,
    broadcast2,
    broadcast_to,
    add_to_tibble,
    init_tibble_from,
)

from ..conftest import assert_iterable_equal


def test_broadcast_base_scalar():
    df = tibble(a=1)
    out = _broadcast_base(1, df)
    assert out is df


def test_broadcast_base_array_groupby():
    df = tibble(a=[]).groupby("a")
    with pytest.raises(ValueError, match=r"`\[1, 2\]` must be size"):
        _broadcast_base([1, 2], df)

    # all size-1 groups
    df = tibble(a=[1, 2]).groupby("a")
    out = _broadcast_base([1, 2], df)
    assert out.a.obj.values.tolist() == [1, 1, 2, 2]

    df = tibble(a=[1, 2, 1, 2]).groupby("a")
    with pytest.raises(ValueError, match=r"Cannot recycle `x` with size"):
        _broadcast_base([1, 2, 3], df, "x")

    df = tibble(a=[2, 2, 1, 2]).groupby("a")
    with pytest.raises(ValueError, match=r"Cannot recycle `\[1, 2\]`"):
        _broadcast_base([1, 2], df)

    df.obj.index = [0, 0, 1, 0]
    out = _broadcast_base([1, 2, 3], df)
    assert out is df

    df = tibble(a=[2, 2, 1, 2]).groupby("a")
    out = _broadcast_base([1, 2, 3], df)
    assert out.a.obj.values.tolist() == [2, 2, 1, 1, 1, 2]

    df = tibble(a=[1, 2, 2, 3, 3, 3]).groupby("a")
    with pytest.raises(
        ValueError, match=r"Cannot recycle `x` with size 2 to 1"
    ):
        _broadcast_base([1, 2], df, "x")

    df = tibble(a=[1, 2, 1, 2]).group_by("a")
    out = _broadcast_base([1, 2], df)
    assert out is df

    # TibbleGrouped
    df = tibble(a=[1, 2, 1]).group_by("a")
    out = _broadcast_base([1, 2], df)
    assert out.a.obj.values.tolist() == [1, 2, 2, 1]


def test_broadcast_base_array_seriesordf():
    df = tibble(a=[1, 2, 3, 4])
    df.index = [0, 1, 1, 1]

    out = _broadcast_base([1, 2], df)
    assert out is df

    df = tibble(a=[1, 2, 3])
    base = _broadcast_base([1, 2, 3], df)
    assert base is df

    df = tibble(a=1)
    base = _broadcast_base([1, 2, 3], df)
    assert base.a.tolist() == [1] * 3

    df = tibble(a=f[:3])
    with pytest.raises(ValueError, match=r"`x` must be size \[1 3\], not 2\."):
        _broadcast_base([1, 2], df, "x")


def test_broadcast_base_groupby_groupby():
    # incompatible grouper
    df = tibble(a=[1, 2, 3]).groupby("a")
    value = tibble(a=[1, 2, 2]).groupby("a")
    with pytest.raises(ValueError, match=r"`x` has an incompatible grouper"):
        _broadcast_base(value, df, "x")

    # base doesn't broadcast when all size-1 groups
    df = tibble(a=[1, 2, 3]).groupby("a")
    base = _broadcast_base(df, df)
    assert base is df

    # group names differ
    value1 = tibble(b=[1, 2, 3]).groupby("b")
    with pytest.raises(ValueError, match=r"`x` has an incompatible grouper"):
        _broadcast_base(value1, df, "x")

    # index not unique, already broadcasted
    df = tibble(a=[2, 1, 2])
    df.index = [1, 0, 1]
    df = df.groupby("a")
    base = _broadcast_base(value, df)
    assert base is df

    # size-1 group gets broadcasted
    df = tibble(a=[2, 1, 2]).groupby("a")
    value = tibble(a=[1, 2, 1]).groupby("a")
    base = _broadcast_base(value, df)
    assert base.a.obj.values.tolist() == [2, 1, 1, 2]

    # TibbleGrouped
    df = tibble(a=[1, 2, 2]).group_by("a")
    base = _broadcast_base(value, df)
    assert base.a.obj.values.tolist() == [1, 1, 2, 2]


def test_broadcast_base_groupby_seriesordf():
    df = tibble(a=[1, 2, 2, 3, 3, 3])
    with pytest.raises(ValueError, match="Can't recycle"):
        _broadcast_base(df.groupby("a"), df)

    # only when group sizes are len(value) or [1, len(value)]
    value = tibble(a=[2, 1, 2, 1]).groupby("a")
    df = tibble(a=3)
    base = _broadcast_base(value, df)
    assert base.a.obj.tolist() == [3, 3, 3, 3]

    df = tibble(a=[3, 4])
    base = _broadcast_base(value, df)
    assert base.a.obj.tolist() == [3, 3, 4, 4]

    # TibbleGrouped
    value = tibble(a=[2, 1, 2, 1]).group_by("a")
    base = _broadcast_base(value, df)
    assert base.a.obj.tolist() == [3, 3, 4, 4]


def test_broadcast_base_seriesordf_groupby():
    df = tibble(a=1).groupby("a")
    value = Series(1, name="b")
    with pytest.raises(
        ValueError, match="`b` is an incompatible aggregated result"
    ):
        _broadcast_base(value, df)

    value = Series(1, index=[2])
    value.index.name = "a"
    with pytest.raises(
        ValueError, match="`x` is an incompatible aggregated result"
    ):
        _broadcast_base(value, df, "x")

    df = tibble(a=[2, 1, 2, 1]).groupby("a")
    value = Series([1, 3, 3, 3], index=[1, 2, 2, 2])
    value.index.name = "a"
    with pytest.raises(
        ValueError, match="`x` is an incompatible aggregated result"
    ):
        _broadcast_base(value, df, "x")

    df = tibble(a=[2, 1, 2, 1]).groupby("a")
    value = df.a.sum()
    base = _broadcast_base(value, df)
    assert base is df

    df = tibble(a=[1, 2]).groupby("a")
    df.obj.index = [1, 1]
    value = df.a.apply(
        lambda x: range(x.values[0])
    ).explode().astype(int)
    base = _broadcast_base(value, df)
    assert base is df

    # Broadcast size-1 groups in base
    df = tibble(a=[1, 2]).groupby("a")
    base = _broadcast_base(value, df)
    assert base.a.obj.tolist() == [1, 2, 2]

    # TibbleGrouped
    df = tibble(a=[1, 2]).group_by("a")
    base = _broadcast_base(value, df)
    assert base.a.obj.tolist() == [1, 2, 2]


def test_broadcast_base_seriesordf_seriesordf():
    df = tibble(a=[1, 2, 3])
    df.index = [0, 1, 1]
    value = tibble(a=[1, 2, 3])
    base = _broadcast_base(value, df)
    assert base is df

    df = tibble(a=[1, 2, 3])
    value = tibble(a=[1, 2, 3])
    value.index = [1, 2, 3]
    base = _broadcast_base(value, df)
    assert_iterable_equal(base.a, [2, 3, None])
