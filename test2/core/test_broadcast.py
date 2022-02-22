import pytest
from pandas import Index, Series
from pandas.testing import assert_frame_equal
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

    df = tibble(a=[1, 2, 1]).rowwise("a")
    with pytest.raises(ValueError, match=r"must be size 1"):
        _broadcast_base([1, 2], df)
    with pytest.raises(ValueError, match=r"must be size 1"):
        _broadcast_base([1, 2], df.a)


def test_broadcast_base_array_ndframe():
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

    # rowwise
    df = tibble(a=[1, 2, 2]).rowwise()
    base = _broadcast_base(df.a, df.a)
    assert base.obj is df.a.obj

    value = tibble(a=[1, 2, 3]).groupby('a')
    with pytest.raises(ValueError):
        _broadcast_base(value, df)


def test_broadcast_base_groupby_ndframe():
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


def test_broadcast_base_ndframe_groupby():
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


def test_broadcast_base_ndframe_ndframe():
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


# broadcast_to
def test_broadcast_to_scalar():
    value = broadcast_to(1, Index([1, 2]))
    assert value == 1


def test_broadcast_to_arrays_ndframe():
    with pytest.raises(ValueError, match=r"Length of values \(0\)"):
        broadcast_to([], Index([1, 2]))

    with pytest.raises(ValueError, match=r"Length of values \(3\)"):
        broadcast_to([1, 2, 3], Index([1, 2]))

    value = broadcast_to([1, 2], Index([1, 2]))
    assert value.index.tolist() == [1, 2]


def test_broadcast_to_arrays_groupby():
    df = tibble(x=[]).groupby('x')
    value = broadcast_to([], df.obj.index, df.grouper)
    assert value.size == 0

    df = tibble(x=[2, 1, 2, 1])
    df.index = [4, 5, 6, 7]
    df = df.groupby('x', sort=False)
    value = broadcast_to(["a", "b"], df.obj.index, df.grouper)
    assert value.tolist() == ["a", "a", "b", "b"]


def test_broadcast_to_ndframe_ndframe():
    df = tibble(x=[1, 2, 3])
    value = Series([1, 2, 3], index=[1, 2, 3])
    out = broadcast_to(value, df.index)
    assert_iterable_equal(out, [None, 1, 2])

    out = broadcast_to(value.to_frame(name="x"), df.index)
    assert_iterable_equal(out.x, [None, 1, 2])


def test_broadcast_to_ndframe_groupby():
    df = tibble(x=[1, 2, 2, 1, 1, 2]).groupby('x')
    value = Series([8, 10], index=[1, 2])
    out = broadcast_to(value, df.obj.index, df.grouper)
    assert out.tolist() == [8, 10, 10, 8, 8, 10]

    out = broadcast_to(value.to_frame(name="x"), df.obj.index, df.grouper)
    assert out.x.tolist() == [8, 10, 10, 8, 8, 10]


def test_broadcast_to_groupby_ndframe():
    df = tibble(x=[1, 2, 2, 1, 1, 2]).groupby('x')
    with pytest.raises(ValueError, match=r"Can't broadcast grouped"):
        broadcast_to(df, df.obj.index)

    out = broadcast_to(df.x, df.obj.index, df.grouper)
    assert_iterable_equal(out, df.x.obj)

    out = broadcast_to(df, df.obj.index, df.grouper)
    assert_tibble_equal(out, df.obj)

    df = tibble(x=[1, 2, 2, 1, 1, 2]).group_by('x')
    out = broadcast_to(df, df.index, df._datar["grouped"].grouper)
    assert_frame_equal(df, out)


def test_broadcast2():
    # types: scalar/arrays, DattaFrame/Series, GroupBy, TibbleGrouped
    # scalar/arrays <-> other
    left, right, grouper, is_rowwise = broadcast2(
        1,
        Series(1),
    )
    assert left == 1
    assert_iterable_equal(right, [1])
    assert grouper is None
    assert not is_rowwise

    # not happening in practice, since 1 + 1 will be calculated directory
    left, right, grouper, is_rowwise = broadcast2(
        1,
        2,
    )
    assert left == 1
    assert right == 2
    assert grouper is None
    assert not is_rowwise

    left, right, grouper, is_rowwise = broadcast2(
        Series([1, 2, 3, 4]).groupby([1, 2, 1, 2]),
        [7, 8],
    )
    assert_iterable_equal(left, [1, 2, 3, 4])
    assert_iterable_equal(right, [7, 7, 8, 8])
    assert_iterable_equal(grouper.group_info[0], [0, 1, 0, 1])
    assert not is_rowwise

    left, right, grouper, is_rowwise = broadcast2(
        tibble(x=[1, 2, 3, 4]).rowwise(),
        7,
    )

    assert_iterable_equal(left.x, [1, 2, 3, 4])
    assert right == 7
    assert_iterable_equal(grouper.group_info[0], [0, 1, 2, 3])
    assert is_rowwise
