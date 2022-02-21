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


def test_broadcast_base_scalar():
    df = tibble(a=1)
    out = _broadcast_base(1, df)[1]
    assert out is df


def test_broadcast_base_array():
    df = tibble(a=[1, 2, 3, 4])
    df.index = [0, 1, 1, 1]

    with pytest.raises(ValueError, match=r"Value must be size \[1 3\], not 2"):
        _broadcast_base([1, 2], df)

    value, base = _broadcast_base([1, 2, 3], df)
    assert base.index.tolist() == [0, 0, 0, 1, 1, 1]
    assert value == [1, 2, 3]

    value, base = _broadcast_base([1], df)
    assert base.index.tolist() == [0, 1, 1, 1]
    assert value == [1]

    # TibbleGrouped
    gf = tibble(a=[1, 2]).group_by("a")
    value, base = _broadcast_base([1, 2], gf)
    assert base.index.tolist() == [0, 0, 1, 1]
    assert value == [1, 2]

    # No index tibble
    df = tibble(a=[1, 2])
    value, base = _broadcast_base([1, 2, 3], df)
    assert base is df
    assert value == [1, 2, 3]

    df.index = [1, 2]
    value, base = _broadcast_base([1, 2, 3, 4], df)
    assert list(value) == [1, 2, 3, 4] * 2
    assert_tibble_equal(
        base.reset_index(drop=True), tibble(a=[1, 1, 1, 1, 2, 2, 2, 2])
    )


def test_broadcast_base_groupby():
    df = tibble(a=[1, 2, 3, 4])
    df.index = [0, 1, 1, 1]

    val = tibble(a=[1, 2, 3]).group_by("a")
    grouped = val._datar["grouped"]
    value, base = _broadcast_base(grouped, df)
    assert value is grouped
    assert base is df

    df.index = [0, 1, 2, 3]
    value, base = _broadcast_base(grouped, df)
    assert value is grouped
    assert base is df

    val2 = tibble(a=[1, 2]).group_by("a")
    with pytest.raises(ValueError):
        _broadcast_base(grouped, val2)

    base3 = tibble(x=["a", "b"], y=[1, 2]).group_by("x")
    val3 = base3.y.sum().take([0, 0, 1, 1]).to_frame().reset_index()
    val3.index = [0, 0, 1, 1]
    val3 = val3.groupby("x")
    value, base = _broadcast_base(val3, base3)
    assert value is val3
    assert base.index.tolist() == [0, 0, 1, 1]

    val4 = base3.y.sum().to_frame().reset_index().groupby("x")
    value, base = _broadcast_base(val4, base3)
    assert value is val4
    assert base is base3


def test_broadcast_base_series():
    df = tibble(a=[1, 2, 3, 4])
    df.index = [0, 1, 1, 1]

    val = Series(1)
    value, base = _broadcast_base(val, df)
    assert value is val
    assert base is df

    df = tibble(a=f[:4])
    val = Series(range(8), index=[0, 0, 1, 1, 2, 2, 3, 3])
    value, base = _broadcast_base(val, df)
    assert value is val
    assert base.index.tolist() == [0, 0, 1, 1, 2, 2, 3, 3]

    df = df.group_by('a')
    val = df.a.sum()
    val.index = [4, 5, 6, 7]
    with pytest.raises(ValueError, match="Incompatible"):
        _broadcast_base(val, df)

    val = df.a.sum()
    value, base = _broadcast_base(val, df)
    assert value is val
    assert base is df

    val = df.a.sum().take([0, 0, 1, 2, 3])
    value, base = _broadcast_base(val, df)
    assert value is val
    assert base.index.tolist() == [0, 0, 1, 2, 3]


def test_broadcast_to_arrays():
    out = broadcast_to(1, None)
    assert out == 1

    out = broadcast_to([], [])
    assert isinstance(out, Series)
    assert len(out) == 0

    base = Series([1, 2, 3, 4]).groupby([1, 1, 2, 2])
    value = broadcast_to([1, 2], base.obj.index, base.grouper)
    assert value.tolist() == [1, 2, 1, 2]


def test_broadcast_to_series():
    value = Series(1)
    out = broadcast_to(value, Index(range(4)))
    assert out.tolist() == [1, 1, 1, 1]

    value = tibble(a=1)
    out = broadcast_to(value, Index(range(4)))
    assert_tibble_equal(out, tibble(a=[1, 1, 1, 1]))

    df = tibble(x=['a', 'b'] * 2, y=[1, 3, 2, 4]).group_by('x')
    value = df.y.sum()
    out = broadcast_to(value, df.index, df._datar["grouped"].grouper)
    assert out.tolist() == [3, 7, 3, 7]

    value = value.to_frame()

    out = broadcast_to(value, df.index, df._datar["grouped"].grouper)
    assert out.y.tolist() == [3, 7, 3, 7]


def test_broadcast_to_sgb():
    df = tibble(x=['a', 'b'] * 2, y=[1, 3, 2, 4]).group_by('x')
    with pytest.raises(ValueError):
        broadcast_to(df.y, df.y.obj.index)

    out = broadcast_to(df.y, df.y.obj.index, df.y.grouper)
    assert out.tolist() == [1, 3, 2, 4]
    assert out.index.tolist() == [0, 1, 2, 3]

    out = broadcast_to(df._datar["grouped"], df.y.obj.index, df.y.grouper)
    assert out.y.tolist() == [1, 3, 2, 4]
    assert out.index.tolist() == [0, 1, 2, 3]

    out = broadcast_to(df, df.y.obj.index, df.y.grouper)
    assert out.y.tolist() == [1, 3, 2, 4]
    assert out.index.tolist() == [0, 1, 2, 3]


def test_broadcast2_left_arrays():
    left, right, grouper, is_rw = broadcast2(1, Series([1, 2]))
    assert left == 1
    assert right.tolist() == [1, 2]
    assert grouper is None
    assert not is_rw

    df = tibble(x=['a', 'b'] * 2, y=[1, 3, 2, 4]).group_by('x')
    left, right, grouper, is_rw = broadcast2([1], df.y)
    assert left == [1]
    assert right.obj.tolist() == [1, 2]
    assert grouper is df.y.grouper
    assert not is_rw
