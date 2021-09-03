import pytest

from datar.base.seq import *
from datar.base import NA
from .conftest import assert_iterable_equal

@pytest.mark.parametrize('from_, to, by, length_out, along_with, expect', [
    (0, 1, None, 11, None, [.0,.1,.2,.3,.4,.5,.6,.7,.8,.9,1.]),
    ([4,2,1,9,3]*4, None, None, None, None, range(1, 21)),
    (1, 9, 2, None, None, [1,3,5,7,9]),
    (1, 9, 2, None, None, [1,3,5,7,9]),
    (1, 6, 3, None, None, [1, 4]),
    (1.575, 5.125, 0.05, None, None, [
        1.575, 1.625, 1.675, 1.725, 1.775, 1.825, 1.875, 1.925, 1.975, 2.025, 2.075, 2.125,
        2.175, 2.225, 2.275, 2.325, 2.375, 2.425, 2.475, 2.525, 2.575, 2.625, 2.675, 2.725,
        2.775, 2.825, 2.875, 2.925, 2.975, 3.025, 3.075, 3.125, 3.175, 3.225, 3.275, 3.325,
        3.375, 3.425, 3.475, 3.525, 3.575, 3.625, 3.675, 3.725, 3.775, 3.825, 3.875, 3.925,
        3.975, 4.025, 4.075, 4.125, 4.175, 4.225, 4.275, 4.325, 4.375, 4.425, 4.475, 4.525,
        4.575, 4.625, 4.675, 4.725, 4.775, 4.825, 4.875, 4.925, 4.975, 5.025, 5.075, 5.125,
    ]),
    (17, None, None, None, None, range(1,18))
])
def test_seq(from_, to, by, length_out, along_with, expect):
    assert_iterable_equal(seq(from_, to, by, length_out, along_with), expect, approx=True)

def test_seq_along():
    assert_iterable_equal(seq_along([]), [])
    assert_iterable_equal(seq_along([1,2]), [1,2])
    assert_iterable_equal(seq_along(['a', 'b']), [1,2])
    assert_iterable_equal(seq_along(['a', 'b'], base0_=True), [0,1])

def test_seq_len(caplog):
    assert_iterable_equal(seq_len([3,4]), [1,2,3])
    assert 'first element' in caplog.text

def test_seq_derives():
    assert_iterable_equal(seq(along_with=['a', 'b']), [1,2])
    assert_iterable_equal(seq(length_out=2), [1,2])
    assert_iterable_equal(seq(to=2), [1,2])

@pytest.mark.parametrize('x, times, length, each, expected', [
    (range(4), 2, None, 1, [0,1,2,3]*2),
    (range(4), 1, None, 2, [0,0,1,1,2,2,3,3]),
    (range(4), [2]*4, None, 1, [0,0,1,1,2,2,3,3]),
    (range(4), [2,1]*2, None, 1, [0,0,1,2,2,3]),
    (range(4), 1, 4, 2, [0,0,1,1]),
    (range(4), 1, 10, 2, [0,0,1,1,2,2,3,3,0,0]),
    (range(4), 3, None, 2, [0,0,1,1,2,2,3,3] * 3),
    (1, 7, None, 1, [1,1,1,1,1,1,1]),
])
def test_rep(x, times, length, each, expected):
    assert_iterable_equal(rep(x, times=times, length=length, each=each), expected)

def test_rep_error():
    with pytest.raises(ValueError):
        rep(c(1,2,3), c(1,2))
    with pytest.raises(ValueError):
        rep(c(1,2,3), c(1,2,3), each=2)

def test_sample():
    x = sample(range(1, 13))
    assert set(x) == set(range(1,13))

    y = sample(x, replace=True)
    assert len(y) <= 12

    z = sample(c(0,1), 100, replace=True)
    assert set(z) == {0, 1}
    assert len(z) == 100

    w = sample('abc', 100, replace=True)
    assert set(w) == {'a', 'b', 'c'}
    assert len(z) == 100

def test_rev():
    assert_iterable_equal(rev([1,2]), [2,1])
    a = numpy.array([1,2], dtype=float)
    out = rev(a)
    assert_iterable_equal(out, [2.0,1.0])
    assert out.dtype == float

def test_unique():
    a = [1,2,2,3]
    assert_iterable_equal(unique(a), [1,2,3])

def test_length():
    assert length(1) == 1
    assert length([1,2]) == 2
    assert_iterable_equal(lengths(1), [1])
    assert_iterable_equal(lengths([[1], [2,3]]), [1,2])

def test_match():
    out = match([1,2,3], [2,3,4])
    assert_iterable_equal(out, [-1,0,1])

def test_sort():
    out = sort([1,2,3])
    assert_iterable_equal(out, [1,2,3])
    out = sort([1,2,3], decreasing=True)
    assert_iterable_equal(out, [3,2,1])
    out = sort([NA, 1,2,3])
    assert_iterable_equal(out, [1,2,3])
    out = sort([NA, 1,2,3], na_last=True)
    assert_iterable_equal(out, [1,2,3, NA])
    out = sort([NA, 1,2,3], na_last=False)
    assert_iterable_equal(out, [NA, 1,2,3])
