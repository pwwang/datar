from datar.datar.funcs import itemgetter
from datar.all import *
from pandas.testing import assert_frame_equal

def test_itemgetter():
    arr = [1,2,3]
    df = tibble(x=2)
    out = df >> mutate(y=itemgetter(arr, f.x))
    assert_frame_equal(out, tibble(x=2, y=3))

def test_get():
    df = tibble(x=2)
    df.index=['a']

    out = df >> get(1, 1)
    assert out == 2

    out = df >> get('a', 'x')
    assert out == 2

    out = df >> get(['a'], ['x'])
    assert out.equals(df)

    out = df >> get('a')
    assert out.equals(df)

    out = df >> get(cols='x')
    assert out.equals(df)

def test_flatten():
    df = tibble(x=[1,2], y=[3,4])
    out = df >> flatten(True)
    assert out == [1,2,3,4]
    out = df >> flatten()
    assert out == [1,3,2,4]
