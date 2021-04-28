from pandas import Series
from datar.all import *

def test_order_by():
    x = [1,2,3,4,5]
    assert order_by(range(5), x).tolist() == x
    assert order_by(range(5, 0, -1), x).tolist() == list(reversed(x))

def test_with_order():
    def cums(it):
        out = []
        for i in it:
            if not out:
                out = [i]
            else:
                out.append(out[-1] + i)
        return out
    def cums_series(it):
        out = []
        for i in it:
            if not out:
                out = [i]
            else:
                out.append(out[-1] + i)
        return Series(out)

    x = [1,2,3,4,5]
    assert with_order(range(5), cums, x).tolist() == [1,3,6,10,15]
    assert with_order(range(5,0,-1), cums_series, x).tolist() == [15, 14, 12, 9, 5]
