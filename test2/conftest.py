import pytest
import pandas as pd


SENTINEL = 85258525.85258525


def assert_iterable_equal(x, y, na=SENTINEL, approx=False):
    x = [na if pd.isnull(elt) else elt for elt in x]
    y = [na if pd.isnull(elt) else elt for elt in y]
    if approx is True:
        x = pytest.approx(x)
    elif approx:
        x = pytest.approx(x, rel=approx)
    assert x == y
