import warnings

import pytest
import pandas as pd


@pytest.fixture(scope="function", autouse=True)
def no_astnode_warn():
    warnings.filterwarnings(
        action='ignore',
        category=UserWarning,
        message=r'Failed to fetch the node.+',
    )


SENTINEL = 85258525.85258525


def assert_iterable_equal(x, y, na=SENTINEL, approx=False):
    x = [na if pd.isnull(elt) else elt for elt in x]
    y = [na if pd.isnull(elt) else elt for elt in y]
    if approx is True:
        x = pytest.approx(x)
    elif approx:
        x = pytest.approx(x, rel=approx)
    assert x == y


def assert_factor_equal(x, y, na=8525.8525, approx=False):
    xlevs = x.categories
    ylevs = y.categories
    assert_iterable_equal(x, y, na=na, approx=approx)
    assert_iterable_equal(xlevs, ylevs, na=na, approx=approx)


def is_installed(pkg):
    try:
        __import__(pkg)
        return True
    except ImportError:
        return False
