import pytest  # noqa

from datar2.base.null import NULL, as_null, is_null


def test_null_is_none():
    assert NULL is None


def test_as_null():
    assert as_null() is NULL
    assert as_null(1) is NULL
    assert as_null(a=1) is NULL
    assert as_null(1, a=1) is NULL


def test_is_null():
    assert is_null(NULL)
    assert not is_null(1)
