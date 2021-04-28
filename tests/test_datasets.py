import pytest

from datar import datasets

def test_nosuch():
    with pytest.raises(ImportError):
        from datar.datasets import nosuch
    with pytest.raises(AttributeError):
        datasets._abc
