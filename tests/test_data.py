import pytest
from datar.data import descr_datasets, add_dataset
from datar.core.utils import NotImplementedByCurrentBackendError


def test_descr_datasets():
    x = descr_datasets()
    assert "iris" in x

    x = descr_datasets("iris")
    assert "iris" in x and len(x) == 1


def test_add_dataset():

    add_dataset("test", {"url": ""})
    assert "test" in descr_datasets()


def test_load_dataset():

    with pytest.raises(NotImplementedByCurrentBackendError):
        from datar.data import iris  # noqa: F401


def test_no_such():
    with pytest.raises(NotImplementedByCurrentBackendError):
        from datar.data import nosuch  # noqa: F401
