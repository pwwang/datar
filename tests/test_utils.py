import pytest

from pandas import DataFrame
from datar.utils import *

def test_head_tail():
    df = DataFrame(range(20), columns=['x'])
    z = df >> head()
    assert z.shape[0] == 6
    z = df >> head(3)
    assert z.shape[0] == 3
    z = list(range(10)) >> head()
    assert len(z) == 6
    with pytest.raises(TypeError):
        head(3)

    z = df >> tail()
    assert z.shape[0] == 6
    z = df >> tail(3)
    assert z.shape[0] == 3
    z = list(range(10)) >> tail()
    assert len(z) == 6
    with pytest.raises(TypeError):
        tail(3)
