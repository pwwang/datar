import numpy, pytest
from datar.core import f
from datar.base import dim
from datar.tibble import tibble
from datar.dplyr import pull

def test_pull_series_with_name():
    df = tibble(x=1)
    out = df >> pull(f.x, to='frame', name='a')
    assert dim(out) == (1,1)
    assert out.columns.tolist() == ['a']

    with pytest.raises(ValueError):
        df >> pull(f.x, to='frame', name=['a', 'b'])

    # pull array
    out = df >> pull(f.x, to='array')
    assert isinstance(out, numpy.ndarray)

def test_pull_series_when_to_equals_series():
    df = tibble(**{'x$a':1})
    out = df >> pull(f.x, to='series')
    assert len(out) == 1
    assert out.values.tolist() == [1]

    # with name
    out = df >> pull(f.x, to='series', name='a')
    assert out.name == 'a'

def test_pull_df():
    df = tibble(**{'x$a':1, 'x$b':2})
    out = df >> pull(f.x, to='series')
    assert len(out) == 2
    assert out['a'].values.tolist() == [1]
    assert out['b'].values.tolist() == [2]

    with pytest.raises(ValueError):
        df >> pull(f.x, to='series', name=['a'])

    out = df >> pull(f.x, to='series', name=['c', 'd'])
    assert len(out) == 2
    assert out['c'].values.tolist() == [1]
    assert out['d'].values.tolist() == [2]
