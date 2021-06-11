import pytest
from pandas import DataFrame
from datar import *
from datar.base import options_context

def test_dtypes_displayed():
    df = DataFrame({'a': [1,2,3], 'b': [1.0,2.0,3.0], 'c': [(1,), (2,), (3,)]})
    out = df.to_string().splitlines()[1]
    assert '<int' in out
    assert '<float' in out
    assert '<object' in out

    with options_context(frame_format_patch=False):
        out = df.to_string().splitlines()[1]
        assert '<int' not in out
        assert '<float' not in out
        assert '<object' not in out

def test_df_cells_collapsed():
    df1 = DataFrame({'a': [1,2,3], 'b': [4,5,6]})
    df2 = DataFrame({'c': [df1, df1], 'd': [7,8]})
    out = df2.to_string().splitlines()
    assert '<DF 3x2>' in out[2]
    assert '<DF 3x2>' in out[3]

    with options_context(frame_format_patch=False):
        out = df2.to_string().splitlines()
        assert '<DF 3x2>' not in out[2]
        assert '<DF 3x2>' not in out[3]
