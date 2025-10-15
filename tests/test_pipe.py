import pytest
import pandas as pd
from datar.all import pipe


def test_pipe_basic_lambda():
    """Test pipe with a basic lambda function"""
    df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
    result = df >> pipe(lambda x: x * 2)
    expected = pd.DataFrame({'a': [2, 4, 6], 'b': [8, 10, 12]})
    pd.testing.assert_frame_equal(result, expected)


def test_pipe_with_args():
    """Test pipe with additional positional arguments"""
    df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
    
    def add_value(df, value):
        return df + value
    
    result = df >> pipe(add_value, 10)
    expected = pd.DataFrame({'a': [11, 12, 13], 'b': [14, 15, 16]})
    pd.testing.assert_frame_equal(result, expected)


def test_pipe_with_kwargs():
    """Test pipe with keyword arguments"""
    df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
    
    def multiply_col(df, col, factor=1):
        df = df.copy()
        df[col] = df[col] * factor
        return df
    
    result = df >> pipe(multiply_col, 'a', factor=10)
    expected = pd.DataFrame({'a': [10, 20, 30], 'b': [4, 5, 6]})
    pd.testing.assert_frame_equal(result, expected)


def test_pipe_with_column_selection():
    """Test pipe with column operations"""
    df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
    
    # Select a column and then multiply it
    result = df >> pipe(lambda df: df[['a']]) >> pipe(lambda x: x * 2)
    expected = pd.DataFrame({'a': [2, 4, 6]})
    pd.testing.assert_frame_equal(result, expected)


def test_pipe_with_column_rename():
    """Test pipe with column renaming (similar to issue example)"""
    df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
    
    # Select a column and rename it
    result = df >> pipe(lambda df: df[['a']]) >> pipe(lambda df: df.rename(columns=str.upper))
    expected = pd.DataFrame({'A': [1, 2, 3]})
    pd.testing.assert_frame_equal(result, expected)


def test_pipe_with_custom_function():
    """Test pipe with a custom function that modifies the dataframe"""
    df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
    
    def custom_transform(df, new_col_name, value):
        df = df.copy()
        df[new_col_name] = df['a'] + value
        return df
    
    result = df >> pipe(custom_transform, 'c', 100)
    expected = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6], 'c': [101, 102, 103]})
    pd.testing.assert_frame_equal(result, expected)


def test_pipe_returns_non_dataframe():
    """Test that pipe can return non-DataFrame objects"""
    df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
    
    result = df >> pipe(lambda x: x['a'].sum())
    assert result == 6


def test_pipe_chain_multiple():
    """Test chaining multiple pipe operations"""
    df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
    
    result = (
        df 
        >> pipe(lambda x: x * 2)
        >> pipe(lambda x: x + 1)
    )
    expected = pd.DataFrame({'a': [3, 5, 7], 'b': [9, 11, 13]})
    pd.testing.assert_frame_equal(result, expected)


def test_pipe_with_set_axis_like_issue():
    """Test pipe similar to the issue example with set_axis"""
    df = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
    
    # Simulate the issue example: convert column names to lowercase
    result = df >> pipe(lambda df: df.set_axis(df.columns.str.lower(), axis=1))
    expected = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
    pd.testing.assert_frame_equal(result, expected)

