import pytest
from datar.all import pipe


def test_pipe_with_list():
    """Test pipe with a list"""
    data = [1, 2, 3, 4, 5]
    result = data >> pipe(lambda x: [i * 2 for i in x])
    expected = [2, 4, 6, 8, 10]
    assert result == expected


def test_pipe_with_dict():
    """Test pipe with a dictionary"""
    data = {'a': 1, 'b': 2, 'c': 3}
    result = data >> pipe(lambda x: {k: v * 2 for k, v in x.items()})
    expected = {'a': 2, 'b': 4, 'c': 6}
    assert result == expected


def test_pipe_with_args():
    """Test pipe with additional positional arguments"""
    data = [1, 2, 3]
    
    def add_value(data, value):
        return [x + value for x in data]
    
    result = data >> pipe(add_value, 10)
    expected = [11, 12, 13]
    assert result == expected


def test_pipe_with_kwargs():
    """Test pipe with keyword arguments"""
    data = [1, 2, 3]
    
    def multiply_data(data, factor=1):
        return [x * factor for x in data]
    
    result = data >> pipe(multiply_data, factor=10)
    expected = [10, 20, 30]
    assert result == expected


def test_pipe_with_string():
    """Test pipe with string operations"""
    data = "hello"
    result = data >> pipe(lambda x: x.upper())
    assert result == "HELLO"


def test_pipe_with_tuple():
    """Test pipe with tuple"""
    data = (1, 2, 3)
    result = data >> pipe(lambda x: tuple(i * 2 for i in x))
    expected = (2, 4, 6)
    assert result == expected


def test_pipe_returns_different_type():
    """Test that pipe can return different types"""
    data = [1, 2, 3, 4, 5]
    result = data >> pipe(sum)
    assert result == 15


def test_pipe_chain_multiple():
    """Test chaining multiple pipe operations"""
    data = [1, 2, 3]
    
    result = (
        data 
        >> pipe(lambda x: [i * 2 for i in x])
        >> pipe(lambda x: [i + 1 for i in x])
    )
    expected = [3, 5, 7]
    assert result == expected


def test_pipe_with_custom_class():
    """Test pipe with a custom class"""
    class Counter:
        def __init__(self, value):
            self.value = value
        
        def increment(self, amount):
            return Counter(self.value + amount)
        
        def __eq__(self, other):
            return self.value == other.value
    
    counter = Counter(5)
    result = counter >> pipe(lambda x: x.increment(10))
    expected = Counter(15)
    assert result == expected


def test_pipe_with_multiple_args_and_kwargs():
    """Test pipe with both args and kwargs"""
    data = [1, 2, 3]
    
    def transform(data, multiplier, offset=0):
        return [x * multiplier + offset for x in data]
    
    result = data >> pipe(transform, 2, offset=5)
    expected = [7, 9, 11]
    assert result == expected

