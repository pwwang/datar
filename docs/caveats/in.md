`%in%` in R is a shortcut for `is.element()` to test if the elements are in a container.

```r
r$> c(1,3,5) %in% 1:4
[1]  TRUE  TRUE FALSE

r$> is.element(c(1,3,5), 1:4)
[1]  TRUE  TRUE FALSE
```

However, `in` in python acts differently:

```python
>>> import numpy as np
>>>
>>> arr = np.array([1,2,3,4])
>>> elts = np.array([1,3,5])
>>>
>>> elts in arr
/.../bin/bpython:1: DeprecationWarning: elementwise comparison failed; this will raise an error in the future.
  #!/.../bin/python
False
>>> [1,2] in [1,2,3]
False
```

It simply tests if the element on the left side of `in` is equal to any of the elements in the right side. Regardless of whether the element on the left side is scalar or not.

Yes, we can redefine the behavior of this by writing your own `__contains__()` methods of the right object. For example:

```python
>>> class MyList(list):
...     def __contains__(self, key):
...         # Just an example to let it return the reversed result
...         return not super().__contains__(key)
...
>>> 1 in MyList([1,2,3])
False
>>> 4 in MyList([1,2,3])
True
```

But the problem is that the result `__contains__()` is forced to be a scalar bool by python. In this sense, we cannot let `x in y` to be evaluated as a bool array or even a pipda `Expression` object.
```python
>>> class MyList(list):
...     def __contains__(self, key):
...         # Just an example
...         return [True, False, True] # logically True in python
...
>>> 1 in MyList([1,2,3])
True
>>> 4 in MyList([1,2,3])
True
```

So instead, we ported `is.element()` from R:

```python
>>> import numpy as np
>>> from datar.base import is_element
>>>
>>> arr = np.array([1,2,3,4])
>>> elts = np.array([1,3,5])
>>>
>>> is_element(elts, arr)
>>> is_element(elts, arr)
array([ True,  True, False])
```

So, as @rleyvasal pointed out in https://github.com/pwwang/datar/issues/31#issuecomment-877499212,

if the left element is a pandas `Series`:
```python
>>> import pandas as pd
>>> pd.Series(elts).isin(arr)
0     True
1     True
2    False
dtype: bool
```
