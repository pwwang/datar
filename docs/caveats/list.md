
R's list is actually a name-value pair container. When there is a need for it, we use python's dict instead, since python's list doesn't support names.

For example:
```python
>>> names({'a':1}, 'x')
{'x': 1}
```

We have `base.c()` to mimic `c()` in R, which will concatenate and flatten anything passed into it. Unlike `list()` in python, it accepts multiple arguments. So that you can do `c(1,2,3)`, but you cannot do `list(1,2,3)` in python.
