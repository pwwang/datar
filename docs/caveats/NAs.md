
- dtype

    `NA` in datar sets to `numpy.nan`, which is a float. So that it causes problems for other dtypes of data, because setting a value to NA (float) in an array with other dtype is not compatible. Unlike R, python does not have missing value type for other dtypes.

    Pandas is developing its own nullabled typing system. See:
    https://pandas.pydata.org/pandas-docs/stable/user_guide/integer_na.html
    https://pandas.pydata.org/pandas-docs/stable/user_guide/boolean.html

    However, `numpy` is still not aware of it, which causes problems for internal computations.

- string

    When initialize a string array intentionally: `numpy.array(['a', NA])`, the `NA` will be converted to a string `'nan'`. That may not be what we want sometimes. To avoid that, use `None` or `NULL` instead:

    ```python
    >>> numpy.array(['a', None])
    array(['a', None], dtype=object)
    ```

    Just pay attention that the dtype falls back to object.


- `NaN`

    Since `NA` is already a float, `NaN` here is equivalent to `NA`.
