## Index base

By default, `datar` follows `R`'s 1-based indexing. You can switch this by:
```python
from datar.base import options

options(index_base_0=False)
```

## Selection

- `DataFrame` indexes

In most cases, indexes/column names are ignored, or reset as `r-dplyr/r-tidyr` does.
When using 1-based indexing selection, `1` will always select the first row, even when the indexes of data frames are ranging from 0.

## Negative indexes

In `R`, negative indexes mean removal. However, here negative indexes are still
selection, as `-1` for the last column, `-2` for the second last, etc. It is
the same for both 0-based and 1-based indexing.

If you want to do negative selection, use tilde `~` instead of `-`.

## Temporary index base change

For example:

```python
from datar.tibble import tibble
from datar.dplyr import slice
from datar.base import c

df = tibble(a=range(10))

# we can also use a context manager
from datar.base import options_context

with options_context(index_base_0=True):
    df >> slice(c(1,2,3))
    # rows #2,3,4: [1,2,3]
```

APIs with arguments related to indexing selection usually have a `base0_` argument, which also switch the index base temporarily. For example:

```python
from datar.datasets iris
from datar.all import mutate, across, c

iris >> mutate(across(c(0,1), round, base0_=True))
# 	Sepal_Length Sepal_Width Petal_Length Petal_Width	Species
# 0	5.0	         4.0         1.4          0.2           setosa
# 1	5.0	         3.0         1.4          0.2           setosa
# 2	5.0	         3.0         1.3          0.2           setosa
# 3	5.0	         3.0         1.5          0.2           setosa
# ...
```
