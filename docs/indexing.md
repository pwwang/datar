## Index base

By default, `datar` follows `R`'s 1-based indexing. You can switch this by:
```python
from datar.base import options

options(index_base_0=False)
```

## Selection

- `c()` vs `[]`

Using `c()` is recommended, since it is able to convert 1-based indexes to 0-based.
`[]` will not do any conversion, and it's not affected by `options('index.base.0')`.

- `DataFrame` indexes

In most cases, indexes/column names are ignored, or reset as `r-dplyr/r-tidyr` does.
When using 1-based indexing selection, `1` will always select the first row, even when the indexes of data frames are ranging from 0.

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
