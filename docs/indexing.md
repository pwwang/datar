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
