## Why `f`?

It is just fast for you to type, since usually, it is `.` right after `f`. Then you have your left hand and right hand working together sequentially.

## The `Symbolic` object `f`

You can import it by `from datar import f`, or `from datar.all import *`

`f` is a universal `Symbolic` object, which does the magic to connect the expressions in verb arguments so that they can be delayed to execute.

There are different uses for the `f`.

- Use as a proxy to refer to dataframe columns (i.e. `f.x`, `f['x']`)
- Use as a slice container. For example:
    - `f[:3]` for `range(0,3)`
    - `f[f.x:f.z]` for columns from `x` to `z` (not indcluded). If you want to include the `stop` column: `f[f.x:f.z:1]`
- Use as the column name marker for `tribble`:
    ```python
    tribble(
        f.x, f.y
        1,   2
        3,   4
    )
    ```

## If you don't like `f` ...

Sometimes if you have mixed verbs with piping and you want to distinguish to proxies for different verbs:

```python
# you can just replicate f with a different name
g = f

df = tibble(x=1, y=2)
df >> left_join(df >> group_by(f.x), by=g.y)
```

Or you can instantiate a new `Symbolic` object:
```python
from pipda.symbolic import Symbolic

g = Symbolic()

# f and g make no difference in execution technically
```

You can also alias `f` by:
```python
from datar import f as g
```
