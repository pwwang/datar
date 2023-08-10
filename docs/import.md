## Import submodule, verbs and functions from datar

You can import everything (all verbs and functions) from datar by:
```python
from datar.all import *
```

which is not recommended. Instead, you can import individual verbs or functions by:
```python
from datar.all import mutate
```

!!! Attention

    When you use `from datar.all import *`, you need to pay attention to the python builtin names that are covered by `datar` (will warn by default). For example, `slice` will be `datar.dplyr.slice` instead of `builtins.slice`. To refer to the builtin one, you need to:
    ```python
    import builtins

    s = builtins.slice(None, 3, None) # [:3]
    ```

Or if you know the origin of the verb, you can also do:
```python
from datar.dplyr import mutate
```

You can also keep the namespace:
```python
from datar import dplyr

# df = tibble(x=1)
# then use it with the dplyr namespace:
df >> dplyr.mutate(y=2)
```

If you feel those namespaces are annoying, you can always use `datar.all`:
```python
from datar.all import mutate
```

## Import datasets from datar

!!! note

    Dataset has to be imported individually. This means  `from datar.datasets import *` won't work (you don't want all datasets to exhaust your memory).

You don't have to worry about other datasets to be imported and take up the memory when you import one. The dataset is only loaded into memory when you explictly import it individually.

See also [datasets](../datasets) for details about available datasets.

## About python reserved names to be masked by `datar`

Sometimes it will be confusing especially when python builtin functions are overriden by `datar`. There are a couple of datar (`r-base`, `dplyr`) functions with the same name as python builtin functions. For example: `filter`, which is a python builtin function, but also a `dplyr` function. You should use `filter_` instead. By default, `datar` will raise an error when you try to import `filter`. You can set this option to `True` to allow this behavior.
