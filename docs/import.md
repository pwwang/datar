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

## Warn about python reserved names to be overriden by `datar`

Sometimes it will be confusing especially when python builtin functions are overriden by `datar`. There are a couple of datar (`r-base`, `dplyr`) functions with the same name as python builtin functions do. For example: `filter`.

To make you aware of the loss of access to builtin functions or other python preserved names, warnings will be reported when those names are imported directly:

```python
>>> from datar.dplyr import filter
[2021-06-18 17:58:23][datar][WARNING] Builtin name "filter" has been overriden by datar.
>>> from datar.all import *
2021-06-18 17:58:48][datar][WARNING] Builtin name "min" has been overriden by datar.
[2021-06-18 17:58:48][datar][WARNING] Builtin name "max" has been overriden by datar.
[2021-06-18 17:58:48][datar][WARNING] Builtin name "sum" has been overriden by datar.
[2021-06-18 17:58:48][datar][WARNING] Builtin name "abs" has been overriden by datar.
[2021-06-18 17:58:48][datar][WARNING] Builtin name "round" has been overriden by datar.
[2021-06-18 17:58:48][datar][WARNING] Builtin name "all" has been overriden by datar.
[2021-06-18 17:58:48][datar][WARNING] Builtin name "any" has been overriden by datar.
[2021-06-18 17:58:48][datar][WARNING] Builtin name "re" has been overriden by datar.
[2021-06-18 17:58:48][datar][WARNING] Builtin name "slice" has been overriden by datar.
```

However, when they are not imported directly, no warnings will show:

```python
from datar import dplyr
dplyr.filter # ok
```

There are a couple of ways to disable:

1. Use option: `warn.builtin.names`

```python
>>> from datar import options
>>> options(warn_builtin_names=False)
>>> from datar.all import * # ok
>>> options(warn_builtin_names=True)
>>> from datar.all import *
[2021-06-18 18:02:35][datar][WARNING] Builtin name "min" has been overriden by datar.
[2021-06-18 18:02:35][datar][WARNING] Builtin name "max" has been overriden by datar.
[2021-06-18 18:02:35][datar][WARNING] Builtin name "sum" has been overriden by datar.
[2021-06-18 18:02:35][datar][WARNING] Builtin name "abs" has been overriden by datar.
[2021-06-18 18:02:35][datar][WARNING] Builtin name "round" has been overriden by datar.
[2021-06-18 18:02:35][datar][WARNING] Builtin name "all" has been overriden by datar.
[2021-06-18 18:02:35][datar][WARNING] Builtin name "any" has been overriden by datar.
[2021-06-18 18:02:35][datar][WARNING] Builtin name "re" has been overriden by datar.
[2021-06-18 18:02:35][datar][WARNING] Builtin name "filter" has been overriden by datar.
[2021-06-18 18:02:35][datar][WARNING] Builtin name "slice" has been overriden by datar.
```

2. Import `_no_warn`

```python
>>> from datar.dplyr import _no_warn
>>> from datar.dplyr import * # ok
>>> from datar.dplyr import _warn
>>> from datar.dplyr import * # warn again
[2021-06-18 18:03:54][datar][WARNING] Builtin name "filter" has been overriden by datar.
[2021-06-18 18:03:54][datar][WARNING] Builtin name "slice" has been overriden by datar.
```

3. Let the `logger` hide the message

```python
>>> from datar.core import logger
>>> logger.setLevel(40)
>>> from datar.dplyr import * # ok
```

4. Use aliases instead

```python
>>> from datar.all import filter
[2021-06-24 10:06:19][datar][WARNING] Builtin name "filter" has been overriden by datar.
```

But this is Okay:
```python
>>> from datar.all import filter_
>>> filter_
<function filter at 0x7fc93396df80>
```

Or you could even aliase it to `filter` by yourself:
```python
>>> from datar.all import filter_ as filter # no warnings
>>> filter
<function filter at 0x7fc93396df80>
```
