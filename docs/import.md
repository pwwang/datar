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

## Warn about python reserved names to be masked by `datar`

Sometimes it will be confusing especially when python builtin functions are overriden by `datar`. There are a couple of datar (`r-base`, `dplyr`) functions with the same name as python builtin functions. For example: `filter`.

To make you aware of the loss of access to builtin functions or other python preserved names, warnings will be reported when those names are imported directly:

```python
>>> from datar.dplyr import filter
[2021-06-18 17:58:23][datar][WARNING] Builtin name "filter" has been masked by datar.
>>> from datar.all import *
2021-06-18 17:58:48][datar][WARNING] Builtin name "min" has been masked by datar.
[2021-06-18 17:58:48][datar][WARNING] Builtin name "max" has been masked by datar.
[2021-06-18 17:58:48][datar][WARNING] Builtin name "sum" has been masked by datar.
[2021-06-18 17:58:48][datar][WARNING] Builtin name "abs" has been masked by datar.
[2021-06-18 17:58:48][datar][WARNING] Builtin name "round" has been masked by datar.
[2021-06-18 17:58:48][datar][WARNING] Builtin name "all" has been masked by datar.
[2021-06-18 17:58:48][datar][WARNING] Builtin name "any" has been masked by datar.
[2021-06-18 17:58:48][datar][WARNING] Builtin name "re" has been masked by datar.
[2021-06-18 17:58:48][datar][WARNING] Builtin name "slice" has been masked by datar.
```

However, when they are not imported directly, no warnings will show:

```python
from datar import dplyr
dplyr.filter # ok
```

There are a couple of other options for this behavior:

1. Use option: `import_names_conflict` or `import.names.conflict`

    ```python
    >>> from datar import options
    >>> options(import_names_conflict="silent")
    >>> from datar.all import * # ok
    >>> options(import_names_conflict="warn")
    >>> from datar.all import *
    [2021-06-18 18:02:35][datar][WARNING] Builtin name "min" has been masked by datar.
    [2021-06-18 18:02:35][datar][WARNING] Builtin name "max" has been masked by datar.
    [2021-06-18 18:02:35][datar][WARNING] Builtin name "sum" has been masked by datar.
    [2021-06-18 18:02:35][datar][WARNING] Builtin name "abs" has been masked by datar.
    [2021-06-18 18:02:35][datar][WARNING] Builtin name "round" has been masked by datar.
    [2021-06-18 18:02:35][datar][WARNING] Builtin name "all" has been masked by datar.
    [2021-06-18 18:02:35][datar][WARNING] Builtin name "any" has been masked by datar.
    [2021-06-18 18:02:35][datar][WARNING] Builtin name "re" has been masked by datar.
    [2021-06-18 18:02:35][datar][WARNING] Builtin name "filter" has been masked by datar.
    [2021-06-18 18:02:35][datar][WARNING] Builtin name "slice" has been masked by datar.
    ```

    If you don't want to use the conflict names at all:
    ```python
    >>> from datar import options
    >>> options(import_names_conflict="underscore_suffixed")
    >>> from datar.all import *
    >>> filter
    >>> # <class 'filter'>  # builtin filter
    >>> filter_
    >>> # <function filter at 0x7f76b34c0940>
    ```

    You can also change the default options and save them in the configuration file if you don't want to change the options every time you use `datar`. See [options](../options/#configuration-files) for details.

2. Let the `logger` hide the message

    ```python
    >>> from datar.core import logger
    >>> logger.setLevel(40)
    >>> from datar.dplyr import * # ok
    ```

3. Use aliases instead

    ```python
    >>> from datar.all import filter
    [2021-06-24 10:06:19][datar][WARNING] Builtin name "filter" has been masked by datar.
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
