Options are used to change some behaviors in `datar`.

## Available options

### allow_conflict_names

Whether to allow conflict names that reversed by python. For example, `filter` is a python builtin function, but also a `dplyr` function. You should use `filter_` instead. By default, `datar` will raise an error when you try to import `filter`. You can set this option to `True` to allow this behavior.

```python
>>> from datar.all import filter
>>> # or from datar.dplyr import filter
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
ImportError: cannot import name 'filter' from 'datar.all'
```

```python
>>> from datar import options
>>> options(allow_conflict_names=True)
>>> from datar.all import filter
>>> filter
<function filter_ at 0x7f76b34c0940>
```

The conflict names under `datar.base` are:

- `min`
- `max`
- `sum`
- `abs`
- `round`
- `all`
- `any`
- `re`

The conflict names under `datar.dplyr` are:

- `filter`
- `slice`

### backends

If you have multiple backends installed, you can use this option to specify which backends to use.

## Configuration files

You can change the default behavior of datar by configuring a `.toml.toml` file in your home directory. For example, to always use underscore-suffixed names for conflicting names, you can add the following to your `~/.datar.toml` file:

```toml
allow_conflict_names = true
```

You can also have a project/directory-based configuration file (`./.datar.toml`) in your current working directory, which has higher priority than the home directory configuration file.
