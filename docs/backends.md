# Backends

The `datar` package is a collection of APIs that are ported from a bunch of R packages. The APIs are implemented in a backend-agnostic way, so that they can be used with different backends. Currently, `datar` supports the following backends:

- [`numpy`](https://github.com/pwwang/datar-numpy): Mostly the implementations of functions from `datar.base`.
- [`pandas`](https://github.com/pwwang/datar-pandas): Implementations using `pandas` as backend.

## Installation of a backend

```bash
pip install -U datar[<pandas>]
```

## Using desired backends

You can install multiple backends, but can use a subset of them.

```python
from datar import options

options(backends=['pandas'])

# Import the API functions then
```

## Writing a backend

A backend is supposed to implement as a `Simplug` plugin. There are a hooks to be implemented.

### Hooks

- `setup()`: calleed before any API is imported. You can do some setup here.
- `get_versions()`: return a dict of versions of the dependencies of the backend. The keys are the names of the packages, and the values are the versions.
- `load_dataset(name: str, metadata: Mapping)`: load a dataset, which can be loaded using `from datar.data import <dataset>`.
- `base_api()`: load the implementation of `datar.apis.base`.
- `dplyr_api()`: load the implementation of `datar.apis.dplyr`.
- `tibble_api()`: load the implementation of `datar.apis.tibble`.
- `forcats_api()`: load the implementation of `datar.apis.forcats`.
- `tidyr_api()`: load the implementation of `datar.apis.tidyr`.
- `other_api()`: load other backend-specific APIs.
- `c_getitem(item)`: load the implementation of `datar.base.c.__getitem__` (`c[...]`).
- `operate(op: str, x: Any, y: Any = None)`: load the implementation of the operators.

## Seleting a backend at runtime

You can use `__backend` to select a backend at runtime.

```python
from datar.tibble import tibble

tibble(..., __backend="pandas")
```

## Selecting a backend for operators

If you have multiple backends installed, you can select a backend for operators.

```python
from datar.core.operator import DatarOperator

DatarOperator.backend = "pandas"

# Or use the context manager
with DatarOperator.with_backend("pandas"):
    data >> mutate(z=f.x + f.y)
```

## Selecting a backend for `c[]`

```python
from datar.base import c

c.backend = "pandas"

# Or use the context manager
with c.with_backend("pandas"):
    data >> mutate(z=c[1:3])
```

## Selecting a backend for numpy ufuncs

```python
from datar.apis.other import array_ufunc

array_ufunc.backend = "pandas"

# Or use the context manager
with array_ufunc.with_backend("pandas"):
    data >> mutate(z=np.sin(f.x))
```
