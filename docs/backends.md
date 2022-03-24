`datar` by default uses `pandas` as backend. But you can switch it to `modin`.

## Installing dependencies for `modin`

To install the dependencies for `modin` as backend, run the following command:

```shell
pip install -U datar[modin]
```

You probably also need to install dependencies for `modin` engines, for example:
```shell
pip install -U modin[ray]
```

## Switching backend to `modin`

To switch backend:

```python
# before you import other modules from datar
from datar import options
options(backend='modin')

# Now import other modules from datar
```

Also checkout the [documentation](https://modin.readthedocs.io/en/latest/getting_started/quickstart.html) on modin usage

## Checking which backend is being used

```python
>>> from datar import get_option
>>> get_option("backend")
'modin'
```
