
See full reference of datasets at: [reference-maps/data][1]

Datasets have to be imported individually by:

```python
from datar.data import iris

# or
from datar import data

iris = data.iris
```

To list all available datasets:

```python
from datar import data
print(datasets.descr_datasets())
```

`file` shows the path to the csv file of the dataset, and `index` shows if it has index (rownames).

!!! Note

    The column names are altered by replace `.` to `_`. For example `Sepal.Width` to `Sepal_Width`.

!!! Note

    Dataset names are case-insensitive. So you can do:

    ```python
    from datar.datasets import ToothGrowth
    # or
    from datar.datasets import toothgrowth
    ```

See also [Backends][2] for implementations to loaad datasets.

[1]: ./reference-maps/data
[2]: ./backends
