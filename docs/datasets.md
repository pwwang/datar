
See full reference of datasets at: [reference-maps/datasets][1]

Datasets have to be imported individually by:
```python
from datar.datasets import iris

# or
from datar import datasets

iris = datasets.iris
```

To list all avaiable datasets:

```python
from datar import datasets
print(datasets.list_datasets())
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

[1] ./reference-maps/datasets
