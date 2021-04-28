
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
print(datasets.all_datasets())

# {'airquality': {'file': PosixPath('/path/to/datar/datasets/airquality.csv.gz'),
#                 'index': False},
#  'anscombe': {'file': PosixPath('/path/to/datar/datasets/anscombe.csv.gz'),
#               'index': False},
#  'band_instruments': {'file': PosixPath('/path/to/datar/datasets/band_instruments.csv.gz'),
#                       'index': False},
#  'band_instruments2': {'file': PosixPath('/path/to/datar/datasets/band_instruments2.csv.gz'),
#                        'index': False},
#  'band_members': {'file': PosixPath('/path/to/datar/datasets/band_members.csv.gz'),
#                   'index': False},
#  'billboard': {'file': PosixPath('/path/to/datar/datasets/billboard.csv.gz'),
#                'index': False},
#  'construction': {'file': PosixPath('/path/to/datar/datasets/construction.csv.gz'),
#                   'index': False},
#  'diamonds': {'file': PosixPath('/path/to/datar/datasets/diamonds.csv.gz'),
#               'index': False},
#  'fish_encounters': {'file': PosixPath('/path/to/datar/datasets/fish_encounters.csv.gz'),
#                      'index': False},
#  'iris': {'file': PosixPath('/path/to/datar/datasets/iris.csv.gz'),
#           'index': False},
#  'mtcars': {'file': PosixPath('/path/to/datar/datasets/mtcars.indexed.csv.gz'),
#             'index': True},
#  'population': {'file': PosixPath('/path/to/datar/datasets/population.csv.gz'),
#                 'index': False},
#  'relig_income': {'file': PosixPath('/path/to/datar/datasets/relig_income.csv.gz'),
#                   'index': False},
#  'smiths': {'file': PosixPath('/path/to/datar/datasets/smiths.csv.gz'),
#             'index': False},
#  'starwars': {'file': PosixPath('/path/to/datar/datasets/starwars.csv.gz'),
#               'index': False},
#  'state_abb': {'file': PosixPath('/path/to/datar/datasets/state_abb.csv.gz'),
#                'index': False},
#  'state_division': {'file': PosixPath('/path/to/datar/datasets/state_division.csv.gz'),
#                     'index': False},
#  'state_region': {'file': PosixPath('/path/to/datar/datasets/state_region.csv.gz'),
#                   'index': False},
#  'storms': {'file': PosixPath('/path/to/datar/datasets/storms.csv.gz'),
#             'index': False},
#  'table1': {'file': PosixPath('/path/to/datar/datasets/table1.csv.gz'),
#             'index': False},
#  'table2': {'file': PosixPath('/path/to/datar/datasets/table2.csv.gz'),
#             'index': False},
#  'table3': {'file': PosixPath('/path/to/datar/datasets/table3.csv.gz'),
#             'index': False},
#  'table4a': {'file': PosixPath('/path/to/datar/datasets/table4a.csv.gz'),
#              'index': False},
#  'table4b': {'file': PosixPath('/path/to/datar/datasets/table4b.csv.gz'),
#              'index': False},
#  'table5': {'file': PosixPath('/path/to/datar/datasets/table5.csv.gz'),
#             'index': False},
#  'us_rent_income': {'file': PosixPath('/path/to/datar/datasets/us_rent_income.csv.gz'),
#                     'index': False},
#  'warpbreaks': {'file': PosixPath('/path/to/datar/datasets/warpbreaks.csv.gz'),
#                 'index': False},
#  'who': {'file': PosixPath('/path/to/datar/datasets/who.csv.gz'),
#          'index': False},
#  'world_bank_pop': {'file': PosixPath('/path/to/datar/datasets/world_bank_pop.csv.gz'),
#                     'index': False}}
```

`file` shows the path to the csv file of the dataset, and `index` shows if it has index (rownames).

!!! Note

    The column names are altered by replace `.` to `_`. For example `Sepal.Width` to `Sepal_Width`.
