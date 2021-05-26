
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

# {
#     "airlines": {
#         "file": "/home/pwwang/github/datar/datar/datasets/airlines.csv.gz",
#         "index": "False"
#     },
#     "airports": {
#         "file": "/home/pwwang/github/datar/datar/datasets/airports.csv.gz",
#         "index": "False"
#     },
#     "airquality": {
#         "file": "/home/pwwang/github/datar/datar/datasets/airquality.csv.gz",
#         "index": "False"
#     },
#     "anscombe": {
#         "file": "/home/pwwang/github/datar/datar/datasets/anscombe.csv.gz",
#         "index": "False"
#     },
#     "band_instruments": {
#         "file": "/home/pwwang/github/datar/datar/datasets/band_instruments.csv.gz",
#         "index": "False"
#     },
#     "band_instruments2": {
#         "file": "/home/pwwang/github/datar/datar/datasets/band_instruments2.csv.gz",
#         "index": "False"
#     },
#     "band_members": {
#         "file": "/home/pwwang/github/datar/datar/datasets/band_members.csv.gz",
#         "index": "False"
#     },
#     "billboard": {
#         "file": "/home/pwwang/github/datar/datar/datasets/billboard.csv.gz",
#         "index": "False"
#     },
#     "construction": {
#         "file": "/home/pwwang/github/datar/datar/datasets/construction.csv.gz",
#         "index": "False"
#     },
#     "diamonds": {
#         "file": "/home/pwwang/github/datar/datar/datasets/diamonds.csv.gz",
#         "index": "False"
#     },
#     "fish_encounters": {
#         "file": "/home/pwwang/github/datar/datar/datasets/fish_encounters.csv.gz",
#         "index": "False"
#     },
#     "flights": {
#         "file": "/home/pwwang/github/datar/datar/datasets/flights.csv.gz",
#         "index": "False"
#     },
#     "iris": {
#         "file": "/home/pwwang/github/datar/datar/datasets/iris.csv.gz",
#         "index": "False"
#     },
#     "mtcars": {
#         "file": "/home/pwwang/github/datar/datar/datasets/mtcars.indexed.csv.gz",
#         "index": "True"
#     },
#     "planes": {
#         "file": "/home/pwwang/github/datar/datar/datasets/planes.csv.gz",
#         "index": "False"
#     },
#     "population": {
#         "file": "/home/pwwang/github/datar/datar/datasets/population.csv.gz",
#         "index": "False"
#     },
#     "relig_income": {
#         "file": "/home/pwwang/github/datar/datar/datasets/relig_income.csv.gz",
#         "index": "False"
#     },
#     "smiths": {
#         "file": "/home/pwwang/github/datar/datar/datasets/smiths.csv.gz",
#         "index": "False"
#     },
#     "starwars": {
#         "file": "/home/pwwang/github/datar/datar/datasets/starwars.csv.gz",
#         "index": "False"
#     },
#     "state_abb": {
#         "file": "/home/pwwang/github/datar/datar/datasets/state_abb.csv.gz",
#         "index": "False"
#     },
#     "state_division": {
#         "file": "/home/pwwang/github/datar/datar/datasets/state_division.csv.gz",
#         "index": "False"
#     },
#     "state_region": {
#         "file": "/home/pwwang/github/datar/datar/datasets/state_region.csv.gz",
#         "index": "False"
#     },
#     "storms": {
#         "file": "/home/pwwang/github/datar/datar/datasets/storms.csv.gz",
#         "index": "False"
#     },
#     "table1": {
#         "file": "/home/pwwang/github/datar/datar/datasets/table1.csv.gz",
#         "index": "False"
#     },
#     "table2": {
#         "file": "/home/pwwang/github/datar/datar/datasets/table2.csv.gz",
#         "index": "False"
#     },
#     "table3": {
#         "file": "/home/pwwang/github/datar/datar/datasets/table3.csv.gz",
#         "index": "False"
#     },
#     "table4a": {
#         "file": "/home/pwwang/github/datar/datar/datasets/table4a.csv.gz",
#         "index": "False"
#     },
#     "table4b": {
#         "file": "/home/pwwang/github/datar/datar/datasets/table4b.csv.gz",
#         "index": "False"
#     },
#     "table5": {
#         "file": "/home/pwwang/github/datar/datar/datasets/table5.csv.gz",
#         "index": "False"
#     },
#     "us_rent_income": {
#         "file": "/home/pwwang/github/datar/datar/datasets/us_rent_income.csv.gz",
#         "index": "False"
#     },
#     "warpbreaks": {
#         "file": "/home/pwwang/github/datar/datar/datasets/warpbreaks.csv.gz",
#         "index": "False"
#     },
#     "weather": {
#         "file": "/home/pwwang/github/datar/datar/datasets/weather.csv.gz",
#         "index": "False"
#     },
#     "who": {
#         "file": "/home/pwwang/github/datar/datar/datasets/who.csv.gz",
#         "index": "False"
#     },
#     "world_bank_pop": {
#         "file": "/home/pwwang/github/datar/datar/datasets/world_bank_pop.csv.gz",
#         "index": "False"
#     }
# }
```

`file` shows the path to the csv file of the dataset, and `index` shows if it has index (rownames).

!!! Note

    The column names are altered by replace `.` to `_`. For example `Sepal.Width` to `Sepal_Width`.
