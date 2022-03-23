# datar

A Grammar of Data Manipulation in python

<!-- badges -->
[![Pypi][6]][7] [![Github][8]][9] ![Building][10] [![Docs and API][11]][5] [![Codacy][12]][13] [![Codacy coverage][14]][13]

[Documentation][5] | [Reference Maps][15] | [Notebook Examples][16] | [API][17] | [Blog][18]

<img width="30%" style="margin: 10px 10px 10px 30px" align="right" src="logo.png">

`datar` is a re-imagining of APIs of data manipulation libraries in python (currently only `pandas` supported) so that you can manipulate your data with it like with `dplyr` in `R`.

`datar` is an in-depth port of `tidyverse` packages, such as `dplyr`, `tidyr`, `forcats` and `tibble`, as well as some functions from `R` itself.

## Installtion

```shell
pip install -U datar
```
or
```shell
conda install -c conda-forge datar
# mamba install -c conda-forge datar
```

## Example usage

```python
from datar import f
from datar.dplyr import mutate, filter, if_else
from datar.tibble import tibble
# or
# from datar.all import f, mutate, filter, if_else, tibble

df = tibble(
    x=range(4),  # or f[:4]
    y=['zero', 'one', 'two', 'three']
)
df >> mutate(z=f.x)
"""# output
        x        y       z
  <int64> <object> <int64>
0       0     zero       0
1       1      one       1
2       2      two       2
3       3    three       3
"""

df >> mutate(z=if_else(f.x>1, 1, 0))
"""# output:
        x        y       z
  <int64> <object> <int64>
0       0     zero       0
1       1      one       0
2       2      two       1
3       3    three       1
"""

df >> filter(f.x>1)
"""# output:
        x        y
  <int64> <object>
0       2      two
1       3    three
"""

df >> mutate(z=if_else(f.x>1, 1, 0)) >> filter(f.z==1)
"""# output:
        x        y       z
  <int64> <object> <int64>
0       2      two       1
1       3    three       1
"""
```

```python
# works with plotnine
# example grabbed from https://github.com/has2k1/plydata
import numpy
from datar.base import sin, pi
from plotnine import ggplot, aes, geom_line, theme_classic

df = tibble(x=numpy.linspace(0, 2*pi, 500))
(df >>
  mutate(y=sin(f.x), sign=if_else(f.y>=0, "positive", "negative")) >>
  ggplot(aes(x='x', y='y')) +
  theme_classic() +
  geom_line(aes(color='sign'), size=1.2))
```

![example](./example.png)

```python
# easy to integrate with other libraries
# for example: klib
import klib
from datar.core.factory import verb_factory
from datar.datasets import iris
from datar.dplyr import pull

dist_plot = verb_factory(func=klib.dist_plot)
iris >> pull(f.Sepal_Length) >> dist_plot()
```

![example](./example2.png)

See also some advanced examples from my answers on StackOverflow:

- [Compare 2 DataFrames and drop rows that do not contain corresponding ID variables](https://stackoverflow.com/a/71532167/5088165)
- [count by id with dynamic criteria](https://stackoverflow.com/a/71519157/5088165)
- [counting the frequency in python size vs count](https://stackoverflow.com/a/71516503/5088165)
- [Pandas equivalent of R/dplyr group_by summarise concatenation](https://stackoverflow.com/a/71490832/5088165)
- [ntiles over columns in python using R's "mutate(across(cols = ..."](https://stackoverflow.com/a/71490501/5088165)
- [Replicate R Solution in Python for Calculating Monthly CRR](https://stackoverflow.com/a/71490194/5088165)
- [Best/Concise Way to Conditionally Concat two Columns in Pandas DataFrame](https://stackoverflow.com/a/71443587/5088165)
- [how to transform R dataframe to rows of indicator values](https://stackoverflow.com/a/71443515/5088165)
- [Left join on multiple columns](https://stackoverflow.com/a/71443441/5088165)
- [Python: change column of strings with None to 0/1](https://stackoverflow.com/a/71429016/5088165)
- [Comparing 2 data frames and finding values are not in 2nd data frame](https://stackoverflow.com/a/71415818/5088165)
- [How to compare two Pandas DataFrames based on specific columns in Python?](https://stackoverflow.com/a/71413499/5088165)
- [expand.grid equivalent to get pandas data frame for prediction in Python](https://stackoverflow.com/a/71376414/5088165)
- [Python pandas equivalent to R's group_by, mutate, and ifelse](https://stackoverflow.com/a/70387267/5088165)
- [How to convert a list of dictionaries to a Pandas Dataframe with one of the values as column name?](https://stackoverflow.com/a/69094005/5088165)
- [Moving window on a Standard Deviation & Mean calculation](https://stackoverflow.com/a/69093067/5088165)
- [Python: creating new "interpolated" rows based on a specific field in Pandas](https://stackoverflow.com/a/69092696/5088165)
- [How would I extend a Pandas DataFrame such as this?](https://stackoverflow.com/a/69092067/5088165)
- [How to define new variable based on multiple conditions in Pandas - dplyr case_when equivalent](https://stackoverflow.com/a/69080870/5088165)
- [What is the Pandas equivalent of top_n() in dplyr?](https://stackoverflow.com/a/69080806/5088165)
- [Equivalent of fct_lump in pandas](https://stackoverflow.com/a/69080727/5088165)
- [pandas equivalent of fct_reorder](https://stackoverflow.com/a/69080638/5088165)
- [Is there a way to find out the 2 X 2 contingency table consisting of the count of values by applying a condition from two dataframe](https://stackoverflow.com/a/68674345/5088165)
- [Count if array in pandas](https://stackoverflow.com/a/68659334/5088165)
- [How to create a new column for transposed data](https://stackoverflow.com/a/68642891/5088165)
- [How to create new DataFrame based on conditions from another DataFrame](https://stackoverflow.com/a/68640494/5088165)
- [Refer to column of a data frame that is being defined](https://stackoverflow.com/a/68308077/5088165)
- [How to use regex in mutate dplython to add new column](https://stackoverflow.com/a/68308033/5088165)
- [Multiplying a row by the previous row (with a certain name) in Pandas](https://stackoverflow.com/a/68137136/5088165)
- [Create dataframe from rows under a row with a certain condition](https://stackoverflow.com/a/68137089/5088165)
- [pandas data frame, group by multiple cols and put other columns' contents in one](https://stackoverflow.com/a/68136982/5088165)
- [Pandas custom aggregate function with condition on group, is it possible?](https://stackoverflow.com/a/68136704/5088165)
- [multiply different values to pandas column with combination of other columns](https://stackoverflow.com/a/68136300/5088165)
- [Vectorized column-wise regex matching in pandas](https://stackoverflow.com/a/68124082/5088165)
- [Iterate through and conditionally append string values in a Pandas dataframe](https://stackoverflow.com/a/68123912/5088165)
- [Groupby mutate equivalent in pandas/python using tidydata principles](https://stackoverflow.com/a/68123753/5088165)
- [More ...](https://stackoverflow.com/search?q=user%3A5088165+and+%5Bpandas%5D)


[1]: https://tidyr.tidyverse.org/index.html
[2]: https://dplyr.tidyverse.org/index.html
[3]: https://github.com/pwwang/pipda
[4]: https://tibble.tidyverse.org/index.html
[5]: https://pwwang.github.io/datar/
[6]: https://img.shields.io/pypi/v/datar?style=flat-square
[7]: https://pypi.org/project/datar/
[8]: https://img.shields.io/github/v/tag/pwwang/datar?style=flat-square
[9]: https://github.com/pwwang/datar
[10]: https://img.shields.io/github/workflow/status/pwwang/datar/Build%20and%20Deploy?style=flat-square
[11]: https://img.shields.io/github/workflow/status/pwwang/datar/Build%20Docs?label=Docs&style=flat-square
[12]: https://img.shields.io/codacy/grade/3d9bdff4d7a34bdfb9cd9e254184cb35?style=flat-square
[13]: https://app.codacy.com/gh/pwwang/datar
[14]: https://img.shields.io/codacy/coverage/3d9bdff4d7a34bdfb9cd9e254184cb35?style=flat-square
[15]: https://pwwang.github.io/datar/reference-maps/ALL/
[16]: https://pwwang.github.io/datar/notebooks/across/
[17]: https://pwwang.github.io/datar/api/datar/
[18]: https://pwwang.github.io/datar-blog
[19]: https://github.com/pwwang/datar-cli
