# datar

A Grammar of Data Manipulation in python

<!-- badges -->
[![Pypi][6]][7] [![Github][8]][9] ![Building][10] [![Docs and API][11]][5] [![Codacy][12]][13] [![Codacy coverage][14]][13]

[Documentation][5] | [Reference Maps][15] | [Notebook Examples][16] | [API][17] | [Blog][18]

<img width="30%" style="margin: 10px 10px 10px 30px" align="right" src="logo.png">

`datar` is a re-imagining of APIs of data manipulation libraries in python (currently only `pandas` supported) so that you can manipulate your data with it like with `dplyr` in `R`.

`datar` is an in-depth port of `tidyverse` packages, such as `dplyr`, `tidyr`, `forcats` and `tibble`, as well as some functions from base R.

## Installation

```shell
pip install -U datar

# install pdtypes support
pip install -U datar[pdtypes]

# install dependencies for modin as backend
pip install -U datar[modin]
# you may also need to install dependencies for modin engines
# pip install -U modin[ray]
```

## Example usage

```python
from datar import f
from datar.dplyr import mutate, filter, if_else
from datar.tibble import tibble
# or
# from datar.all import f, mutate, filter, if_else, tibble

df = tibble(
    x=range(4),  # or c[:4]  (from datar.base import c)
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
# very easy to integrate with other libraries
# for example: klib
import klib
from pandas import Series
from pipda import register_verb
from datar.datasets import iris
from datar.dplyr import pull

dist_plot = register_verb(Series, func=klib.dist_plot)
iris >> pull(f.Sepal_Length) >> dist_plot()
```

![example](./example2.png)


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
