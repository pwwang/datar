# datar

A Grammar of Data Manipulation in python

<!-- badges -->
[![Pypi][6]][7] [![Github][8]][9] ![Building][10] [![Docs and API][11]][5] [![Codacy][12]][13] [![Codacy coverage][14]][13] [![Downloads][20]][7]

[Documentation][5] | [Reference Maps][15] | [Notebook Examples][16] | [API][17]

`datar` is a re-imagining of APIs for data manipulation in python with multiple backends supported. Those APIs are aligned with tidyrverse packages in R as much as possible.

## Installation

```shell
pip install -U datar

# install with a backend
pip install -U datar[pandas]

# More backends support coming soon
```

<!-- ## Maximum compatibility with R packages

|Package|Version|
|-|-|
|[dplyr][21]|1.0.8| -->

## Backends

|Repo|Badges|
|-|-|
|[datar-numpy][1]|![3] ![18]|
|[datar-pandas][2]|![4] ![19]|
|[datar-arrow][22]|![23] ![24]|

## Example usage

```python
# with pandas backend
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
from datar import f
from datar.base import sin, pi
from datar.tibble import tibble
from datar.dplyr import mutate, if_else
from plotnine import ggplot, aes, geom_line, theme_classic

df = tibble(x=numpy.linspace(0, 2 * pi, 500))
(
    df
    >> mutate(y=sin(f.x), sign=if_else(f.y >= 0, "positive", "negative"))
    >> ggplot(aes(x="x", y="y"))
    + theme_classic()
    + geom_line(aes(color="sign"), size=1.2)
)
```

![example](./example.png)

```python
# very easy to integrate with other libraries
# for example: klib
import klib
from pipda import register_verb
from datar import f
from datar.data import iris
from datar.dplyr import pull

dist_plot = register_verb(func=klib.dist_plot)
iris >> pull(f.Sepal_Length) >> dist_plot()
```

![example](./example2.png)

## Testimonials

[@coforfe](https://github.com/coforfe):
> Thanks for your excellent package to port R (`dplyr`) flow of processing to Python. I have been using other alternatives, and yours is the one that offers the most extensive and equivalent to what is possible now with `dplyr`.

[1]: https://github.com/pwwang/datar-numpy
[2]: https://github.com/pwwang/datar-pandas
[3]: https://img.shields.io/codacy/coverage/0a7519dad44246b6bab30576895f6766?style=flat-square
[4]: https://img.shields.io/codacy/coverage/45f4ea84ae024f1a8cf84be54dd144f7?style=flat-square
[5]: https://pwwang.github.io/datar/
[6]: https://img.shields.io/pypi/v/datar?style=flat-square
[7]: https://pypi.org/project/datar/
[8]: https://img.shields.io/github/v/tag/pwwang/datar?style=flat-square
[9]: https://github.com/pwwang/datar
[10]: https://img.shields.io/github/actions/workflow/status/pwwang/datar/ci.yml?branch=master&style=flat-square
[11]: https://img.shields.io/github/actions/workflow/status/pwwang/datar/docs.yml?branch=master&style=flat-square
[12]: https://img.shields.io/codacy/grade/3d9bdff4d7a34bdfb9cd9e254184cb35?style=flat-square
[13]: https://app.codacy.com/gh/pwwang/datar
[14]: https://img.shields.io/codacy/coverage/3d9bdff4d7a34bdfb9cd9e254184cb35?style=flat-square
[15]: https://pwwang.github.io/datar/reference-maps/ALL/
[16]: https://pwwang.github.io/datar/notebooks/across/
[17]: https://pwwang.github.io/datar/api/datar/
[18]: https://img.shields.io/pypi/v/datar-numpy?style=flat-square
[19]: https://img.shields.io/pypi/v/datar-pandas?style=flat-square
[20]: https://img.shields.io/pypi/dm/datar?style=flat-square
[21]: https://github.com/tidyverse/dplyr
[22]: https://github.com/pwwang/datar-arrow
[23]: https://img.shields.io/codacy/coverage/5f4ef9dd2503437db18786ff9e841d8b?style=flat-square
[24]: https://img.shields.io/pypi/v/datar-arrow?style=flat-square
