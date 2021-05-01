# datar

Port of R data packages (especially from tidyverse): [tidyr][1], [dplyr][2], [tibble][4] and so on in python, using [pipda][3].

Unlike other similar packages in python that just mimic the piping sign, `datar` follows the API designs from the original packages as possible. So that nearly no extra effort is needed for those who are familar with those R packages to transition to python.

<!-- badges -->

[Documentation][5]

## Installtion

```shell
pip install -U datar
```

## Example usage

```python
from datar import f
from datar.dplyr import mutate, filter, if_else
from datar.tibble import tibble

df = tibble(
    x=range(4),
    y=['zero', 'one', 'two', 'three']
)
df >> mutate(z=f.x)
"""# output
   x      y  z
0  0   zero  0
1  1    one  1
2  2    two  2
3  3  three  3
"""

df >> mutate(z=if_else(f.x>1, 1, 0))
"""# output:
   x      y  z
0  0   zero  0
1  1    one  0
2  2    two  1
3  3  three  1
"""

df >> filter(f.x>1)
"""# output:
   x      y
0  2    two
1  3  three
"""

df >> mutate(z=if_else(f.x>1, 1, 0)) >> filter(f.z==1)
"""# output:
   x      y  z
0  2    two  1
1  3  three  1
"""
```

```python
# works with plotnine
import numpy
from datar.base import sin, pi
from plotnine import ggplot, aes, geom_line, theme_classic

df = tibble(x=numpy.linspace(0, 2*pi, 500))
(df >>
   mutate(y=sin(f.x), sign=if_else(f.y>=0, "positive", "negative")) >>
   ggplot(aes(x='x', y='y')) + theme_classic()
) + geom_line(aes(color='sign'), size=1.2)
```

![example](./example.png)

```python
# very easy to integrate with other libraries
# for example: klib
import klib
from pipda import register_verb
from datar.datasets import iris
from datar.dplyr import pull

dist_plot = register_verb(func=klib.dist_plot)
iris >> pull(f.Sepal_Length) >> dist_plot()
```

![example](./example2.png)

[1]: https://tidyr.tidyverse.org/index.html
[2]: https://dplyr.tidyverse.org/index.html
[3]: https://github.com/pwwang/pipda
[4]: https://tibble.tidyverse.org/index.html
[5]: https://pwwang.github.io/datar/
