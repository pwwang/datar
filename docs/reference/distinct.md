# distinct
Subset distinct/unique rows

See dplyr reference: [https://dplyr.tidyverse.org/reference/distinct.html](https://dplyr.tidyverse.org/reference/distinct.html)


## Side-by-side comparison
<table width=100%>
<tr>
<th> datar </th>
<th> dplyr </th>
</tr>

<tr valign="top">
<td>

```python
from numpy.random import randint
from pandas import DataFrame
from datar.all import *
from datar.data import starwars

df = DataFrame({
    'x': randint(0, 10, 100),
    'y': randint(0, 10, 100),
})
df >> nrow()
# 100
df >> distinct() >> nrow()
# 65
df >> distinct(X.x, X.y) >> nrow()
# 65

df >> distinct(X.x)
#      x
# 0    8
# 1    3
# 2    7
# 3    9
# 4    0
# 5    4
# 6    5
# 7    2
# 8    6
# 9    1
```

</td>
<td>

```R
df <- tibble(
  x = sample(10, 100, rep = TRUE),
  y = sample(10, 100, rep = TRUE)
)
nrow(df)
#> [1] 100
nrow(distinct(df))
#> [1] 69
nrow(distinct(df, x, y))
#> [1] 69

distinct(df, x)
#> # A tibble: 10 x 1
#>        x
#>    <int>
#>  1     6
#>  2     9
#>  3     1
#>  4     4
#>  5     2
#>  6     8
#>  7     7
#>  8    10
#>  9     5
#> 10     3
```

</td>
</tr>
<tr valign="top">
<td>

```python
df >> distinct(X.y)
#    y
# 0  1
# 1  7
# 2  0
# 3  8
# 4  6
# 5  2
# 6  4
# 7  9
# 8  5
# 9  3
```

</td>
<td>

```R
distinct(df, y)
#> # A tibble: 10 x 1
#>        y
#>    <int>
#>  1     3
#>  2     9
#>  3    10
#>  4     1
#>  5     8
#>  6     6
#>  7     5
#>  8     4
#>  9     2
#> 10     7
```

</td>
</tr>
<tr valign="top">
<td>

```python
df >> distinct(X.x, _keep_all=True)
#    x  y
# 0  8  1
# 1  3  7
# 2  7  0
# 3  9  6
# 4  0  2
# 5  4  9
# 6  5  8
# 7  2  0
# 8  6  4
# 9  1  4
```

</td>
<td>

```R
# You can choose to keep all other variables as well
distinct(df, x, .keep_all = TRUE)
#> # A tibble: 10 x 2
#>        x     y
#>    <int> <int>
#>  1     6     3
#>  2     9     9
#>  3     1    10
#>  4     4     8
#>  5     2     6
#>  6     8    10
#>  7     7     6
#>  8    10     3
#>  9     5     6
#> 10     3     6
```

</td>
</tr>
<tr valign="top">
<td>

```python
df >> distinct(X.y, _keep_all=True)
#    x  y
# 0  8  1
# 1  3  7
# 2  7  0
# 3  9  6
# 4  0  2
# 5  4  9
# 6  5  8
# 7  2  0
# 8  6  4
# 9  1  4
```

</td>
<td>

```R
distinct(df, y, .keep_all = TRUE)
#> # A tibble: 10 x 2
#>        x     y
#>    <int> <int>
#>  1     6     3
#>  2     9     9
#>  3     1    10
#>  4     9     1
#>  5     4     8
#>  6     2     6
#>  7     7     5
#>  8     8     4
#>  9     7     2
#> 10     9     7
```

</td>
</tr>
<tr valign="top">
<td>

```python
df >> distinct(diff = abs(X.x - X.y))
#    diff
# 0     2
# 1     3
# 2     7
# 3     5
# 4     0
# 5     8
# 6     1
# 7     4
# 8     6
# 9     9
```

</td>
<td>

```R
# You can also use distinct on computed variables
distinct(df, diff = abs(x - y))
#> # A tibble: 10 x 1
#>     diff
#>    <int>
#>  1     3
#>  2     0
#>  3     9
#>  4     8
#>  5     4
#>  6     2
#>  7     1
#>  8     7
#>  9     6
#> 10     5
```

</td>
</tr>
<tr valign="top">
<td>

```python
starwars >> distinct(across(contains('color')))
#    hair_color   skin_color eye_color
# 0       blond         fair      blue
# 1         NaN         gold    yellow
# 2         NaN  white, blue       red
# 3        none        white    yellow
# 4       brown        light     brown
# ..        ...          ...       ...
# 62       none         pale     white
# 63      black         dark      dark
# 64      brown        light     hazel
# 65       none         none     black
# 66    unknown      unknown   unknown

# [67 rows x 3 columns]
```

</td>
<td>

```R
# use across() to access select()-style semantics
distinct(starwars, across(contains("color")))
#> # A tibble: 67 x 3
#>    hair_color    skin_color  eye_color
#>    <chr>         <chr>       <chr>
#>  1 blond         fair        blue
#>  2 NA            gold        yellow
#>  3 NA            white, blue red
#>  4 none          white       yellow
#>  5 brown         light       brown
#>  6 brown, grey   light       blue
#>  7 brown         light       blue
#>  8 NA            white, red  red
#>  9 black         light       brown
#> 10 auburn, white fair        blue-gray
#> # … with 57 more rows
```

</td>
</tr>
<tr valign="top">
<td>

```python
df >> group_by('g') >> distinct('x')
#    g  x
# 0  1  1
# 1  2  2
# 2  2  1
```

</td>
<td>

```R
# Grouping -------------------------------------------------
# The same behaviour applies for grouped data frames,
# except that the grouping variables are always included
df <- tibble(
  g = c(1, 1, 2, 2),
  x = c(1, 1, 2, 1)
) %>% group_by(g)
df %>% distinct(x)
#> # A tibble: 3 x 2
#> # Groups:   g [2]
#>       g     x
#>   <dbl> <dbl>
#> 1     1     1
#> 2     2     2
#> 3     2     1
```

</td>
</tr>
</table>