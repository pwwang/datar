# count
Count observations by group

See dplyr reference: [https://dplyr.tidyverse.org/reference/count.html](https://dplyr.tidyverse.org/reference/count.html)


## Side-by-side comparison


<table width=100%>
<tr>
<th> plyrda </th>
<th> dplyr </th>
</tr>

<tr valign="top">
<td>

```python
from plyrda.all import *
from plyrda.data import starwars

starwars >> count(X.species) >> head(10)
#      species  n
# 0     Aleena  1
# 1   Besalisk  1
# 2     Cerean  1
# 3   Chagrian  1
# 4   Clawdite  1
# 5      Droid  6
# 6        Dug  1
# 7       Ewok  1
# 8  Geonosian  1
# 9     Gungan  3
```

</td>
<td>

```R
# count() is a convenient way to get a sense of the distribution of
# values in a dataset
starwars %>% count(species)
#> # A tibble: 38 x 2
#>    species       n
#>    <chr>     <int>
#>  1 Aleena        1
#>  2 Besalisk      1
#>  3 Cerean        1
#>  4 Chagrian      1
#>  5 Clawdite      1
#>  6 Droid         6
#>  7 Dug           1
#>  8 Ewok          1
#>  9 Geonosian     1
#> 10 Gungan        3
#> # … with 28 more rows
```

</td>
</tr>
<tr valign="top">
<td>

```python
starwars >> count(X.species, sort=True) >> head(10)
#        species   n
# 10       Human  35
# 5        Droid   6
# 9       Gungan   3
# 31     Twi'lek   2
# 16    Mirialan   2
# 14    Kaminoan   2
# 36      Zabrak   2
# 33     Wookiee   2
# 32  Vulptereen   1
# 30  Trandoshan   1
```

</td>
<td>

```R
starwars %>% count(species, sort = TRUE)
#> # A tibble: 38 x 2
#>    species      n
#>    <chr>    <int>
#>  1 Human       35
#>  2 Droid        6
#>  3 NA           4
#>  4 Gungan       3
#>  5 Kaminoan     2
#>  6 Mirialan     2
#>  7 Twi'lek      2
#>  8 Wookiee      2
#>  9 Zabrak       2
#> 10 Aleena       1
#> # … with 28 more rows
```

</td>
</tr>
<tr valign="top">
<td>

```python
starwars >> count(X.sex, X.gender, sort=True)
#               sex     gender   n
# 2            male  masculine  60
# 0          female   feminine  16
# 4            none  masculine   5
# 5             NaN        NaN   4
# 1  hermaphroditic  masculine   1
# 3            none   feminine   1
```

</td>
<td>

```R
starwars %>% count(sex, gender, sort = TRUE)
#> # A tibble: 6 x 3
#>   sex            gender        n
#>   <chr>          <chr>     <int>
#> 1 male           masculine    60
#> 2 female         feminine     16
#> 3 none           masculine     5
#> 4 NA             NA            4
#> 5 hermaphroditic masculine     1
#> 6 none           feminine      1
```

</td>
</tr>
<tr valign="top">
<td>

```python
starwars >> count(birth_decade = round(X.birth_year, -1))
#     birth_decade   n
# 0           10.0   1
# 1           20.0   6
# 2           30.0   4
# 3           40.0   6
# 4           50.0   8
# 5           60.0   4
# 6           70.0   4
# 7           80.0   2
# 8           90.0   3
# 9          100.0   1
# 10         110.0   1
# 11         200.0   1
# 12         600.0   1
# 13         900.0   1
# 14           NaN  44
```

</td>
<td>

```R
starwars %>% count(birth_decade = round(birth_year, -1))
#> # A tibble: 15 x 2
#>    birth_decade     n
#>           <dbl> <int>
#>  1           10     1
#>  2           20     6
#>  3           30     4
#>  4           40     6
#>  5           50     8
#>  6           60     4
#>  7           70     4
#>  8           80     2
#>  9           90     3
#> 10          100     1
#> 11          110     1
#> 12          200     1
#> 13          600     1
#> 14          900     1
#> 15           NA    44
```

</td>
</tr>
<tr valign="top">
<td>

```python
df = DataFrame({
    'name': ["Max", "Sandra", "Susan"],
    'gender': ["male", "female", "female"],
    'runs': [10, 1, 4]
})

df >> count(X.gender)
#    gender  n
# 0  female  2
# 1    male  1
```

</td>
<td>

```R
# use the `wt` argument to perform a weighted count. This is useful
# when the data has already been aggregated once
df <- tribble(
  ~name,    ~gender,   ~runs,
  "Max",    "male",       10,
  "Sandra", "female",      1,
  "Susan",  "female",      4
)
# counts rows:
df %>% count(gender)
#> # A tibble: 2 x 2
#>   gender     n
#>   <chr>  <int>
#> 1 female     2
#> 2 male       1
```

</td>
</tr>
<tr valign="top">
<td>

```python
df >> count(X.gender, wt=X.runs)
#    gender  runs
# 0  female     5
# 1    male    10
```

</td>
<td>

```R
# counts runs:
df %>% count(gender, wt = runs)
#> # A tibble: 2 x 2
#>   gender     n
#>   <chr>  <dbl>
#> 1 female     5
#> 2 male      10
```

</td>
</tr>
<tr valign="top">
<td>

```python
starwars >> tally()
   n
0  87
```

</td>
<td>

```R
# tally() is a lower-level function that assumes you've done the grouping
starwars %>% tally()
#> # A tibble: 1 x 1
#>       n
#>   <int>
#> 1    87
```

</td>
</tr>
<tr valign="top">
<td>

```python
starwars >> group_by(X.species) >> tally() >> head(10)
#      species  n
# 0     Aleena  1
# 1   Besalisk  1
# 2     Cerean  1
# 3   Chagrian  1
# 4   Clawdite  1
# 5      Droid  6
# 6        Dug  1
# 7       Ewok  1
# 8  Geonosian  1
# 9     Gungan  3
```

</td>
<td>

```R
starwars %>% group_by(species) %>% tally()
#> # A tibble: 38 x 2
#>    species       n
#>    <chr>     <int>
#>  1 Aleena        1
#>  2 Besalisk      1
#>  3 Cerean        1
#>  4 Chagrian      1
#>  5 Clawdite      1
#>  6 Droid         6
#>  7 Dug           1
#>  8 Ewok          1
#>  9 Geonosian     1
#> 10 Gungan        3
#> # … with 28 more rows
```

</td>
</tr>
<tr valign="top">
<td>

```python
     name  gender  runs   n
0     Max    male    10  10
1  Sandra  female     1   5
2   Susan  female     4   5
```

</td>
<td>

```R
# both count() and tally() have add_ variants that work like
# mutate() instead of summarise
df %>% add_count(gender, wt = runs)
#> # A tibble: 3 x 4
#>   name   gender  runs     n
#>   <chr>  <chr>  <dbl> <dbl>
#> 1 Max    male      10    10
#> 2 Sandra female     1     5
#> 3 Susan  female     4     5
```

</td>
</tr>
<tr valign="top">
<td>

```python
df >> add_tally(wt = X.runs)
#      name  gender  runs   n
# 0     Max    male    10  15
# 1  Sandra  female     1  15
# 2   Susan  female     4  15
```

</td>
<td>

```R
df %>% add_tally(wt = runs)
#> # A tibble: 3 x 4
#>   name   gender  runs     n
#>   <chr>  <chr>  <dbl> <dbl>
#> 1 Max    male      10    15
#> 2 Sandra female     1    15
#> 3 Susan  female     4    15
```

</td>
</tr>
</table>
