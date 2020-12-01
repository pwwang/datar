# mutate
Create, modify, and delete columns

See dplyr reference: [https://dplyr.tidyverse.org/reference/mutate.html](https://dplyr.tidyverse.org/reference/mutate.html)

## Side-by-side comparison

<table width=100%>
<tr>
<th> plyrda </th>
<th> dplyr </th>
</tr>
<tr valign="top">
<td>
<tr valign="top">
<td>

```python
from plyrda.all import *
from plyrda.data import starwars

(starwars >>
  select(X.name, X.mass) >>
  mutate(mass2=X.mass * 2, mass2_squared = X.mass2 * X.mass2))

#               name   mass  mass2  mass2_squared
# 0   Luke Skywalker   77.0  154.0        23716.0
# 1            C-3PO   75.0  150.0        22500.0
# 2            R2-D2   32.0   64.0         4096.0
# 3      Darth Vader  136.0  272.0        73984.0
# 4      Leia Organa   49.0   98.0         9604.0
# ..             ...    ...    ...            ...
# 82             Rey    NaN    NaN            NaN
# 83     Poe Dameron    NaN    NaN            NaN
# 84             BB8    NaN    NaN            NaN
# 85  Captain Phasma    NaN    NaN            NaN
# 86   Padmé Amidala   45.0   90.0         8100.0
```

</td>
<td>

```R
# Newly created variables are available immediately
starwars %>%
 select(name, mass) %>%
 mutate(
  mass2 = mass * 2,
  mass2_squared = mass2 * mass2
)
#> # A tibble: 87 x 4
#>    name                mass mass2 mass2_squared
#>    <chr>              <dbl> <dbl>         <dbl>
#>  1 Luke Skywalker        77   154         23716
#>  2 C-3PO                 75   150         22500
#>  3 R2-D2                 32    64          4096
#>  4 Darth Vader          136   272         73984
#>  5 Leia Organa           49    98          9604
#>  6 Owen Lars            120   240         57600
#>  7 Beru Whitesun lars    75   150         22500
#>  8 R5-D4                 32    64          4096
#>  9 Biggs Darklighter     84   168         28224
#> 10 Obi-Wan Kenobi        77   154         23716
#> # … with 77 more rows
```

</td>
</tr>

<tr valign="top">
<td>

```python
(starwars >>
  select(X.name, X.height, X.mass, X.homeworld) >>
  mutate(mass=None, height = X.height * 0.0328084))

#               name    height homeworld
# 0   Luke Skywalker  5.643045  Tatooine
# 1            C-3PO  5.479003  Tatooine
# 2            R2-D2  3.149606     Naboo
# 3      Darth Vader  6.627297  Tatooine
# 4      Leia Organa  4.921260  Alderaan
# ..             ...       ...       ...
# 82             Rey       NaN       NaN
# 83     Poe Dameron       NaN       NaN
# 84             BB8       NaN       NaN
# 85  Captain Phasma       NaN       NaN
# 86   Padmé Amidala  5.413386     Naboo

# [87 rows x 3 columns]
```

</td>
<td>

```R
# As well as adding new variables, you can use mutate() to
# remove variables and modify existing variables.
starwars %>%
 select(name, height, mass, homeworld) %>%
 mutate(
  mass = NULL,
  height = height * 0.0328084 # convert to feet
)
#> # A tibble: 87 x 3
#>    name               height homeworld
#>    <chr>               <dbl> <chr>
#>  1 Luke Skywalker       5.64 Tatooine
#>  2 C-3PO                5.48 Tatooine
#>  3 R2-D2                3.15 Naboo
#>  4 Darth Vader          6.63 Tatooine
#>  5 Leia Organa          4.92 Alderaan
#>  6 Owen Lars            5.84 Tatooine
#>  7 Beru Whitesun lars   5.41 Tatooine
#>  8 R5-D4                3.18 Tatooine
#>  9 Biggs Darklighter    6.00 Tatooine
#> 10 Obi-Wan Kenobi       5.97 Stewjon
#> # … with 77 more rows
```

</td>
</tr>
<tr valign="top">
<td>

```python
# as_factor turn the column into dtype of category
s = starwars >> \
  select(X.name, X.homeworld, X.species) >> \
  mutate(across(~X.name, as_factor))
s
#               name homeworld species
# 0   Luke Skywalker  Tatooine   Human
# 1            C-3PO  Tatooine   Droid
# 2            R2-D2     Naboo   Droid
# 3      Darth Vader  Tatooine   Human
# 4      Leia Organa  Alderaan   Human
# ..             ...       ...     ...
# 82             Rey       NaN   Human
# 83     Poe Dameron       NaN   Human
# 84             BB8       NaN   Droid
# 85  Captain Phasma       NaN     NaN
# 86   Padmé Amidala     Naboo   Human

# [87 rows x 3 columns]

s.homeworld
# 0     Tatooine
# 1     Tatooine
# 2        Naboo
# 3     Tatooine
# 4     Alderaan
#         ...
# 82         NaN
# 83         NaN
# 84         NaN
# 85         NaN
# 86       Naboo
# Name: homeworld, Length: 87, dtype: category
# Categories (48, object): ['Alderaan', 'Aleen Minor', 'Bespin', 'Bestine IV', ..., 'Umbara', 'Utapau', 'Vulpter', 'Zolan']
```

</td>
<td>

```R
# Use across() with mutate() to apply a transformation
# to multiple columns in a tibble.
starwars %>%
 select(name, homeworld, species) %>%
 mutate(across(!name, as.factor))
#> # A tibble: 87 x 3
#>    name               homeworld species
#>    <chr>              <fct>     <fct>
#>  1 Luke Skywalker     Tatooine  Human
#>  2 C-3PO              Tatooine  Droid
#>  3 R2-D2              Naboo     Droid
#>  4 Darth Vader        Tatooine  Human
#>  5 Leia Organa        Alderaan  Human
#>  6 Owen Lars          Tatooine  Human
#>  7 Beru Whitesun lars Tatooine  Human
#>  8 R5-D4              Tatooine  Droid
#>  9 Biggs Darklighter  Tatooine  Human
#> 10 Obi-Wan Kenobi     Stewjon   Human
#> # … with 77 more rows
# see more in ?across
```

</td>
</tr>
<tr valign="top">
<td>

```python
(starwars >>
  select(X.name, X.mass, X.homeworld) >>
  group_by(X.homeworld) >>
  mutate(rank = min_rank(desc(X.mass))) >>
  head(10))
#                  name   mass homeworld  rank
# 0      Luke Skywalker   77.0  Tatooine   5.0
# 1               C-3PO   75.0  Tatooine   6.0
# 2               R2-D2   32.0     Naboo   6.0
# 3         Darth Vader  136.0  Tatooine   1.0
# 4         Leia Organa   49.0  Alderaan   2.0
# 5           Owen Lars  120.0  Tatooine   2.0
# 6  Beru Whitesun lars   75.0  Tatooine   6.0
# 7               R5-D4   32.0  Tatooine   8.0
# 8   Biggs Darklighter   84.0  Tatooine   3.0
# 9      Obi-Wan Kenobi   77.0   Stewjon   1.0
```

</td>
<td>

```R
# Window functions are useful for grouped mutates:
starwars %>%
 select(name, mass, homeworld) %>%
 group_by(homeworld) %>%
 mutate(rank = min_rank(desc(mass)))
#> # A tibble: 87 x 4
#> # Groups:   homeworld [49]
#>    name                mass homeworld  rank
#>    <chr>              <dbl> <chr>     <int>
#>  1 Luke Skywalker        77 Tatooine      5
#>  2 C-3PO                 75 Tatooine      6
#>  3 R2-D2                 32 Naboo         6
#>  4 Darth Vader          136 Tatooine      1
#>  5 Leia Organa           49 Alderaan      2
#>  6 Owen Lars            120 Tatooine      2
#>  7 Beru Whitesun lars    75 Tatooine      6
#>  8 R5-D4                 32 Tatooine      8
#>  9 Biggs Darklighter     84 Tatooine      3
#> 10 Obi-Wan Kenobi        77 Stewjon       1
#> # … with 77 more rows
```

</td>
</tr>
<tr valign="top">
<td>

```python
df = DataFrame({'x': [1], 'y': [2]})
df >> mutate(z=X.x + X.y)
#    x  y  z
# 0  1  2  3

# note python's index is 0-based
df >> mutate(z = X.x + X.y, _before = 0)
#    z  x  y
# 0  3  1  2

df >> mutate(z = X.x + X.y, _after = X.x)
#    x  z  y
# 0  1  3  2
```

</td>
<td>

```R
# By default, new columns are placed on the far right.
# Experimental: you can override with `.before` or `.after`
df <- tibble(x = 1, y = 2)
df %>% mutate(z = x + y)
#> # A tibble: 1 x 3
#>       x     y     z
#>   <dbl> <dbl> <dbl>
#> 1     1     2     3
df %>% mutate(z = x + y, .before = 1)
#> # A tibble: 1 x 3
#>       z     x     y
#>   <dbl> <dbl> <dbl>
#> 1     3     1     2
df %>% mutate(z = x + y, .after = x)
#> # A tibble: 1 x 3
#>       x     z     y
#>   <dbl> <dbl> <dbl>
#> 1     1     3     2
```

</td>
</tr>

<tr valign="top">
<td>

```python
df = DataFrame({'x': [1], 'y': [2], 'a': ['a'], 'b': ['b']})
df >> mutate(z = X.x + X.y, _keep='all')
#    x  y  a  b  z
# 0  1  2  a  b  3

# only works with proxies (X.x, X.y, X['x'], ...)
df >> mutate(z = X.x + X.y, _keep='used')
#    x  y  z
# 0  1  2  3

df >> mutate(z = X.x + X.y, _keep='unused')
#    a  b  z
# 0  a  b  3

df >> mutate(z = X.x + X.y, _keep='none')
#    z
# 0  3
```

</td>
<td>

```R
# By default, mutate() keeps all columns from the input data.
# Experimental: You can override with `.keep`
df <- tibble(x = 1, y = 2, a = "a", b = "b")
df %>% mutate(z = x + y, .keep = "all") # the default
#> # A tibble: 1 x 5
#>       x     y a     b         z
#>   <dbl> <dbl> <chr> <chr> <dbl>
#> 1     1     2 a     b         3
df %>% mutate(z = x + y, .keep = "used")
#> # A tibble: 1 x 3
#>       x     y     z
#>   <dbl> <dbl> <dbl>
#> 1     1     2     3
df %>% mutate(z = x + y, .keep = "unused")
#> # A tibble: 1 x 3
#>   a     b         z
#>   <chr> <chr> <dbl>
#> 1 a     b         3
df %>% mutate(z = x + y, .keep = "none") # same as transmute()
#> # A tibble: 1 x 1
#>       z
#>   <dbl>
#> 1     3
```

</td>
</tr>
<tr valign="top">
<td>

```python
(starwars >>
  select(X.name, X.mass, X.species) >>
  mutate(mass_norm = X.mass/mean(X.mass))
  >> head(10))
#                  name   mass species  mass_norm
# 0      Luke Skywalker   77.0   Human   0.791270
# 1               C-3PO   75.0   Droid   0.770718
# 2               R2-D2   32.0   Droid   0.328840
# 3         Darth Vader  136.0   Human   1.397569
# 4         Leia Organa   49.0   Human   0.503536
# 5           Owen Lars  120.0   Human   1.233149
# 6  Beru Whitesun lars   75.0   Human   0.770718
# 7               R5-D4   32.0   Droid   0.328840
# 8   Biggs Darklighter   84.0   Human   0.863204
# 9      Obi-Wan Kenobi   77.0   Human   0.791270
```

</td>
<td>

```R
# Grouping ----------------------------------------
# The mutate operation may yield different results on grouped
# tibbles because the expressions are computed within groups.
# The following normalises `mass` by the global average:
starwars %>%
  select(name, mass, species) %>%
  mutate(mass_norm = mass / mean(mass, na.rm = TRUE))
#> # A tibble: 87 x 4
#>    name                mass species mass_norm
#>    <chr>              <dbl> <chr>       <dbl>
#>  1 Luke Skywalker        77 Human       0.791
#>  2 C-3PO                 75 Droid       0.771
#>  3 R2-D2                 32 Droid       0.329
#>  4 Darth Vader          136 Human       1.40
#>  5 Leia Organa           49 Human       0.504
#>  6 Owen Lars            120 Human       1.23
#>  7 Beru Whitesun lars    75 Human       0.771
#>  8 R5-D4                 32 Droid       0.329
#>  9 Biggs Darklighter     84 Human       0.863
#> 10 Obi-Wan Kenobi        77 Human       0.791
#> # … with 77 more rows
```

</td>
</tr>
<tr valign="top">
<td>

```python
(starwars >>
  select(X.name, X.mass, X.species) >>
  group_by(X.species) >>
  mutate(mass_norm = X.mass/mean(X.mass))
  >> head(10))
#                  name   mass species  mass_norm
# 0      Luke Skywalker   77.0   Human   0.930156
# 1               C-3PO   75.0   Droid   1.075269
# 2               R2-D2   32.0   Droid   0.458781
# 3         Darth Vader  136.0   Human   1.642873
# 4         Leia Organa   49.0   Human   0.591917
# 5           Owen Lars  120.0   Human   1.449594
# 6  Beru Whitesun lars   75.0   Human   0.905996
# 7               R5-D4   32.0   Droid   0.458781
# 8   Biggs Darklighter   84.0   Human   1.014716
# 9      Obi-Wan Kenobi   77.0   Human   0.930156
```

</td>
<td>

```R
# Whereas this normalises `mass` by the averages within species
# levels:
starwars %>%
  select(name, mass, species) %>%
  group_by(species) %>%
  mutate(mass_norm = mass / mean(mass, na.rm = TRUE))
#> # A tibble: 87 x 4
#> # Groups:   species [38]
#>    name                mass species mass_norm
#>    <chr>              <dbl> <chr>       <dbl>
#>  1 Luke Skywalker        77 Human       0.930
#>  2 C-3PO                 75 Droid       1.08
#>  3 R2-D2                 32 Droid       0.459
#>  4 Darth Vader          136 Human       1.64
#>  5 Leia Organa           49 Human       0.592
#>  6 Owen Lars            120 Human       1.45
#>  7 Beru Whitesun lars    75 Human       0.906
#>  8 R5-D4                 32 Droid       0.459
#>  9 Biggs Darklighter     84 Human       1.01
#> 10 Obi-Wan Kenobi        77 Human       0.930
#> # … with 77 more rows
```

</td>
</tr>
<tr valign="top">
<td>

```python
vars = ["mass", "height"]
starwars >> mutate(prod=X[vars[0]] * X[vars[1]]) >> head(10)
#                  name  height   mass     hair_color   skin_color  eye_color  \
# 0      Luke Skywalker   172.0   77.0          blond         fair       blue
# 1               C-3PO   167.0   75.0            NaN         gold     yellow
# 2               R2-D2    96.0   32.0            NaN  white, blue        red
# 3         Darth Vader   202.0  136.0           none        white     yellow
# 4         Leia Organa   150.0   49.0          brown        light      brown
# 5           Owen Lars   178.0  120.0    brown, grey        light       blue
# 6  Beru Whitesun lars   165.0   75.0          brown        light       blue
# 7               R5-D4    97.0   32.0            NaN   white, red        red
# 8   Biggs Darklighter   183.0   84.0          black        light      brown
# 9      Obi-Wan Kenobi   182.0   77.0  auburn, white         fair  blue-gray

#    birth_year     sex     gender homeworld species     prod
# 0        19.0    male  masculine  Tatooine   Human  13244.0
# 1       112.0    none  masculine  Tatooine   Droid  12525.0
# 2        33.0    none  masculine     Naboo   Droid   3072.0
# 3        41.9    male  masculine  Tatooine   Human  27472.0
# 4        19.0  female   feminine  Alderaan   Human   7350.0
# 5        52.0    male  masculine  Tatooine   Human  21360.0
# 6        47.0  female   feminine  Tatooine   Human  12375.0
# 7         NaN    none  masculine  Tatooine   Droid   3104.0
# 8        24.0    male  masculine  Tatooine   Human  15372.0
# 9        57.0    male  masculine   Stewjon   Human  14014.0
```

</td>
<td>

```R
# Indirection ----------------------------------------
# Refer to column names stored as strings with the `.data` pronoun:
vars <- c("mass", "height")
mutate(starwars, prod = .data[[vars[[1]]]] * .data[[vars[[2]]]])
#> # A tibble: 87 x 15
#>    name  height  mass hair_color skin_color eye_color birth_year sex   gender
#>    <chr>  <int> <dbl> <chr>      <chr>      <chr>          <dbl> <chr> <chr>
#>  1 Luke…    172    77 blond      fair       blue            19   male  mascu…
#>  2 C-3PO    167    75 NA         gold       yellow         112   none  mascu…
#>  3 R2-D2     96    32 NA         white, bl… red             33   none  mascu…
#>  4 Dart…    202   136 none       white      yellow          41.9 male  mascu…
#>  5 Leia…    150    49 brown      light      brown           19   fema… femin…
#>  6 Owen…    178   120 brown, gr… light      blue            52   male  mascu…
#>  7 Beru…    165    75 brown      light      blue            47   fema… femin…
#>  8 R5-D4     97    32 NA         white, red red             NA   none  mascu…
#>  9 Bigg…    183    84 black      light      brown           24   male  mascu…
#> 10 Obi-…    182    77 auburn, w… fair       blue-gray       57   male  mascu…
#> # … with 77 more rows, and 6 more variables: homeworld <chr>, species <chr>,
#> #   films <list>, vehicles <list>, starships <list>, prod <dbl>
# Learn more in ?dplyr_data_masking
```

</td>
</tr>
</table