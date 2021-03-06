# filter
Subset rows using column values

See dplyr reference: [https://dplyr.tidyverse.org/reference/filter.html](https://dplyr.tidyverse.org/reference/filter.html)

## Side-by-side comparison

<table width=100%>
<tr>
<th> datar </th>
<th> dplyr </th>
</tr>
<tr valign="top">
<td>

```python
from datar.all import *
from datar.data import starwars

starwars >> filter(X.species == 'Human')
#                    name  height   mass     hair_color skin_color  eye_color  \
# 0        Luke Skywalker   172.0   77.0          blond       fair       blue
# 3           Darth Vader   202.0  136.0           none      white     yellow
# 4           Leia Organa   150.0   49.0          brown      light      brown
# 5             Owen Lars   178.0  120.0    brown, grey      light       blue
# 6    Beru Whitesun lars   165.0   75.0          brown      light       blue
# 8     Biggs Darklighter   183.0   84.0          black      light      brown
# 9        Obi-Wan Kenobi   182.0   77.0  auburn, white       fair  blue-gray
# 10     Anakin Skywalker   188.0   84.0          blond       fair       blue
# 11       Wilhuff Tarkin   180.0    NaN   auburn, grey       fair       blue
# 13             Han Solo   180.0   80.0          brown       fair      brown
# 16       Wedge Antilles   170.0   77.0          brown       fair      hazel
# 17     Jek Tono Porkins   180.0  110.0          brown       fair       blue
# 19            Palpatine   170.0   75.0           grey       pale     yellow
# 20            Boba Fett   183.0   78.2          black       fair      brown
# 23     Lando Calrissian   177.0   79.0          black       dark      brown
# 24                Lobot   175.0   79.0           none      light       blue
# 26           Mon Mothma   150.0    NaN         auburn       fair       blue
# 27         Arvel Crynyd     NaN    NaN          brown       fair      brown
# 30         Qui-Gon Jinn   193.0   89.0          brown       fair       blue
# 32        Finis Valorum   170.0    NaN          blond       fair       blue
# 40       Shmi Skywalker   163.0    NaN          black       fair      brown
# 47           Mace Windu   188.0   84.0           none       dark      brown
# 56         Gregar Typho   185.0   85.0          black       dark      brown
# 57                Cordé   157.0    NaN          brown      light      brown
# 58          Cliegg Lars   183.0    NaN          brown       fair       blue
# 62                Dormé   165.0    NaN          brown      light      brown
# 63                Dooku   193.0   80.0          white       fair      brown
# 64  Bail Prestor Organa   191.0    NaN          black        tan      brown
# 65           Jango Fett   183.0   79.0          black        tan      brown
# 70           Jocasta Nu   167.0    NaN          white       fair       blue
# 78      Raymus Antilles   188.0   79.0          brown      light      brown
# 81                 Finn     NaN    NaN          black       dark       dark
# 82                  Rey     NaN    NaN          brown      light      hazel
# 83          Poe Dameron     NaN    NaN          brown      light      brown
# 86        Padmé Amidala   165.0   45.0          brown      light      brown

#     birth_year     sex     gender     homeworld species
# 0         19.0    male  masculine      Tatooine   Human
# 3         41.9    male  masculine      Tatooine   Human
# 4         19.0  female   feminine      Alderaan   Human
# 5         52.0    male  masculine      Tatooine   Human
# 6         47.0  female   feminine      Tatooine   Human
# 8         24.0    male  masculine      Tatooine   Human
# 9         57.0    male  masculine       Stewjon   Human
# 10        41.9    male  masculine      Tatooine   Human
# 11        64.0    male  masculine        Eriadu   Human
# 13        29.0    male  masculine      Corellia   Human
# 16        21.0    male  masculine      Corellia   Human
# 17         NaN    male  masculine    Bestine IV   Human
# 19        82.0    male  masculine         Naboo   Human
# 20        31.5    male  masculine        Kamino   Human
# 23        31.0    male  masculine       Socorro   Human
# 24        37.0    male  masculine        Bespin   Human
# 26        48.0  female   feminine     Chandrila   Human
# 27         NaN    male  masculine           NaN   Human
# 30        92.0    male  masculine           NaN   Human
# 32        91.0    male  masculine     Coruscant   Human
# 40        72.0  female   feminine      Tatooine   Human
# 47        72.0    male  masculine    Haruun Kal   Human
# 56         NaN    male  masculine         Naboo   Human
# 57         NaN  female   feminine         Naboo   Human
# 58        82.0    male  masculine      Tatooine   Human
# 62         NaN  female   feminine         Naboo   Human
# 63       102.0    male  masculine       Serenno   Human
# 64        67.0    male  masculine      Alderaan   Human
# 65        66.0    male  masculine  Concord Dawn   Human
# 70         NaN  female   feminine     Coruscant   Human
# 78         NaN    male  masculine      Alderaan   Human
# 81         NaN    male  masculine           NaN   Human
# 82         NaN  female   feminine           NaN   Human
# 83         NaN    male  masculine           NaN   Human
# 86        46.0  female   feminine         Naboo   Human
```

</td>
<td>

```R
# Filtering by one criterion
filter(starwars, species == "Human")
#> # A tibble: 35 x 14
#>    name  height  mass hair_color skin_color eye_color birth_year sex   gender
#>    <chr>  <int> <dbl> <chr>      <chr>      <chr>          <dbl> <chr> <chr>
#>  1 Luke…    172    77 blond      fair       blue            19   male  mascu…
#>  2 Dart…    202   136 none       white      yellow          41.9 male  mascu…
#>  3 Leia…    150    49 brown      light      brown           19   fema… femin…
#>  4 Owen…    178   120 brown, gr… light      blue            52   male  mascu…
#>  5 Beru…    165    75 brown      light      blue            47   fema… femin…
#>  6 Bigg…    183    84 black      light      brown           24   male  mascu…
#>  7 Obi-…    182    77 auburn, w… fair       blue-gray       57   male  mascu…
#>  8 Anak…    188    84 blond      fair       blue            41.9 male  mascu…
#>  9 Wilh…    180    NA auburn, g… fair       blue            64   male  mascu…
#> 10 Han …    180    80 brown      fair       brown           29   male  mascu…
#> # … with 25 more rows, and 5 more variables: homeworld <chr>, species <chr>,
#> #   films <list>, vehicles <list>, starships <list>
```

</td>
</tr>

<tr valign="top">
<td>

```python
starwars >> filter(X.mass > 1000)
#                      name  height    mass hair_color        skin_color  \
# 15  Jabba Desilijic Tiure   175.0  1358.0        NaN  green-tan, brown

#    eye_color  birth_year             sex     gender  homeworld species
# 15    orange       600.0  hermaphroditic  masculine  Nal Hutta    Hutt
```

</td>
<td>

```R
filter(starwars, mass > 1000)
#> # A tibble: 1 x 14
#>   name  height  mass hair_color skin_color eye_color birth_year sex   gender
#>   <chr>  <int> <dbl> <chr>      <chr>      <chr>          <dbl> <chr> <chr>
#> 1 Jabb…    175  1358 NA         green-tan… orange           600 herm… mascu…
#> # … with 5 more variables: homeworld <chr>, species <chr>, films <list>,
#> #   vehicles <list>, starships <list>

# Filtering by multiple criteria within a single logical expression
```

</td>
</tr>
<tr valign="top">
<td>

```python
# & in python has higher priority than ==
starwars >> filter((X.hair_color == "none") & (X.eye_color == "black"))
#           name  height  mass hair_color        skin_color eye_color  \
# 29   Nien Nunb   160.0  68.0       none              grey     black
# 45     Gasgano   122.0   NaN       none       white, blue     black
# 49   Kit Fisto   196.0  87.0       none             green     black
# 54    Plo Koon   188.0  80.0       none            orange     black
# 68     Lama Su   229.0  88.0       none              grey     black
# 69     Taun We   213.0   NaN       none              grey     black
# 75    Shaak Ti   178.0  57.0       none  red, blue, white     black
# 80  Tion Medon   206.0  80.0       none              grey     black
# 84         BB8     NaN   NaN       none              none     black

#     birth_year     sex     gender    homeworld    species
# 29         NaN    male  masculine      Sullust  Sullustan
# 45         NaN    male  masculine      Troiken      Xexto
# 49         NaN    male  masculine  Glee Anselm   Nautolan
# 54        22.0    male  masculine        Dorin    Kel Dor
# 68         NaN    male  masculine       Kamino   Kaminoan
# 69         NaN  female   feminine       Kamino   Kaminoan
# 75         NaN  female   feminine        Shili    Togruta
# 80         NaN    male  masculine       Utapau     Pau'an
# 84         NaN    none  masculine          NaN      Droid
```

</td>
<td>

```R
# Filtering by multiple criteria within a single logical expression
filter(starwars, hair_color == "none" & eye_color == "black")
#> # A tibble: 9 x 14
#>   name  height  mass hair_color skin_color eye_color birth_year sex   gender
#>   <chr>  <int> <dbl> <chr>      <chr>      <chr>          <dbl> <chr> <chr>
#> 1 Nien…    160    68 none       grey       black             NA male  mascu…
#> 2 Gasg…    122    NA none       white, bl… black             NA male  mascu…
#> 3 Kit …    196    87 none       green      black             NA male  mascu…
#> 4 Plo …    188    80 none       orange     black             22 male  mascu…
#> 5 Lama…    229    88 none       grey       black             NA male  mascu…
#> 6 Taun…    213    NA none       grey       black             NA fema… femin…
#> 7 Shaa…    178    57 none       red, blue… black             NA fema… femin…
#> 8 Tion…    206    80 none       grey       black             NA male  mascu…
#> 9 BB8       NA    NA none       none       black             NA none  mascu…
#> # … with 5 more variables: homeworld <chr>, species <chr>, films <list>,
#> #   vehicles <list>, starships <list>
```

</td>
</tr>
<tr valign="top">
<td>

```python
starwars >> filter((X.hair_color == "none") | (X.eye_color == "black"))
#                  name  height   mass hair_color           skin_color  \
# 3         Darth Vader   202.0  136.0       none                white
# 14             Greedo   173.0   74.0        NaN                green
# 21              IG-88   200.0  140.0       none                metal
# 22              Bossk   190.0  113.0       none                green
# 24              Lobot   175.0   79.0       none                light
# 25             Ackbar   180.0   83.0       none         brown mottle
# 29          Nien Nunb   160.0   68.0       none                 grey
# 31        Nute Gunray   191.0   90.0       none        mottled green
# 33      Jar Jar Binks   196.0   66.0       none               orange
# 34       Roos Tarpals   224.0   82.0       none                 grey
# 35         Rugor Nass   206.0    NaN       none                green
# 38            Sebulba   112.0   40.0       none            grey, red
# 41         Darth Maul   175.0   80.0       none                  red
# 42        Bib Fortuna   180.0    NaN       none                 pale
# 43        Ayla Secura   178.0   55.0       none                 blue
# 44           Dud Bolt    94.0   45.0       none           blue, grey
# 45            Gasgano   122.0    NaN       none          white, blue
# 46     Ben Quadinaros   163.0   65.0       none  grey, green, yellow
# 47         Mace Windu   188.0   84.0       none                 dark
# 49          Kit Fisto   196.0   87.0       none                green
# 51         Adi Gallia   184.0   50.0       none                 dark
# 52        Saesee Tiin   188.0    NaN       none                 pale
# 53        Yarael Poof   264.0    NaN       none                white
# 54           Plo Koon   188.0   80.0       none               orange
# 55         Mas Amedda   196.0    NaN       none                 blue
# 59  Poggle the Lesser   183.0   80.0       none                green
# 67    Dexter Jettster   198.0  102.0       none                brown
# 68            Lama Su   229.0   88.0       none                 grey
# 69            Taun We   213.0    NaN       none                 grey
# 71      Ratts Tyerell    79.0   15.0       none           grey, blue
# 72             R4-P17    96.0    NaN       none          silver, red
# 73         Wat Tambor   193.0   48.0       none          green, grey
# 74           San Hill   191.0    NaN       none                 grey
# 75           Shaak Ti   178.0   57.0       none     red, blue, white
# 76           Grievous   216.0  159.0       none         brown, white
# 79          Sly Moore   178.0   48.0       none                 pale
# 80         Tion Medon   206.0   80.0       none                 grey
# 84                BB8     NaN    NaN       none                 none

#         eye_color  birth_year     sex     gender       homeworld       species
# 3          yellow        41.9    male  masculine        Tatooine         Human
# 14          black        44.0    male  masculine           Rodia        Rodian
# 21            red        15.0    none  masculine             NaN         Droid
# 22            red        53.0    male  masculine       Trandosha    Trandoshan
# 24           blue        37.0    male  masculine          Bespin         Human
# 25         orange        41.0    male  masculine        Mon Cala  Mon Calamari
# 29          black         NaN    male  masculine         Sullust     Sullustan
# 31            red         NaN    male  masculine  Cato Neimoidia     Neimodian
# 33         orange        52.0    male  masculine           Naboo        Gungan
# 34         orange         NaN    male  masculine           Naboo        Gungan
# 35         orange         NaN    male  masculine           Naboo        Gungan
# 38         orange         NaN    male  masculine       Malastare           Dug
# 41         yellow        54.0    male  masculine        Dathomir        Zabrak
# 42           pink         NaN    male  masculine          Ryloth       Twi'lek
# 43          hazel        48.0  female   feminine          Ryloth       Twi'lek
# 44         yellow         NaN    male  masculine         Vulpter    Vulptereen
# 45          black         NaN    male  masculine         Troiken         Xexto
# 46         orange         NaN    male  masculine            Tund         Toong
# 47          brown        72.0    male  masculine      Haruun Kal         Human
# 49          black         NaN    male  masculine     Glee Anselm      Nautolan
# 51           blue         NaN  female   feminine       Coruscant    Tholothian
# 52         orange         NaN    male  masculine         Iktotch      Iktotchi
# 53         yellow         NaN    male  masculine         Quermia      Quermian
# 54          black        22.0    male  masculine           Dorin       Kel Dor
# 55           blue         NaN    male  masculine        Champala      Chagrian
# 59         yellow         NaN    male  masculine        Geonosis     Geonosian
# 67         yellow         NaN    male  masculine            Ojom      Besalisk
# 68          black         NaN    male  masculine          Kamino      Kaminoan
# 69          black         NaN  female   feminine          Kamino      Kaminoan
# 71        unknown         NaN    male  masculine     Aleen Minor        Aleena
# 72      red, blue         NaN    none   feminine             NaN         Droid
# 73        unknown         NaN    male  masculine           Skako       Skakoan
# 74           gold         NaN    male  masculine      Muunilinst          Muun
# 75          black         NaN  female   feminine           Shili       Togruta
# 76  green, yellow         NaN    male  masculine           Kalee       Kaleesh
# 79          white         NaN     NaN        NaN          Umbara           NaN
# 80          black         NaN    male  masculine          Utapau        Pau'an
# 84          black         NaN    none  masculine             NaN         Droid
```

</td>
<td>

```R
filter(starwars, hair_color == "none" | eye_color == "black")
#> # A tibble: 38 x 14
#>    name  height  mass hair_color skin_color eye_color birth_year sex   gender
#>    <chr>  <int> <dbl> <chr>      <chr>      <chr>          <dbl> <chr> <chr>
#>  1 Dart…    202   136 none       white      yellow          41.9 male  mascu…
#>  2 Gree…    173    74 NA         green      black           44   male  mascu…
#>  3 IG-88    200   140 none       metal      red             15   none  mascu…
#>  4 Bossk    190   113 none       green      red             53   male  mascu…
#>  5 Lobot    175    79 none       light      blue            37   male  mascu…
#>  6 Ackb…    180    83 none       brown mot… orange          41   male  mascu…
#>  7 Nien…    160    68 none       grey       black           NA   male  mascu…
#>  8 Nute…    191    90 none       mottled g… red             NA   male  mascu…
#>  9 Jar …    196    66 none       orange     orange          52   male  mascu…
#> 10 Roos…    224    82 none       grey       orange          NA   male  mascu…
#> # … with 28 more rows, and 5 more variables: homeworld <chr>, species <chr>,
#> #   films <list>, vehicles <list>, starships <list>
```

</td>
</tr>
<tr valign="top">
<td>

```python
starwars >> filter(X.hair_color == "none", X.eye_color == "black")
#           name  height  mass hair_color        skin_color eye_color  \
# 29   Nien Nunb   160.0  68.0       none              grey     black
# 45     Gasgano   122.0   NaN       none       white, blue     black
# 49   Kit Fisto   196.0  87.0       none             green     black
# 54    Plo Koon   188.0  80.0       none            orange     black
# 68     Lama Su   229.0  88.0       none              grey     black
# 69     Taun We   213.0   NaN       none              grey     black
# 75    Shaak Ti   178.0  57.0       none  red, blue, white     black
# 80  Tion Medon   206.0  80.0       none              grey     black
# 84         BB8     NaN   NaN       none              none     black

#     birth_year     sex     gender    homeworld    species
# 29         NaN    male  masculine      Sullust  Sullustan
# 45         NaN    male  masculine      Troiken      Xexto
# 49         NaN    male  masculine  Glee Anselm   Nautolan
# 54        22.0    male  masculine        Dorin    Kel Dor
# 68         NaN    male  masculine       Kamino   Kaminoan
# 69         NaN  female   feminine       Kamino   Kaminoan
# 75         NaN  female   feminine        Shili    Togruta
# 80         NaN    male  masculine       Utapau     Pau'an
# 84         NaN    none  masculine          NaN      Droid
```

</td>
<td>

```R
# When multiple expressions are used, they are combined using &
filter(starwars, hair_color == "none", eye_color == "black")
#> # A tibble: 9 x 14
#>   name  height  mass hair_color skin_color eye_color birth_year sex   gender
#>   <chr>  <int> <dbl> <chr>      <chr>      <chr>          <dbl> <chr> <chr>
#> 1 Nien…    160    68 none       grey       black             NA male  mascu…
#> 2 Gasg…    122    NA none       white, bl… black             NA male  mascu…
#> 3 Kit …    196    87 none       green      black             NA male  mascu…
#> 4 Plo …    188    80 none       orange     black             22 male  mascu…
#> 5 Lama…    229    88 none       grey       black             NA male  mascu…
#> 6 Taun…    213    NA none       grey       black             NA fema… femin…
#> 7 Shaa…    178    57 none       red, blue… black             NA fema… femin…
#> 8 Tion…    206    80 none       grey       black             NA male  mascu…
#> 9 BB8       NA    NA none       none       black             NA none  mascu…
#> # … with 5 more variables: homeworld <chr>, species <chr>, films <list>,
#> #   vehicles <list>, starships <list>
```

</td>
</tr>
<tr valign="top">
<td>

```python
starwars >> filter(X.mass > mean(X.mass, na_rm=True))
#                      name  height    mass   hair_color        skin_color  \
# 3             Darth Vader   202.0   136.0         none             white
# 5               Owen Lars   178.0   120.0  brown, grey             light
# 12              Chewbacca   228.0   112.0        brown           unknown
# 15  Jabba Desilijic Tiure   175.0  1358.0          NaN  green-tan, brown
# 17       Jek Tono Porkins   180.0   110.0        brown              fair
# 21                  IG-88   200.0   140.0         none             metal
# 22                  Bossk   190.0   113.0         none             green
# 67        Dexter Jettster   198.0   102.0         none             brown
# 76               Grievous   216.0   159.0         none      brown, white
# 77                Tarfful   234.0   136.0        brown             brown

#         eye_color  birth_year             sex     gender   homeworld  \
# 3          yellow        41.9            male  masculine    Tatooine
# 5            blue        52.0            male  masculine    Tatooine
# 12           blue       200.0            male  masculine    Kashyyyk
# 15         orange       600.0  hermaphroditic  masculine   Nal Hutta
# 17           blue         NaN            male  masculine  Bestine IV
# 21            red        15.0            none  masculine         NaN
# 22            red        53.0            male  masculine   Trandosha
# 67         yellow         NaN            male  masculine        Ojom
# 76  green, yellow         NaN            male  masculine       Kalee
# 77           blue         NaN            male  masculine    Kashyyyk

#        species
# 3        Human
# 5        Human
# 12     Wookiee
# 15        Hutt
# 17       Human
# 21       Droid
# 22  Trandoshan
# 67    Besalisk
# 76     Kaleesh
# 77     Wookiee
```

</td>
<td>

```R
# The filtering operation may yield different results on grouped
# tibbles because the expressions are computed within groups.
#
# The following filters rows where `mass` is greater than the
# global average:
starwars %>% filter(mass > mean(mass, na.rm = TRUE))
#> # A tibble: 10 x 14
#>    name  height  mass hair_color skin_color eye_color birth_year sex   gender
#>    <chr>  <int> <dbl> <chr>      <chr>      <chr>          <dbl> <chr> <chr>
#>  1 Dart…    202   136 none       white      yellow          41.9 male  mascu…
#>  2 Owen…    178   120 brown, gr… light      blue            52   male  mascu…
#>  3 Chew…    228   112 brown      unknown    blue           200   male  mascu…
#>  4 Jabb…    175  1358 NA         green-tan… orange         600   herm… mascu…
#>  5 Jek …    180   110 brown      fair       blue            NA   male  mascu…
#>  6 IG-88    200   140 none       metal      red             15   none  mascu…
#>  7 Bossk    190   113 none       green      red             53   male  mascu…
#>  8 Dext…    198   102 none       brown      yellow          NA   male  mascu…
#>  9 Grie…    216   159 none       brown, wh… green, y…       NA   male  mascu…
#> 10 Tarf…    234   136 brown      brown      blue            NA   male  mascu…
#> # … with 5 more variables: homeworld <chr>, species <chr>, films <list>,
#> #   vehicles <list>, starships <list>
```

</td>
</tr>
<tr valign="top">
<td>

```python
starwars >> group_by(X.gender) >> filter(mean(X.mass, na_rm=True) < X.mass)
#                      name  height    mass   hair_color           skin_color  \
# 3             Darth Vader   202.0   136.0         none                white
# 5               Owen Lars   178.0   120.0  brown, grey                light
# 6      Beru Whitesun lars   165.0    75.0        brown                light
# 12              Chewbacca   228.0   112.0        brown              unknown
# 15  Jabba Desilijic Tiure   175.0  1358.0          NaN     green-tan, brown
# 17       Jek Tono Porkins   180.0   110.0        brown                 fair
# 21                  IG-88   200.0   140.0         none                metal
# 22                  Bossk   190.0   113.0         none                green
# 43            Ayla Secura   178.0    55.0         none                 blue
# 60        Luminara Unduli   170.0    56.2        black               yellow
# 66             Zam Wesell   168.0    55.0       blonde  fair, green, yellow
# 75               Shaak Ti   178.0    57.0         none     red, blue, white
# 76               Grievous   216.0   159.0         none         brown, white
# 77                Tarfful   234.0   136.0        brown                brown

#         eye_color  birth_year             sex     gender   homeworld  \
# 3          yellow        41.9            male  masculine    Tatooine
# 5            blue        52.0            male  masculine    Tatooine
# 6            blue        47.0          female   feminine    Tatooine
# 12           blue       200.0            male  masculine    Kashyyyk
# 15         orange       600.0  hermaphroditic  masculine   Nal Hutta
# 17           blue         NaN            male  masculine  Bestine IV
# 21            red        15.0            none  masculine         NaN
# 22            red        53.0            male  masculine   Trandosha
# 43          hazel        48.0          female   feminine      Ryloth
# 60           blue        58.0          female   feminine      Mirial
# 66         yellow         NaN          female   feminine       Zolan
# 75          black         NaN          female   feminine       Shili
# 76  green, yellow         NaN            male  masculine       Kalee
# 77           blue         NaN            male  masculine    Kashyyyk

#        species
# 3        Human
# 5        Human
# 6        Human
# 12     Wookiee
# 15        Hutt
# 17       Human
# 21       Droid
# 22  Trandoshan
# 43     Twi'lek
# 60    Mirialan
# 66    Clawdite
# 75     Togruta
# 76     Kaleesh
# 77     Wookiee
```

</td>
<td>

```R
# Whereas this keeps rows with `mass` greater than the gender
# average:
starwars %>% group_by(gender) %>% filter(mass > mean(mass, na.rm = TRUE))
#> # A tibble: 14 x 14
#> # Groups:   gender [2]
#>    name  height   mass hair_color skin_color eye_color birth_year sex   gender
#>    <chr>  <int>  <dbl> <chr>      <chr>      <chr>          <dbl> <chr> <chr>
#>  1 Dart…    202  136   none       white      yellow          41.9 male  mascu…
#>  2 Owen…    178  120   brown, gr… light      blue            52   male  mascu…
#>  3 Beru…    165   75   brown      light      blue            47   fema… femin…
#>  4 Chew…    228  112   brown      unknown    blue           200   male  mascu…
#>  5 Jabb…    175 1358   NA         green-tan… orange         600   herm… mascu…
#>  6 Jek …    180  110   brown      fair       blue            NA   male  mascu…
#>  7 IG-88    200  140   none       metal      red             15   none  mascu…
#>  8 Bossk    190  113   none       green      red             53   male  mascu…
#>  9 Ayla…    178   55   none       blue       hazel           48   fema… femin…
#> 10 Lumi…    170   56.2 black      yellow     blue            58   fema… femin…
#> 11 Zam …    168   55   blonde     fair, gre… yellow          NA   fema… femin…
#> 12 Shaa…    178   57   none       red, blue… black           NA   fema… femin…
#> 13 Grie…    216  159   none       brown, wh… green, y…       NA   male  mascu…
#> 14 Tarf…    234  136   brown      brown      blue            NA   male  mascu…
#> # … with 5 more variables: homeworld <chr>, species <chr>, films <list>,
#> #   vehicles <list>, starships <list>
```

</td>
</tr>
<tr valign="top">
<td>

```python
vars = ['mass', 'height']
cond = [80, 150]
starwars >> filter(
    X[vars[0]] > cond[0],
    X[vars[1]] > cond[1]
)
#                      name  height    mass   hair_color        skin_color  \
# 3             Darth Vader   202.0   136.0         none             white
# 5               Owen Lars   178.0   120.0  brown, grey             light
# 8       Biggs Darklighter   183.0    84.0        black             light
# 10       Anakin Skywalker   188.0    84.0        blond              fair
# 12              Chewbacca   228.0   112.0        brown           unknown
# 15  Jabba Desilijic Tiure   175.0  1358.0          NaN  green-tan, brown
# 17       Jek Tono Porkins   180.0   110.0        brown              fair
# 21                  IG-88   200.0   140.0         none             metal
# 22                  Bossk   190.0   113.0         none             green
# 25                 Ackbar   180.0    83.0         none      brown mottle
# 30           Qui-Gon Jinn   193.0    89.0        brown              fair
# 31            Nute Gunray   191.0    90.0         none     mottled green
# 34           Roos Tarpals   224.0    82.0         none              grey
# 47             Mace Windu   188.0    84.0         none              dark
# 48           Ki-Adi-Mundi   198.0    82.0        white              pale
# 49              Kit Fisto   196.0    87.0         none             green
# 56           Gregar Typho   185.0    85.0        black              dark
# 67        Dexter Jettster   198.0   102.0         none             brown
# 68                Lama Su   229.0    88.0         none              grey
# 76               Grievous   216.0   159.0         none      brown, white
# 77                Tarfful   234.0   136.0        brown             brown

#         eye_color  birth_year             sex     gender       homeworld  \
# 3          yellow        41.9            male  masculine        Tatooine
# 5            blue        52.0            male  masculine        Tatooine
# 8           brown        24.0            male  masculine        Tatooine
# 10           blue        41.9            male  masculine        Tatooine
# 12           blue       200.0            male  masculine        Kashyyyk
# 15         orange       600.0  hermaphroditic  masculine       Nal Hutta
# 17           blue         NaN            male  masculine      Bestine IV
# 21            red        15.0            none  masculine             NaN
# 22            red        53.0            male  masculine       Trandosha
# 25         orange        41.0            male  masculine        Mon Cala
# 30           blue        92.0            male  masculine             NaN
# 31            red         NaN            male  masculine  Cato Neimoidia
# 34         orange         NaN            male  masculine           Naboo
# 47          brown        72.0            male  masculine      Haruun Kal
# 48         yellow        92.0            male  masculine           Cerea
# 49          black         NaN            male  masculine     Glee Anselm
# 56          brown         NaN            male  masculine           Naboo
# 67         yellow         NaN            male  masculine            Ojom
# 68          black         NaN            male  masculine          Kamino
# 76  green, yellow         NaN            male  masculine           Kalee
# 77           blue         NaN            male  masculine        Kashyyyk

#          species
# 3          Human
# 5          Human
# 8          Human
# 10         Human
# 12       Wookiee
# 15          Hutt
# 17         Human
# 21         Droid
# 22    Trandoshan
# 25  Mon Calamari
# 30         Human
# 31     Neimodian
# 34        Gungan
# 47         Human
# 48        Cerean
# 49      Nautolan
# 56         Human
# 67      Besalisk
# 68      Kaminoan
# 76       Kaleesh
# 77       Wookiee
```

</td>
<td>

```R
# To refer to column names that are stored as strings, use the `.data` pronoun:
vars <- c("mass", "height")
cond <- c(80, 150)
starwars %>%
  filter(
    .data[[vars[[1]]]] > cond[[1]],
    .data[[vars[[2]]]] > cond[[2]]
  )
#> # A tibble: 21 x 14
#>    name  height  mass hair_color skin_color eye_color birth_year sex   gender
#>    <chr>  <int> <dbl> <chr>      <chr>      <chr>          <dbl> <chr> <chr>
#>  1 Dart…    202   136 none       white      yellow          41.9 male  mascu…
#>  2 Owen…    178   120 brown, gr… light      blue            52   male  mascu…
#>  3 Bigg…    183    84 black      light      brown           24   male  mascu…
#>  4 Anak…    188    84 blond      fair       blue            41.9 male  mascu…
#>  5 Chew…    228   112 brown      unknown    blue           200   male  mascu…
#>  6 Jabb…    175  1358 NA         green-tan… orange         600   herm… mascu…
#>  7 Jek …    180   110 brown      fair       blue            NA   male  mascu…
#>  8 IG-88    200   140 none       metal      red             15   none  mascu…
#>  9 Bossk    190   113 none       green      red             53   male  mascu…
#> 10 Ackb…    180    83 none       brown mot… orange          41   male  mascu…
#> # … with 11 more rows, and 5 more variables: homeworld <chr>, species <chr>,
#> #   films <list>, vehicles <list>, starships <list>
# Learn more in ?dplyr_data_masking
```

</td>
</tr>
</table>
