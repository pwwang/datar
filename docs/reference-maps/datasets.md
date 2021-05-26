<style>
.md-typeset__table {
   min-width: 100%;
}

.md-typeset table:not([class]) {
    display: table;
    max-width: 80%;
}
</style>

## Reference of `datar.datasets`

|API|Description|Source|
|---|---|---:|
|`airlines`|translation between two letter carrier codes and names|[`r-nycflights13`][1]|
|`airports`|airport names and locations|[`r-nycflights13`][1]|
|`flights`|all flights that departed from NYC in 2013|[`r-nycflights13`][1]|
|`weather`|hourly meterological data for each airport|[`r-nycflights13`][1]|
|`planes`|construction information about each plane|[`r-nycflights13`][1]|
|#|#|#|
|`state_abb`|character vector of 2-letter abbreviations for the state names.|[`r-datasets-state`][15]|
|`state_division`|factor giving state divisions (New England, Middle Atlantic, South Atlantic, East South Central, West South Central, East North Central, West North Central, Mountain, and Pacific).|[`r-datasets-state`][15]|
|`state_region`|factor giving the region (Northeast, South, North Central, West) that each state belongs to.|[`r-datasets-state`][15]|
|#|#|#|
|`airquality`|Daily air quality measurements in New York, May to September 1973.|[`r-datasets-airquality`][2]|
|`anscombe`|Four x-y datasets which have the same traditional statistical properties|[`r-datasets-anscombe`][3]|
|`iris`|Edgar Anderson's Iris Data|[`r-datasets-iris`][9]|
|`mtcars`|Motor Trend Car Road Tests|[`r-datasets-mtcars`][10]|
|`warpbreaks`|The Number of Breaks in Yarn during Weaving|[`r-datasets-warpbreaks`][19]|
|#|#|#|
|`band_instruments`|Band members of the Beatles and Rolling Stones|[`r-dplyr-band_members`][4]|
|`band_instruments2`|Band members of the Beatles and Rolling Stones|[`r-dplyr-band_members`][4]|
|`band_members`|Band members of the Beatles and Rolling Stones|[`r-dplyr-band_members`][4]|
|#|#|#|
|`table1`|Example tabular representations|[`r-dplyr-storms`][17]|
|`table2`|Example tabular representations|[`r-dplyr-storms`][17]|
|`table3`|Example tabular representations|[`r-dplyr-storms`][17]|
|`table4a`|Example tabular representations|[`r-dplyr-storms`][17]|
|`table4b`|Example tabular representations|[`r-dplyr-storms`][17]|
|`table5`|Example tabular representations|[`r-dplyr-storms`][17]|
|#|#|#|
|`starwars`|Starwars characters (columns `films`, `vehicles` and `starships` are not included)|[`r-dplyr-starwars`][14]|
|`storms`|This data is a subset of the NOAA Atlantic hurricane database best track data|[`r-dplyr-storms`][16]|
|`us_rent_income`|US rent and income data|[`r-dplyr-us_rent_income`][18]|
|`world_bank_pop`|Population data from the world bank|[`r-dplyr-world_bank_pop`][20]|
|#|#|#|
|`billboard`|Song rankings for Billboard top 100 in the year 2000|[`r-tidyr-billboard`][5]|
|`construction`|Completed construction in the US in 2018|[`r-tidyr-construction`][6]|
|`fish_encounters`|Information about fish swimming down a river|[`r-tidyr-fish_encounters`][8]|
|`population`|A subset of data from the World Health Organization Global Tuberculosis Report, and accompanying global populations.|[`r-tidyr-who`][11]|
|`relig_income`|Pew religion and income survey|[`r-tidyr-relig_income`][12]|
|`smiths`|A small demo dataset describing John and Mary Smith.|[`r-tidyr-smiths`][13]|
|`who`|A subset of data from the World Health Organization Global Tuberculosis Report, and accompanying global populations.|[`r-tidyr-who`][11]|
|#|#|#|
|`diamonds`|A dataset containing the prices and other attributes of almost 54,000 diamonds|[`r-ggplot2-diamonds`][7]|

[1]: https://github.com/tidyverse/nycflights13
[2]: https://www.rdocumentation.org/packages/datasets/versions/3.6.2/topics/airquality
[3]: https://www.rdocumentation.org/packages/datasets/versions/3.6.2/topics/anscombe
[4]: https://dplyr.tidyverse.org/reference/band_members.html
[5]: https://tidyr.tidyverse.org/reference/billboard.html
[6]: https://tidyr.tidyverse.org/reference/construction.html
[7]: https://ggplot2.tidyverse.org/reference/diamonds.html
[8]: https://tidyr.tidyverse.org/reference/fish_encounters.html
[9]: https://www.rdocumentation.org/packages/datasets/versions/3.6.2/topics/iris
[10]: https://www.rdocumentation.org/packages/datasets/versions/3.6.2/topics/mtcars
[11]: https://tidyr.tidyverse.org/reference/who.html
[12]: https://tidyr.tidyverse.org/reference/relig_income.html
[13]: https://tidyr.tidyverse.org/reference/smiths.html
[14]: https://dplyr.tidyverse.org/reference/starwars.html
[15]: https://www.rdocumentation.org/packages/datasets/versions/3.6.2/topics/state
[16]: https://dplyr.tidyverse.org/reference/storms.html
[17]: https://tidyr.tidyverse.org/reference/table1.html
[18]: https://tidyr.tidyverse.org/reference/us_rent_income.html
[19]: https://www.rdocumentation.org/packages/datasets/versions/3.6.2/topics/warpbreaks
[20]: https://tidyr.tidyverse.org/reference/world_bank_pop.html
