
`datar` introduced `tibble` package as well.

However, unlike in R, `tidyverse`'s `tibble` is a different class than the `data.frame` from base R, the data frame created by `datar.tibble.tibble()` and family is actually a pandas `DataFrame`. It's just a wrapper around the constructor.

So you can do anything you do using pandas API after creation.
