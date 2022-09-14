## Index base

By default, `datar` follows `python`'s 0-based indexing.

## Selection

- `DataFrame` indexes

For most `dplyr` verbs, index of a frame is kept. However, the frame is grouped,
the index is usually dropped.


## Negative indexes

In `R`, negative indexes mean removal. However, here negative indexes are still
selection, as `-1` for the last column, `-2` for the second last, etc.

If you want to do negative selection, use tilde `~` instead of `-`.

See more at `select()`'s doc.
