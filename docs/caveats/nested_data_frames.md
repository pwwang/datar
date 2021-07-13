
pandas DataFrame doesn't support nested data frames. However, some R packages do, especially `tidyr`.

Here we uses fake nested data frames:

```python
>>> df = tibble(x=1, y=tibble(a=2, b=3))
>>> df
        x     y$a     y$b
  <int64> <int64> <int64>
0       1       2       3
```

Now `df` is a fake nested data frame, with an inner data frame as column `y` in `df`.

!!! Warning

    For APIs from `tidyr` that tidies nested data frames, this is fully supported, but just pay attention when you operate it in pandas way. For other APIs, this feature is still experimental.
