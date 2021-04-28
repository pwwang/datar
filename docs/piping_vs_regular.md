
A verb can be called in a piping form:
```python
df >> verb(...)
```

Or in a regular way:
```python
verb(df, ...)
```

The piping is recommended and is designed specially to enable full features of `datar`.

The regular form of verb calling is limited when an argument is calling a function that is registered requiring the data argument. For example:

```python
df >> head(n=10)
head(df, n=10) # same
```

However,
```python
df >> select(everything()) # works
select(df, everything()) # not working
```
Since `everything` is registered requiring the first argument to be a data frame. With the regular form, we are not able (or need too much effort) to obtain the data frame, but for the piping form, `pipda` is designed to pass the data piped to the verb and every argument of it.

The functions registered by `register_func` are supposed to be used as arguments of verbs. However, they have to be used with the right signature. For example, `everything` signature has `_data` as the first argument, to be called regularly:
```python
everything(df)
# everything() not working, everything of what?
```

When the functions are registered by `register_func(None, ...)`, which does not require the data argument, they are able to be used in regular form:

```python
from datar.core import f
from datar.base import abs
from datar.tibble import tibble
from datar.dplyr import mutate

df = tibble(x=[-1,-2,-3])
df >> mutate(y=abs(f.x))
#   x  y
# 0 -1 1
# 1 -2 2
# 2 -3 3

mutate(df, abs(f.x)) # works the same way
```
