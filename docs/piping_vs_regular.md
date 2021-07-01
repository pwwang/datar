
A verb can be called using a piping syntax:
```python
df >> verb(...)
```

Or in a regular way:
```python
verb(df, ...)
```

The piping is recommended and is designed specially to enable full features of `datar` with [`pipda`][1].

The regular form of calling a verb has no problems with simple arguments (arguments that don't involve any functions registered by `register_func()/register_verb()`). Functions registered by `register_func(None, ...)` that don't have data argument as the first argument are also perfect to work in this form.

However, there may be problems with verb calls as arguments of a verb, or a function call with data argument as arguments of a verb. In most cases, they are just fine, but there are ambiguous cases when the functions have optional arguments, and the second argument has the same type annotation as the first one. Because we cannot distinguish whether we should call it regularly or let it return a `Function` object to wait for the data to be piped in.

For example:

```python
@register_verb(int)
def add(a: int, b: int):
    return a + b

@register_func(int)
def incr(x: int, y: int = 3):
    return x + y

add(1, incr(2))
```

In such a case, we don't know whether `incr(2)` should be interpreted as `incr(2, y=3)` or `add(y=3)` waiting for `x` to be piped in.

The above code will still run and get a result of `6`, but a warning will be showing about the ambiguity.

To avoid this, use the piping syntax: `1 >> add(incr(2))`, resulting in `4`. Or if you are intended to do `incr(2, y=3)`, specify a value for `y`: `add(1, incr(2, 3))`, resulting in `6`, without a warning.

For more details, see also the [caveats][2] from `pipda`

[1]: https://github.com/pwwang/pipda
[2]: https://github.com/pwwang/pipda#caveats
