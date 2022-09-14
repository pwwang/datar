## Register your own verb

Verbs are registered by `register_verb()` from [`pipda`][1] package.

The full signature of `register_verb` is as follows:

```python
def register_verb(
    types: Type | Sequence[Type],
    *,
    context: ContextType = None,
    extra_contexts: Mapping[str, ContextType] = None,
    dep: bool = False,
    ast_fallback: str = "normal_warning",
    func: Callable = None,
) -> Callable[[Callable], Verb] | Verb:
    """Register a verb

    Args:
        types: The types of the data allowed to pipe in
        context: The context to evaluate the arguments
        extra_contexts: Extra contexts to evaluate the keyword arguments
        dep: Whether the verb is dependent.
            >>> @register_func([1, 2], context=Context.EVAL, dep=True)
            >>> def length(data):
            >>>     return len(data)
            >>> # with dep=True
            >>> # length()  -> VerbCall
            >>> # with dep=False
            >>> # length()  -> TypeError, argument data is missing
        ast_fallback: What's the supposed way to call the verb when
            AST node detection fails.
            piping - Suppose this verb is called like `data >> verb(...)`
            normal - Suppose this verb is called like `verb(data, ...)`
            piping_warning - Suppose piping call, but show a warning
            normal_warning - Suppose normal call, but show a warning
            raise - Raise an error
        func: The function works as a verb.
    """
```

!!! note

    When defining a verb, it requires the first argument of the `Verb` a positional argument as data argument.


## Register your own function

A function should work as a value passed to a `Verb` to defer the execution of the verb call. It should also work regularly no expresssions passed as arguments.

For example:

```python
>>> from datar import f
>>> from datar.base import c, sum
>>> from datar.dplyr import summarise, group_by
>>> from datar.tibble import tibble
>>> df = tibble(x=c[1:5], g=[1, 1, 2, 2]) >> group_by(f.g)
>>> df >> summarise(sum=sum(f.x))
        g     sum
  <int64> <int64>
0       1       3
1       2       7
# sum should also work normally
>>> sum([3, 7])  # 10
```

### Register a non-dispatching function

We can use `register_func()` from `pipda` to register a function that handles all
data types by the function itself.

See [https://pwwang.github.io/pipda/functions/#registering-functions](https://pwwang.github.io/pipda/functions/#registering-functions) for its usage.

```python
import builtins
from pipda import register_func

@register_func
def sum(x):
    # works for list, np.ndarray, Series
    return builtins.sum(x)
```

### Register a dispatching function

However, sometimes, we want our functions to handle different types of data using
one focal function. For example, we want `sum` to not only work on `Series` data, but also `SeriesGroupBy` data. If `sum` works on `Series` data, then we expect it to work like `<SeriesGroupBy>.agg(sum)`. That is, to `sum` the `Series` data in each group.

In such a case, we just need to register that focal function `sum`:

```python
import builtins
from datar.core.factory import func_factory

@func_factory(kind="agg")
def sum(x):
    return builtins.sum(x)
```

Let's see if it works:

```python
from pandas import Series

s = Series([1, 2, 3, 4])
sum(s)  # 10
g = s.groupby([1, 1, 2, 2])
sum(g)
# 1    3
# 2    7
# Name: x, dtype: int64
```

It works! We don't need to handle any "groupedness" inside the function.

#### How it works

![func_factory](./func_factory.png)

Arguments of `func_factory`:

- `data_args`: The arguments of the base function to be vectorized and
    recycled.
    The values of those arguments will compose a frame,
    which can be accessed by `__args_frame` argument in your
    base function. Since those values become `Series` objects,
    the raw values can be accessed by `__args_raw`.
    If not specified, the first argument is used
- `kind`: what kind of operation we should do on higher-order data structure
    (for example, `DataFrame` compared to `Series`)
    using the base function we registered.
- `name`: The name of the function if `func.__name__` is unavailable
- `qualname`: The qualname of the function `if func.__qualname__` is
    unavailable
- `module`: The module of the function if `func.__module__` is unavailable
- `doc`: The doc of the function if `func.__doc__` is unavailable
- `signature`: The signature of the function if it is unavailable
- `context`: The context to evaluate the function
- `extra_contexts`: The extra contexts used on keyword-only arguments
- `func`: Default focal function to use in agg/transform/apply

#### Chose an operator kind: `agg`, `transform`, `apply` or `apply_df`?

See also: https://pandas.pydata.org/docs/user_guide/groupby.html

> **agg/aggregate**: compute a summary statistic (or statistics) for each group. Some examples:
> - Compute group sums or means.
> - Compute group sizes / counts.

> **transform**: perform some group-specific computations and return a like-indexed object. Some examples:
> - Standardize data (zscore) within a group.
> - Filling NAs within groups with a value derived from each group.

> **apply**: Some combination of the above: GroupBy will examine the results of the apply step and try to return a sensibly combined result if it doesn’t fit into either of the above two categories. This applies on a single argument.

> **apply_df**: Applies `__args_frame` that is composed of multiple arguments (specified by `data_args`). When running `__args_frame.apply(wrapped_func, ...)`, the arguments specified by `data_args` are replaced by the `Series` of `__args_frame` in each group.

#### Register focal functions for different types

One could register a focal function for a specific type by:

```python
@func_factory()
def func(...):
    ...

@func.register(...)
def _(...):
    ...
```

Arguments of `func.register()`:

- `cls`: The cls to be registered (could be a list of types)
- `pre`: The pre function to prepare for agg/transform/apply
    It takes the arguments passed to the function and returns None
    or (x, args, kwargs) to replace the original
    If `"default"` use the default one
    If `"decor"` use the decorated function
    for `apply_df`, it takes `**arguments` and returns the updated `arguments`
- `post`: The  post function to wrap up results
    It takes `__out, *args, **kwargs` and returns the output.
    If `"default"` use the default one
    If `"decor"` use the decorated function
    for `apply_df`, it takes `__out, **arguments` and returns the new output.
- `args_raw`: Whether pass the raw values as a dict to `__args_raw`
    True to add it to bound arguments; False to remove it from
    bound arguments; None to keep it intact; "default" to use
    the default value
- `args_frame`: Whether pass the argument data frame (compiled by
    self.data_args) to `__args_frame`.
    True to add it to bound arguments; False to remove it from
    bound arguments; None to keep it intact; "default" to use
    the default value
- `op_args`: The `(axis, raw, result_type)` alike inside
    `DataFrame.apply(func, axis, raw, result_type, args, **kwargs)`
- `keep_args`: Whether to keep the `args` as a whole to pass to the
    operator or decouple them by `*args`
- `op_kwargs`: The keyword argument, will be passed to operation
    by `**op_kwargs`
- `keep_series`: Whether to keep Series if input is not a PandasObject.
    Only for transform
- `func`: The focal function used in agg/transform/apply
    If func is "default"`, initial registered function is used.

#### Register functions to handle all by yourself

```python
@func.register_dispatchee(SeriesGroupBy)
def sum(x):
    // pre-hook
    out = x.agg("sum")
    // post-hook
    return out
```

This way, it doesn't matter which kind of opertion you choose.



[1]: https://github.com/pwwang/pipda
[2]: https://github.com/pwwang/pipda#caveats
