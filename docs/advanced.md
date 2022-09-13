## Register your own verb

Verbs are registered by `register_verb()` from [`pipda`][1] package.

The full signature of `register_verb()` is as follows:

```python
register_verb(
    types: Union[function, Type, Iterable[Type]] = <class 'object'>,
    context: Union[pipda.context.Context, pipda.context.ContextBase] = None,
    func: Union[function, NoneType] = None,
    extra_contexts: Union[Mapping[str, Union[pipda.context.Context, pipda.context.ContextBase]], NoneType] = None,
    **attrs: Any
) -> Callable
    """
    Register a verb with specific types of data

    If `func` is not given (works like `register_verb(types, context=...)`),
    it returns a function, works as a decorator.

    For example
        >>> @register_verb(DataFrame, context=Context.EVAL)
        >>> def verb(data, ...):
        >>>     ...

    When function is passed as a non-keyword argument, other arguments are as
    defaults
        >>> @register_verb
        >>> def verb(data, ...):
        >>>     ...

    In such a case, it is like a generic function to work with all types of
    data.

    Args:
        types: The classes of data for the verb
            Multiple classes are supported to be passed as a list/tuple/set.
        context: The context to evaluate the Expression objects
        func: The function to be decorated if passed explicitly
        extra_contexts: Extra contexts (if not the same as `context`)
            for specific arguments
        **attrs: Other attributes to be attached to the function

    Returns:
        A decorator function if `func` is not given or a wrapper function
        like a singledispatch generic function that can register other types,
        show all registry and dispatch for a specific type
    """
```

Note that when define a verb, a data argument as the first argument is requried.

## Register your own function

### Register contextual functions

These functions, registered by `context_func_factory`, are usually context-dependent, meaning it needs the data to calculate. For example, `row_number()` to get the row numbers of a frame. It does not require any arguments, however, when you register it, a data argument should be declared and it's used in side the function.

You can also register specific types for a contextual function, like a verb. And `context_func_factory` accepts same arguments as `register_verb` does.

Those functions are just like verbs, but just don't support piping and they are supposed to be used inside verbs. To limit the function to be used inside a verb, use `verb_arg_only=True` with `context_func_factory`.

### Register non-contextual functions

This type of functions should be registered by `func_factory` and don't require data argument, the `types` argument should be specified as `None`.

Both types of functions are supposed to be used inside a verb (as its arguments), but you can still use them somewhere else. That means you are calling it regularly. For caveats for that, please checkout [this section][2].

`func_factory()` accepts quite a different set of arguments:

```
def func_factory(
    kind,  # transform, agg/aggregate, apply
    data_args,
    name=None,
    qualname=None,
    doc=None,
    apply_type=None,
    context=Context.EVAL,
    signature=None,
    keep_series=False,
    func=None,
):
```

- `kind`: what kind of operation we should do on higher-order data structure (for example, `DataFrame` compared to `Series`) using the base function we registered.
- `data_args`: The arguments of the base function to be vectorized and recycled. The values of those arguments will compose a frame, which can be accessed by `__args_frame` argument in your base function. Since those values become `Series` objects, the raw values can be accessed by `__args_raw`.
- `name`, `qualname` and `doc`: The `__name__`, `__qualname__` and `__doc__` for the final returned function
- `apply_type`: The result type when we do apply on frames, only for kind `apply`. See:
  - https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.apply.html
- `context`: The context to evaluate the function
- `signature`: The function signature to detect data arguments from the function arguments. Default is `inspect.signature(func)`. But inspect cannot detect signature of some functions, for example, `numpy.sqrt()`, then you can pass a signature instead.
- `keep_series`: Whether try to keep the result as series if input is not series.
- `func`: The base function. The default data type to handle is `Series`. When hight-order data is encountered, for example, `SeriesGroupBy`, with `kind` `agg`, `sgb.agg(func)` will run for it.


You may also register a different base function for different higher-order data types. For example:
```python
@func_factory("agg", "x")
def my_sum(x):
    return x.sum()

# run sgb.agg("sum") directly on SeriesGroupBy object
# will be faster than sgb.agg(my_sum)
my_sum.register(SeriesGroupBy, "sum")
```

There are two hooks for you to modify the arguments (`pre`) and results (`post`)
For example:
```python
# make x integers before sum
my_sum.register(SeriesGroupBy, func=None, pre=lambda x: (x.astype(int), (), {}))
# or (you can specify pre and post at the same time)
my_sum.register(SeriesGroupBy, func=None, post=lambda out, x: out.astype(float))
# get the results back to float
```


You can handle the whole `SeriesGroupBy` object, instead of run the `agg`:
```python
@my_sum.register(SeriesGroupBy, meta=False)
def _(x: SeriesGroupBy):
    # Do whatever with the object
    ...
```


[1]: https://github.com/pwwang/pipda
[2]: https://github.com/pwwang/pipda#caveats
