## Register your own verb

Verbs are registered by `register_verb()` from [`pipda`][1].

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

There are types of functions, with data argument (as the first argument) or without.

The functions with data argument, is just like a verb, but just don't support piping and they are supposed to be used inside verbs. To limit the function to be used inside a verb, use `verb_arg_only=True` with `register_func`.

!!! Note:

    In `dplyr`, you can call a verb regularly: `mutate(df, across(...))`, and `across` can still be recognized as a verb argument, but we cann't do it in python. When used in this situation, error will still be raised. Instead, use the piping syntax: `df >> mutate(across(...))`

For the functions without data argument, the `types` argument should be specified as `None`.

Both types of functions are supposed to be used inside a verb (as its arguments), but you can still use them somewhere else. That means you are calling it regularly. For caveats for that, please checkout [this section][2].

[1]: https://github.com/pwwang/pipda
[2]: https://github.com/pwwang/pipda#caveats
