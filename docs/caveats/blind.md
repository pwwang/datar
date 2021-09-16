
Related issues: [GH#45][1] [GH#54][2] [GH#55][3]

## Why?
To make `datar` in both regular calling and piping calling for verbs:

```python
# regular calling
num_rows = nrow(df)

# piping calling
num_rows = df >> nrow()
```

we need the source code available to detect the AST node, especially the piping sign (`ast.BinOp(op=ast.RShift)`), so we can preserve the slot of the first argument for the data to pipe in.

However, the source code is not always avaiable at runtime (i.e. raw python REPL, `exec()`), or the there could be some environment that modifies the AST tree (`assert` from `pytest`). We call those environments "blind".

A quick example to simulate this siutation:

```python
>>> from datar.all import *
>>> df = tibble(a="a")
>>> df >> mutate(A=f.a.str.upper())
         a        A
  <object> <object>
0        a        A
>>> source = "df >> mutate(A=f.a.str.upper())"
>>> exec(source)
/path/to/site-packages/pipda/utils.py:161: UserWarning: Failed to fet
ch the node calling the function, call it with the original function.
  warnings.warn(
Traceback (most recent call last):
  ...
```

## Solutions

- Try switching to a REPL that maintains the source code (`ipython` instead of raw python REPL, for example)
- Save the code into a file, and run that script with python interpreter
- Stick with the regular calling:

    ```python
    >>> source = "df2 = mutate(df, A=f.a.str.upper())"
    >>> exec(source)  # you still get a warning, but the code works
    /home/pwwang/miniconda3/lib/python3.9/site-packages/pipda/utils.py:161: UserWarning: Failed to fet
    ch the node calling the function, call it with the original function.
    warnings.warn(
    >>> df2
            a        A
    <object> <object>
    0        a        A
    ```

- Stick with the piping calling:

    ```python
    >>> from pipda import options
    >>> options.assume_all_piping = True
    >>> source = "df2 = df >> mutate(A=f.a.str.upper())"
    >>> exec(source)  # no warnings, we know we don't need the AST node anymore
    >>> df2
            a        A
    <object> <object>
    0        a        A
    ```

    !!! Note

        Whichever calling mode you are sticking with, you have to stick with it for all verbs, even for those simple ones (i.e. `dim()`, `nrow()`, etc)

    !!! Tip

        If you wonder whether a python function is registered as a verb or a plain function:

        ```python
        >>> mutate.__pipda__
        'Verb'
        >>> nrow.__pipda__
        'Verb'
        >>> as_integer.__pipda__
        'PlainFunction'
        ```



[1]: https://github.com/pwwang/datar/issues/45
[2]: https://github.com/pwwang/datar/issues/54
[3]: https://github.com/pwwang/datar/issues/55
