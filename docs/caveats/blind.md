
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
/path/to/pipda/pipda/utils.py:54: VerbCallingCheckWarning: Failed to detect AST node calling `mutate`, assuming a normal call.
  warnings.warn(
Traceback (most recent call last):
  ...
```

## Solutions

- Try switching to a REPL that maintains the source code (`ipython`/`bpython` instead of raw python REPL, for example)
- Save the code into a file, and run that script with python interpreter
- Stick with the regular calling:

    ```python
    >>> df = tibble("a")
    >>> source = "df2 = mutate(df, A=f.a.str.upper())"
    >>> exec(source)  # you still get a warning, but the code works
    pipda/utils.py:163: UserWarning: Failed to fetch the node calling the
    function, call it with the original function.
    warnings.warn(
    >>> df2
             a        A
      <object> <object>
    0        a        A
    ```

- Change the fallback of a verb:

    ```python
    >>> from datar.all import *
    >>> df = tibble(a="a")
    >>> mutate.ast_fallback = "piping"
    >>> source = "df2 = df >> mutate(A=f.a.str.upper())"
    >>> exec(source)
    >>> df2
             a        A
      <object> <object>
    0        a        A
    ```

  Avaiable fallbacks:

  - `piping`: fallback to piping mode if AST node not avaiable
  - `normal`: fallback to normal mode if AST node not avaiable
  - `piping_warning`: fallback to piping mode if AST node not avaiable and given a warning
  - `normal_warning` (default): fallback to normal mode if AST node not avaiable and given a warning
  - `raise`: Raise an error

  See also: [https://pwwang.github.io/pipda/verbs/#fallbacks-when-ast-node-detection-fails](https://pwwang.github.io/pipda/verbs/#fallbacks-when-ast-node-detection-fails)

- Change the fallback temporarily

  ```python
  >>> from datar.all import *
  >>> df = tibble(a="a")
  >>> source = "df2 = df >> mutate(A=f.a.str.upper(), __ast_fallback='piping')"
  >>> exec(source)
  >>> df2
            a        A
    <object> <object>
  0        a        A
  ```


[1]: https://github.com/pwwang/datar/issues/45
[2]: https://github.com/pwwang/datar/issues/54
[3]: https://github.com/pwwang/datar/issues/55
