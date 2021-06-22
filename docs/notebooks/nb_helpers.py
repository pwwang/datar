"""helpers for notebooks"""
from contextlib import contextmanager

from IPython.display import display, Markdown, HTML
from IPython.core.interactiveshell import InteractiveShell
import pardoc
from varname.helpers import debug

InteractiveShell.ast_node_interactivity = "all"

BINDER_URL = (
    'https://mybinder.org/v2/gh/pwwang/datar/'
    '93d069f3ca36711fc811c61dcf60e9fc3d1460a5?filepath=docs%2Fnotebooks%2F'
)

def nb_header(*funcs, book=None):
    """Print the header of a notebooks, mostly the docs"""
    if book is None:
        book = funcs[0].__name__
    display(HTML(
        '<div style="text-align: right; text-style: italic">Try this notebook '
        f'on <a target="_blank" href="{BINDER_URL}{book}.ipynb">'
        'binder</a>.</div>'
    ))

    for func in funcs:
        formatted = pardoc.google_parser.format(
            func.__doc__,
            to='markdown',
            heading=5,
            indent_base='&emsp;&emsp;'
        )
        display(Markdown(f'{"#"*3} # {func.__name__}  '))
        display(Markdown(formatted))

@contextmanager
def try_catch():
    """Catch the error and print it out"""
    try:
        yield
    except Exception as exc:
        print(f"[{type(exc).__name__}] {exc}")
