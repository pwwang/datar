"""helpers for notebooks"""
from contextlib import contextmanager

from IPython.display import display, Markdown, HTML
from IPython.core.interactiveshell import InteractiveShell
import pardoc
from varname.helpers import debug  # noqa
from datar import options

options(allow_conflict_names=True)

InteractiveShell.ast_node_interactivity = "all"

BINDER_URL = (
    "https://mybinder.org/v2/gh/pwwang/datar/"
    "dev?filepath=docs%2Fnotebooks%2F"
)


def nb_header(*funcs, book=None):
    """Print the header of a notebooks, mostly the docs"""
    if book is None:
        book = funcs[0].__name__
    display(
        HTML(
            '<div style="text-align: right; text-style: italic">'
            'Try this notebook on '
            f'<a target="_blank" href="{BINDER_URL}{book}.ipynb">'
            "binder</a>.</div>"
        )
    )

    for func in funcs:
        try:
            parsed = pardoc.google_parser.parse(func.__doc__)
            try:
                del parsed["Examples"]
            except KeyError:
                pass
        except Exception:
            formatted = func.__doc__
        else:
            formatted = pardoc.google_parser.format(
                parsed,
                to="markdown",
                heading=5,
                indent_base="&emsp;&emsp;",
            )

        display(Markdown(
            f'{"#"*3} '
            '<div style="background-color: #EEE; padding: 5px 0 8px 0">'
            f'★ {func.__name__}'
            '</div>')
        )
        display(Markdown(formatted))


@contextmanager
def try_catch():
    """Catch the error and print it out"""
    try:
        yield
    except Exception as exc:
        print(f"[{type(exc).__name__}] {exc}")
