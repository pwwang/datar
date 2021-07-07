"""Let datar warn when builtin names are tried to be imported"""
import sys
from typing import Callable, Any
from executing import Source

WARNED = set()


def warn_builtin_names(**names: Callable) -> Callable[[str], Any]:
    """Generate __getattr__ function to warn the builtin names"""
    from .utils import logger, get_option

    # Enables tempoarory warn on or off
    warn = True

    def _getattr(name: str):
        """A route to let us check if the function is imported by
            >>> from module import func
        But not
            >>> import module
            >>> module.func

        If func is a python built-in function, then it gets overriden by datar.

        The side effects:
        1. This executes every time when `module.func` is called
        2. When the source is not avaiable for `module.func`, it will be a
           false alarm treat this as `from module import func`
        3. It warns even you do `from module import func as alias`

        Instead, you can do `from module import func_` to access function `func`

        Args:
            **names: The name-module pairs

        Returns:
            A function that can be used as `__getattr__` for a module
        """
        nonlocal warn
        if name == "_warn":
            warn = True
            return None
        if name == "_no_warn":
            warn = False
            return None

        if name == "__path__" or name not in names:
            raise AttributeError

        if warn and name not in WARNED and get_option("warn.builtin.names"):
            node = Source.executing(sys._getframe(1)).node
            if not node:
                WARNED.add(name)
                logger.warning(
                    'Builtin name "%s" has been overriden by datar.', name
                )
        return names[name]

    return _getattr
