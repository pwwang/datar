from pathlib import Path

from pipda import Symbolic

f = Symbolic()

OPTION_FILE_HOME = Path("~/.datar.toml").expanduser()
OPTION_FILE_CWD = Path("./.datar.toml").resolve()
