from .core.plugin import plugin as _plugin

_additional_imports = _plugin.hooks.other_api()
locals().update(_additional_imports)
