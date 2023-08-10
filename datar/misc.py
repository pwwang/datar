from .core.load_plugins import plugin as _plugin

locals().update(_plugin.hooks.misc_api())
