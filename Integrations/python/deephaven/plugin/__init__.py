DEEPHAVEN_PLUGIN_ENTRY_KEY = 'deephaven.plugin'
DEEPHAVEN_PLUGIN_REGISTER_NAME = 'register_into'

def get_plugin_entrypoints(name):
    import sys
    if sys.version_info < (3, 8):
        # TODO(deephaven-base-images#6): Add importlib-metadata backport install for future server plugin support
        # We can remove the exception handling once above gets merged in
        try:
            from importlib_metadata import entry_points
        except ImportError:
            return []
    else:
        from importlib.metadata import entry_points
    return entry_points(group=DEEPHAVEN_PLUGIN_ENTRY_KEY, name=name) or []

def all_plugins_register_into(callback):
    for entrypoint in get_plugin_entrypoints(DEEPHAVEN_PLUGIN_REGISTER_NAME):
        plugin_register_into = entrypoint.load()
        plugin_register_into(callback)
