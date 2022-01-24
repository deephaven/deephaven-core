import jpy

_JCallbackAdapter = jpy.get_type('io.deephaven.server.plugin.python.CallbackAdapter')

def initialize_all_and_register_into(callback: _JCallbackAdapter):
    try:
        from . import register
    except ModuleNotFoundError as e:
        # deephaven.plugin is an optional dependency, so if it can't be found, there are no Deephaven python plugins
        # to register
        if e.name == 'deephaven.plugin':
            return
        raise e
    register.initialize_all_and_register_into(callback)
