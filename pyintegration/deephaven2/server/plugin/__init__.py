import jpy

_JCallbackAdapter = jpy.get_type('io.deephaven.server.plugin.python.CallbackAdapter')

def register_all_into(callback: _JCallbackAdapter):
    try:
        from .register import register_all_into
    except ModuleNotFoundError as e:
        # deephaven.plugin is an optional dependency, so if it can't be found, there are no Deephaven python plugins
        # to register
        if e.name == 'deephaven.plugin':
            return
        raise e
    register_all_into(callback)
