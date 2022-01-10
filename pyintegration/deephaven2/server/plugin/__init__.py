import jpy

RegistrationCallback = jpy.get_type('io.deephaven.plugin.Registration$Callback')
Exporter = jpy.get_type('io.deephaven.plugin.type.ObjectType$Exporter')

DEEPHAVEN_PLUGIN_ENTRY_KEY = 'deephaven.plugin'
DEEPHAVEN_PLUGIN_REGISTER_NAME = 'register_into'

def get_plugin_entrypoints(name: str):
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

def all_plugins_register_into(callback: RegistrationCallback):
    callback_adapter = CallbackAdapter(callback)
    for entrypoint in get_plugin_entrypoints(DEEPHAVEN_PLUGIN_REGISTER_NAME):
        plugin_register_into = entrypoint.load()
        plugin_register_into(callback_adapter)

# TODO(deephaven-core#1791): CallbackAdapter implements CallbackABC
class CallbackAdapter:
    def __init__(self, callback: RegistrationCallback):
        self._callback = callback

    # TODO(deephaven-core#1791): type hint object_type as ObjectTypeABC
    def register_object_type(self, object_type):
        self._callback.registerObjectType(object_type.name, ObjectTypeAdapter(object_type))

    def __str__(self):
        return str(self._callback)

# TODO(deephaven-core#1791): ExporterAdapter implements ExporterABC
class ExporterAdapter:
    def __init__(self, exporter: Exporter):
        self._exporter = exporter

    def new_server_side_reference(self, object):
        # TODO(deephaven-core#1791): define and use ReferenceABC
        raise NotImplementedError

    def __str__(self):
        return str(self._exporter)

class ObjectTypeAdapter:
    # TODO(deephaven-core#1791): type hint user_object_type as ObjectTypeABC
    def __init__(self, user_object_type):
        self._user_object_type = user_object_type

    def is_type(self, object):
        return self._user_object_type.is_type(object)

    def to_bytes(self, exporter: Exporter, object):
        return self._user_object_type.to_bytes(ExporterAdapter(exporter), object)

    def __str__(self):
        return str(self._user_object_type)
