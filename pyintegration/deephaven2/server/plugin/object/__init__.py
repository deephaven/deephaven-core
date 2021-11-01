import jpy
from deephaven.plugin.object import Exporter, ObjectType, Reference

_JExporterAdapter = jpy.get_type('io.deephaven.server.plugin.python.ExporterAdapter')

class ExporterAdapter(Exporter):
    def __init__(self, exporter: _JExporterAdapter):
        self._exporter = exporter

    def new_server_side_reference(self, object) -> Reference:
        if isinstance(object, jpy.JType):
            _reference = self._exporter.newServerSideReference(object)
        else:
            _reference = self._exporter.newServerSideReferencePyObject(object)
        return Reference(bytes(_reference.id().toByteArray()))

    def __str__(self):
        return str(self._exporter)

# see io.deephaven.server.plugin.python.ObjectTypeAdapter for calling details
class ObjectTypeAdapter:
    def __init__(self, user_object_type: ObjectType):
        self._user_object_type = user_object_type

    def is_type(self, object):
        return self._user_object_type.is_type(object)

    def to_bytes(self, exporter: _JExporterAdapter, object):
        return self._user_object_type.to_bytes(ExporterAdapter(exporter), object)

    def __str__(self):
        return str(self._user_object_type)
