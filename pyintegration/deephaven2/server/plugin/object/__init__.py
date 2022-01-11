import jpy
from deephaven.plugin.object import Exporter, ObjectType, Reference

_JExporter = jpy.get_type('io.deephaven.plugin.type.ObjectType$Exporter')

class ExporterAdapter(Exporter):
    def __init__(self, exporter: _JExporter):
        self._exporter = exporter

    def new_server_side_reference(self, object) -> Reference:
        # todo: unwrap_to_java_type(object)?
        _reference = self._exporter.newServerSideReference(object)
        return Reference(bytes(_reference.id().toByteArray()))

    def __str__(self):
        return str(self._exporter)

# see io.deephaven.server.plugin.python.ObjectTypeAdapter for calling details
class ObjectTypeAdapter:
    def __init__(self, user_object_type: ObjectType):
        self._user_object_type = user_object_type

    def is_type(self, object):
        return self._user_object_type.is_type(object)

    def to_bytes(self, exporter: _JExporter, object):
        return self._user_object_type.to_bytes(ExporterAdapter(exporter), object)

    def __str__(self):
        return str(self._user_object_type)
