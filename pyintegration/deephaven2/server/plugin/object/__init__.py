import jpy
from deephaven.plugin.object import Exporter, ObjectType, Reference

_JReference = jpy.get_type('io.deephaven.plugin.type.ObjectType$Exporter$Reference')
_JExporterAdapter = jpy.get_type('io.deephaven.server.plugin.python.ExporterAdapter')


def adapt_reference(ref: _JReference) -> Reference:
    return Reference(ref.index(), ref.type().orElse(None), bytes(ref.ticket()))


class ExporterAdapter(Exporter):
    def __init__(self, exporter: _JExporterAdapter):
        self._exporter = exporter

    def reference(self, object) -> Reference:
        if isinstance(object, jpy.JType):
            ref = self._exporter.reference(object)
        else:
            ref = self._exporter.referencePyObject(object)
        return adapt_reference(ref)

    def new_reference(self, object) -> Reference:
        if isinstance(object, jpy.JType):
            ref = self._exporter.newReference(object)
        else:
            ref = self._exporter.newReferencePyObject(object)
        return adapt_reference(ref)

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
