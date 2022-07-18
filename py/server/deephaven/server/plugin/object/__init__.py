#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import jpy

from typing import Optional
from deephaven.plugin.object import Exporter, ObjectType, Reference
from deephaven._wrapper import JObjectWrapper

_JReference = jpy.get_type('io.deephaven.plugin.type.ObjectType$Exporter$Reference')
_JExporterAdapter = jpy.get_type('io.deephaven.server.plugin.python.ExporterAdapter')


def _adapt_reference(ref: _JReference) -> Reference:
    return Reference(ref.index(), ref.type().orElse(None))


def _unwrap(object):
    # todo: we should have generic unwrapping code ABC
    if isinstance(object, JObjectWrapper):
        return object.j_object
    return object


class ExporterAdapter(Exporter):
    def __init__(self, exporter: _JExporterAdapter):
        self._exporter = exporter

    def reference(self, object, allow_unknown_type : bool = False, force_new : bool = False) -> Optional[Reference]:
        object = _unwrap(object)
        if isinstance(object, jpy.JType):
            ref = self._exporter.reference(object, allow_unknown_type, force_new)
        else:
            ref = self._exporter.referencePyObject(object, allow_unknown_type, force_new)
        return _adapt_reference(ref) if ref else None

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
