#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import jpy

from typing import Optional, List, Any
from deephaven.plugin.object import Exporter, ObjectType, Reference, MessageStream, FetchOnlyObjectType
from deephaven._wrapper import JObjectWrapper

_JReference = jpy.get_type('io.deephaven.plugin.type.ObjectType$Exporter$Reference')
_JExporterAdapter = jpy.get_type('io.deephaven.server.plugin.python.ExporterAdapter')
_JMessageStream = jpy.get_type('io.deephaven.plugin.type.ObjectType$MessageStream')


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

    def reference(self, obj: Any, allow_unknown_type: bool = True, force_new: bool = True) -> Optional[Reference]:
        obj = _unwrap(obj)
        if isinstance(obj, jpy.JType):
            ref = self._exporter.reference(obj, allow_unknown_type, force_new)
        else:
            ref = self._exporter.referencePyObject(obj, allow_unknown_type, force_new)
        return _adapt_reference(ref) if ref else None

    def __str__(self):
        return str(self._exporter)


class MessageStreamAdapter(MessageStream):
    def __init__(self, wrapped: _JMessageStream):
        self._wrapped = wrapped

    def on_data(self, payload: bytes, references: List[Any]):
        self._wrapped.on_data(payload, [_unwrap(ref) for ref in references])

    def on_close(self):
        self._wrapped.on_close()


# see io.deephaven.server.plugin.python.ObjectTypeAdapter for calling details
class ObjectTypeAdapter:
    def __init__(self, user_object_type: ObjectType):
        self._user_object_type = user_object_type

    def is_type(self, obj):
        return self._user_object_type.is_type(obj)

    def is_fetch_only(self):
        return isinstance(self._user_object_type, FetchOnlyObjectType)

    def to_bytes(self, exporter: _JExporterAdapter, obj: Any):
        return self._user_object_type.to_bytes(ExporterAdapter(exporter), obj)

    def create_client_connection(self, obj: Any, connection: _JMessageStream):
        return self._user_object_type.create_client_connection(obj, MessageStreamAdapter(connection))

    def __str__(self):
        return str(self._user_object_type)
