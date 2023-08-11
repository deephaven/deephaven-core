#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import jpy

from typing import Optional, List, Any
from deephaven.plugin.object_type import Exporter, ObjectType, Reference, MessageStream, FetchOnlyObjectType
from deephaven._wrapper import JObjectWrapper, wrap_j_object

JReference = jpy.get_type('io.deephaven.plugin.type.Exporter$Reference')
JExporterAdapter = jpy.get_type('io.deephaven.server.plugin.python.ExporterAdapter')
JMessageStream = jpy.get_type('io.deephaven.plugin.type.ObjectType$MessageStream')


def _adapt_reference(ref: JReference) -> Reference:
    return Reference(ref.index(), ref.type().orElse(None))


def _unwrap(object):
    # todo: we should have generic unwrapping code ABC
    if isinstance(object, JObjectWrapper):
        return object.j_object
    return object


class ExporterAdapter(Exporter):
    """Python implementation of Exporter that delegates to its Java counterpart."""

    def __init__(self, exporter: JExporterAdapter):
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


class ClientResponseStreamAdapter(MessageStream):
    """Python implementation of MessageStream that delegates to its Java counterpart"""
    def __init__(self, wrapped: JMessageStream):
        self._wrapped = wrapped

    def on_data(self, payload: bytes, references: List[Any]) -> None:
        self._wrapped.onData(payload, [_unwrap(ref) for ref in references])

    def on_close(self) -> None:
        self._wrapped.onClose()


class ServerRequestStreamAdapter(MessageStream):
    """Wraps Python server MessageStream implementations to correctly adapt objects coming from the client
    """
    def __init__(self, wrapped: MessageStream):
        self._wrapped = wrapped

    def on_data(self, payload:bytes, references: List[Any]) -> None:
        self._wrapped.on_data(payload, [wrap_j_object(ref) for ref in references])

    def on_close(self) -> None:
        self._wrapped.on_close()


class ObjectTypeAdapter:
    """Python type that Java's ObjectTypeAdapter will call in order to communicate with a Python ObjectType instance."""

    def __init__(self, user_object_type: ObjectType):
        self._user_object_type = user_object_type

    def is_type(self, obj) -> bool:
        return self._user_object_type.is_type(obj)

    def is_fetch_only(self) -> bool:
        return isinstance(self._user_object_type, FetchOnlyObjectType)

    def to_bytes(self, exporter: JExporterAdapter, obj: Any) -> bytes:
        return self._user_object_type.to_bytes(ExporterAdapter(exporter), obj)

    def create_client_connection(self, obj: Any, connection: JMessageStream) -> MessageStream:
        return ServerRequestStreamAdapter(
            self._user_object_type.create_client_connection(obj, ClientResponseStreamAdapter(connection))
        )

    def __str__(self):
        return str(self._user_object_type)
