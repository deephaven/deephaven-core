#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import jpy

from typing import Optional, Union
from deephaven.plugin.object import Exporter, ObjectType, Reference, BidiObjectBase, MessageSender
from deephaven._wrapper import JObjectWrapper

_JReference = jpy.get_type('io.deephaven.plugin.type.ObjectType$Exporter$Reference')
_JExporterAdapter = jpy.get_type('io.deephaven.server.plugin.python.ExporterAdapter')
_JMessageSender= jpy.get_type('io.deephaven.plugin.type.ObjectType$MessageSender')


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

    def reference(self, object, allow_unknown_type: bool = False, force_new: bool = False) -> Optional[Reference]:
        object = _unwrap(object)
        if isinstance(object, jpy.JType):
            ref = self._exporter.reference(object, allow_unknown_type, force_new)
        else:
            ref = self._exporter.referencePyObject(object, allow_unknown_type, force_new)
        return _adapt_reference(ref) if ref else None

    def __str__(self):
        return str(self._exporter)


class MessageSenderAdapter(MessageSender, ExporterAdapter):

    def __init__(self, sender: _JMessageSender):
        ExporterAdapter.__init__(self, _JExporterAdapter(sender.getExporter()))
        self._sender = sender

    def send_message(self, message: bytes):
        self._sender.sendMessage(message)


# see io.deephaven.server.plugin.python.ObjectTypeAdapter for calling details
class ObjectTypeAdapter:
    def __init__(self, user_object_type: ObjectType):
        self._user_object_type = user_object_type

    def is_type(self, object):
        return self._user_object_type.is_type(object)

    def to_bytes(self, exporter: _JExporterAdapter, object):
        return self._user_object_type.to_bytes(ExporterAdapter(exporter), object)

    def supports_bidi_messaging(self, object):
        return self._user_object_type.supports_bidi_messaging(object)

    def handle_message(self, message, object, objects):
        self._user_object_type.handle_message(bytes(message), object, objects)

    def add_message_sender(self, object, sender):
        self._user_object_type.add_message_sender(object, MessageSenderAdapter(sender))

    def remove_message_sender(self):
        self._user_object_type.remove_message_sender()

    def __str__(self):
        return str(self._user_object_type)
