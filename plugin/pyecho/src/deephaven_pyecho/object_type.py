#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#
from typing import Any

from deephaven.plugin.object_type import BidirectionalObjectType, MessageStream
from deephaven.plugin_authorization import transform

from ._pyecho import PyEcho


class PyEchoMessageStream(MessageStream):
    """Echoes objects back to the client, applying the authorization transform to each reference first.

    Because the transform is applied here - at connection/render time, in the requesting user's context - the echoed
    references carry that user's ACLs. A reference the user is not permitted to access is dropped.
    """

    def __init__(self, echo: PyEcho, connection: MessageStream):
        self._echo = echo
        self._connection = connection

    def start(self) -> None:
        # Echo the wrapped object back to the client as the initial message.
        self._echo_objects(b"", [self._echo.value])

    def on_data(self, payload: bytes, references: list[Any]) -> None:
        # Echo whatever references the client sent us straight back.
        self._echo_objects(payload, list(references))

    def on_close(self) -> None:
        pass

    def _echo_objects(self, payload: bytes, objects: list[Any]) -> None:
        transformed = []
        for obj in objects:
            # transform() returns None when the current user is not permitted to access the object; we drop those.
            authorized = transform(obj)
            if authorized is not None:
                transformed.append(authorized)
        self._connection.on_data(payload, transformed)


class PyEchoType(BidirectionalObjectType):
    """An example ObjectType plugin that echoes a :class:`PyEcho`'s wrapped object back to the client.

    It performs its own authorization transform on the references it exports (see :class:`PyEchoMessageStream`), so it
    declares ``authorization_export_behavior = "manual"`` to opt out of the server's automatic transformation and avoid
    transforming twice.
    """

    # Opt out of server-applied transformation; this plugin transforms its own references.
    authorization_export_behavior = "manual"

    @property
    def name(self) -> str:
        return "PyEcho"

    def is_type(self, obj: Any) -> bool:
        return isinstance(obj, PyEcho)

    def create_client_connection(self, obj: object, connection: MessageStream) -> MessageStream:
        if not isinstance(obj, PyEcho):
            raise TypeError(f"Expected PyEcho, got {type(obj)}")
        client_connection = PyEchoMessageStream(obj, connection)
        client_connection.start()
        return client_connection
