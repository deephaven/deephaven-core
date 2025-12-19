#
# Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
#
"""This module provides internal tools to communicate with plugins on the server."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from deephaven_core.proto import object_pb2_grpc
from pydeephaven.dherror import DHError
from pydeephaven.experimental.plugin_client import PluginRequestStream

if TYPE_CHECKING:
    from pydeephaven.session import Session


class PluginObjService:
    """
    PluginObjectService defines utility methods to make gRPC calls to the ObjectService.
    """

    def __init__(self, session: Session) -> None:
        self.session = session
        self._grpc_app_stub = object_pb2_grpc.ObjectServiceStub(session.grpc_channel)

    def message_stream(self, req_stream: PluginRequestStream) -> Any:
        """Opens a connection to the server-side implementation of this plugin."""
        try:
            resp = self.session.wrap_bidi_rpc(
                self._grpc_app_stub.MessageStream, req_stream
            )
            return resp
        except Exception as e:
            raise DHError(
                "failed to establish bidirectional stream with the server."
            ) from e
