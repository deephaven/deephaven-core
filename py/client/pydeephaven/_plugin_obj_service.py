#
# Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#
import io
from typing import Any

from pydeephaven.dherror import DHError
from pydeephaven.proto import object_pb2_grpc
from pydeephaven.experimental.plugin_client import PluginRequestStream


"""This module provides internal tools to communicate with plugins on the server."""


class PluginObjService:
    """
    PluginObjectService defines utility methods to make gRPC calls to the ObjectService.
    """
    def __init__(self, session: 'pydeephaven.session.Session'):
        self.session = session
        self._grpc_app_stub = object_pb2_grpc.ObjectServiceStub(session.grpc_channel)

    def message_stream(self, req_stream: PluginRequestStream) -> Any:
        """Opens a connection to the server-side implementation of this plugin."""
        try:
            resp = self._grpc_app_stub.MessageStream(req_stream, metadata=self.session.grpc_metadata)
            return resp
        except Exception as e:
            raise DHError("failed to establish bidirectional stream with the server.") from e
