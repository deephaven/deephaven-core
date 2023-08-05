#
# Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#
import io
from typing import Any

from pydeephaven.dherror import DHError
from pydeephaven.proto import object_pb2_grpc, object_pb2


class PluginObjService:
    def __init__(self, session):
        self.session = session
        self._grpc_app_stub = object_pb2_grpc.ObjectServiceStub(session.grpc_channel)

    def message_stream(self, req_stream) -> Any:
        """Fetches the current application fields."""
        try:
            resp = self._grpc_app_stub.MessageStream(req_stream, metadata=self.session.grpc_metadata)
            # resp = self._grpc_app_stub.MessageStream(iter((stream_req,)), metadata=self.session.grpc_metadata)
            return resp
        except Exception as e:
            raise DHError("failed to establish bidirectional stream with the server.") from e
