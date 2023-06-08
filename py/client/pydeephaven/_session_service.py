#
# Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#

import grpc

from pydeephaven.dherror import DHError
from pydeephaven.proto import session_pb2_grpc, session_pb2


class SessionService:
    def __init__(self, session):
        self.session = session
        self._grpc_session_stub = None

    def connect(self) -> grpc.Channel:
        """Connects to the server and returns a gRPC channel upon success."""
        target = ":".join([self.session.host, str(self.session.port)])
        if self.session.target_name_override is None:
            options = None
        else:
            options = (('grpc.ssl_target_name_override', self.session.target_name_override))
        if self.session.use_tls:
            credentials = grpc.ssl_channel_credentials(root_certificates=self.session.pem)
            grpc_channel = grpc.secure_channel(target, credentials, options) 
        else:
            grpc_channel = grpc.insecure_channel(target)
        self._grpc_session_stub = session_pb2_grpc.SessionServiceStub(grpc_channel)
        return grpc_channel

    def close(self):
        """Closes the gRPC connection."""
        try:
            self._grpc_session_stub.CloseSession(
                session_pb2.HandshakeRequest(auth_protocol=0, payload=self.session._auth_token),
                metadata=self.session.grpc_metadata)
        except Exception as e:
            raise DHError("failed to close the session.") from e

    def release(self, ticket):
        """Releases an exported ticket."""
        try:
            self._grpc_session_stub.Release(session_pb2.ReleaseRequest(id=ticket), metadata=self.session.grpc_metadata)
        except Exception as e:
            raise DHError("failed to release a ticket.") from e
