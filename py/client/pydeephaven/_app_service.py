#
# Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
#
from typing import Any

from deephaven_core.proto import application_pb2, application_pb2_grpc
from pydeephaven.dherror import DHError


class AppService:
    def __init__(self, session):
        self.session = session
        self._grpc_app_stub = application_pb2_grpc.ApplicationServiceStub(
            session.grpc_channel
        )

    def list_fields(self) -> Any:
        """Fetches the current application fields."""
        try:
            fields = self.session.wrap_bidi_rpc(
                self._grpc_app_stub.ListFields, application_pb2.ListFieldsRequest()
            )
            return fields
        except Exception as e:
            raise DHError("failed to list fields.") from e
