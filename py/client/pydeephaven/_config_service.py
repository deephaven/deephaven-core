#
# Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
#
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from deephaven_core.proto import config_pb2, config_pb2_grpc
from pydeephaven.dherror import DHError

if TYPE_CHECKING:
    from pydeephaven.session import Session


class ConfigService:
    def __init__(self, session: Session):
        self.session = session
        self._grpc_app_stub = config_pb2_grpc.ConfigServiceStub(session.grpc_channel)

    def get_configuration_constants(self) -> dict[str, Any]:
        """Fetches the server configuration as a dict."""
        try:
            response = self.session.wrap_rpc(
                self._grpc_app_stub.GetConfigurationConstants,
                config_pb2.ConfigurationConstantsRequest(),
            )
            return dict(response.config_values)
        except Exception as e:
            raise DHError("failed to get the configuration constants.") from e
