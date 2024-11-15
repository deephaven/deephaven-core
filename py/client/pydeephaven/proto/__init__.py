#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

# Re-export the generated protobuf and gRPC modules in their old locations
from deephaven_core.proto import application_pb2, application_pb2_grpc
from deephaven_core.proto import config_pb2, config_pb2_grpc
from deephaven_core.proto import console_pb2, console_pb2_grpc
from deephaven_core.proto import hierarchicaltable_pb2, hierarchicaltable_pb2_grpc
from deephaven_core.proto import inputtable_pb2, inputtable_pb2_grpc
from deephaven_core.proto import object_pb2, object_pb2_grpc
from deephaven_core.proto import partitionedtable_pb2, partitionedtable_pb2_grpc
from deephaven_core.proto import session_pb2, session_pb2_grpc
from deephaven_core.proto import storage_pb2, storage_pb2_grpc
from deephaven_core.proto import table_pb2, table_pb2_grpc
from deephaven_core.proto import ticket_pb2

# Log a warning that users should update imports
from warnings import warn

warn('The pydeephaven.proto namespace is deprecated, please use deephaven_core.proto instead. Deprecated in 0.37.0. Will be removed in 0.39.0', FutureWarning, stacklevel=2)
