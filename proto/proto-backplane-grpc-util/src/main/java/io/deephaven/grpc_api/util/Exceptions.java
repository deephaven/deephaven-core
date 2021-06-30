package io.deephaven.grpc_api.util;

import com.google.rpc.Code;
import com.google.rpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;

public class Exceptions {
    public static StatusRuntimeException statusRuntimeException(final Code statusCode,
        final String details) {
        return StatusProto.toStatusRuntimeException(
            Status.newBuilder().setCode(statusCode.getNumber()).setMessage(details).build());
    }
}
