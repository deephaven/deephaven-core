//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.proto.util;

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

    static StatusRuntimeException error(io.grpc.Status.Code code, String message) {
        return code
                .toStatus()
                .withDescription("Flight SQL: " + message)
                .asRuntimeException();
    }

    static StatusRuntimeException error(io.grpc.Status.Code code, String message, Throwable cause) {
        return code
                .toStatus()
                .withDescription("Flight SQL: " + message)
                .withCause(cause)
                .asRuntimeException();
    }
}
