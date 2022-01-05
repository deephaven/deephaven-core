/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

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
}
