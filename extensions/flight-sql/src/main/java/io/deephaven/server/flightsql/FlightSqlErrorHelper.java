//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.flightsql;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

final class FlightSqlErrorHelper {

    static StatusRuntimeException error(Status.Code code, String message) {
        return code
                .toStatus()
                .withDescription("Flight SQL: " + message)
                .asRuntimeException();
    }

    static StatusRuntimeException error(Status.Code code, String message, Throwable cause) {
        return code
                .toStatus()
                .withDescription("Flight SQL: " + message)
                .withCause(cause)
                .asRuntimeException();
    }
}
