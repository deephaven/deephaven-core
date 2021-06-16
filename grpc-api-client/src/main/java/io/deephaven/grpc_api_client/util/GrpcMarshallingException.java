package io.deephaven.grpc_api_client.util;

import io.deephaven.UncheckedDeephavenException;

public class GrpcMarshallingException extends UncheckedDeephavenException {
    public GrpcMarshallingException(final String reason, final Exception cause) {
        super(reason, cause);
    }
}
