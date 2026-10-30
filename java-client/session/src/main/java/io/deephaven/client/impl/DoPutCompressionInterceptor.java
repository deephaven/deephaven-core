//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.MethodDescriptor;

/**
 * Compresses the messages of each Arrow Flight {@code DoPut} call, which carry uploads and the rows added to input
 * tables, with one compression type. See {@link ClientConfig#requestCompression()}.
 */
final class DoPutCompressionInterceptor implements ClientInterceptor {

    static final String DO_PUT =
            MethodDescriptor.generateFullMethodName("arrow.flight.protocol.FlightService", "DoPut");

    private final String compression;

    /**
     * @param compression the compression type; the channel's compressor registry must hold it
     */
    DoPutCompressionInterceptor(final String compression) {
        this.compression = compression;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            final MethodDescriptor<ReqT, RespT> method,
            final CallOptions callOptions,
            final Channel next) {
        return next.newCall(method, DO_PUT.equals(method.getFullMethodName())
                ? callOptions.withCompression(compression)
                : callOptions);
    }
}
