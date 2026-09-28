//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.grpc;

import com.vertispan.tsdefs.annotations.TsInterface;
import elemental2.core.Uint8Array;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;

/**
 * Factory for creating gRPC transports.
 */
@TsInterface
@JsType(namespace = "dh.grpc")
public interface GrpcTransportFactory {
    /**
     * Create a new transport instance.
     *
     * @param options options for creating the transport
     * @return a transport instance to use for gRPC communication
     */
    GrpcTransport create(GrpcTransportOptions options);

    /**
     * Return {@code true} to signal that created transports may have {@link GrpcTransport#sendMessage(Uint8Array)}
     * called on it more than once before {@link GrpcTransport#finishSend()} should be called.
     *
     * @return {@code true} to signal that the implementation can stream multiple messages, or {@code false} to indicate
     *         that Open/Next gRPC calls should be used.
     */
    @JsProperty
    boolean getSupportsClientStreaming();
}
