//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.grpc;

import com.vertispan.tsdefs.annotations.TsInterface;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.browserheaders.BrowserHeaders;
import io.deephaven.javascript.proto.dhinternal.grpcweb.transports.transport.Transport;
import io.deephaven.javascript.proto.dhinternal.grpcweb.transports.transport.TransportFactory;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

/**
 * Factory for creating gRPC transports.
 */
@TsInterface
@JsType(namespace = "dh.grpc", isNative = true)
public interface GrpcTransportFactory {
    /**
     * Create a new transport instance.
     * 
     * @param options options for creating the transport
     * @return a transport instance to use for gRPC communication
     */
    GrpcTransport create(GrpcTransportOptions options);

    /**
     * Return true to signal that created transports may have {@link GrpcTransport#sendMessage(Uint8Array)} called on it
     * more than once before {@link GrpcTransport#finishSend()} should be called.
     * 
     * @return true to signal that the implementation can stream multiple messages, false otherwise indicating that
     *         Open/Next gRPC calls should be used
     */
    @JsProperty
    boolean getSupportsClientStreaming();

    /**
     * Adapt this factory to the transport factory used by the gRPC-web library.
     */
    @JsOverlay
    default TransportFactory adapt() {
        return options -> {
            GrpcTransport impl = create(GrpcTransportOptions.from(options));
            return new Transport() {
                @Override
                public void cancel() {
                    impl.cancel();
                }

                @Override
                public void finishSend() {
                    impl.finishSend();
                }

                @Override
                public void sendMessage(Uint8Array msgBytes) {
                    impl.sendMessage(msgBytes);
                }

                @Override
                public void start(BrowserHeaders metadata) {
                    impl.start(Js.cast(metadata.headersMap));
                }
            };
        };
    }
}
