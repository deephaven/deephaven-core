package io.deephaven.web.client.api.grpc;

import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.browserheaders.BrowserHeaders;
import io.deephaven.javascript.proto.dhinternal.grpcweb.transports.transport.Transport;
import io.deephaven.javascript.proto.dhinternal.grpcweb.transports.transport.TransportFactory;
import jsinterop.annotations.JsFunction;
import jsinterop.annotations.JsOverlay;

/**
 * Factory for creating gRPC transports.
 */
@JsFunction
public interface GrpcTransportFactory {
    /**
     * Create a new transport instance.
     * @param options options for creating the transport
     * @return a transport to use for gRPC communication
     */
    GrpcTransport create(GrpcTransportOptions options);

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
                    impl.start(metadata.headersMap);
                }
            };
        };
    }
}
