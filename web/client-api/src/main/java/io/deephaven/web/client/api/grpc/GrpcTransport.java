//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.grpc;

import com.vertispan.tsdefs.annotations.TsInterface;
import elemental2.core.Uint8Array;
import jsinterop.annotations.JsType;
import jsinterop.base.JsPropertyMap;

/**
 * gRPC transport implementation.
 */
@JsType(namespace = "dh.grpc")
@TsInterface
public interface GrpcTransport {
    /**
     * Starts the stream, sending metadata to the server.
     *
     * @param metadata the headers to send the server when opening the connection
     */
    void start(JsPropertyMap<HeaderValueUnion> metadata);

    /**
     * Sends a message to the server.
     *
     * @param msgBytes bytes to send to the server
     */
    void sendMessage(Uint8Array msgBytes);

    /**
     * "Half-close" the stream. This signals to the server that no more messages will be sent, but that the client is
     * still open to receiving messages.
     */
    void finishSend();

    /**
     * End the stream. This notifies the server that no more messages will be sent or received, and prevents the client
     * from receiving any more events.
     */
    void cancel();
}
