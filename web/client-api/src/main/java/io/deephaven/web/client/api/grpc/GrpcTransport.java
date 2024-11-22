package io.deephaven.web.client.api.grpc;

import com.vertispan.tsdefs.annotations.TsInterface;
import elemental2.core.JsArray;
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
     * Return true to signal that the client may have {@link #sendMessage(Uint8Array)} called on it more than once before {@link #finishSend()} should be called.
     * @return true to signal that the implementation can stream multiple messages, false otherwise indicating that Open/Next gRPC calls should be used
     */
    boolean supportsClientStreaming();
    /**
     * Starts the stream, sending metadata to the server.
     * @param metadata the headers to send the server when opening the connection
     */
    void start(JsPropertyMap<JsArray<String>> metadata);

    /**
     * Sends a message to the server.
     * @param msgBytes bytes to send to the server
     */
    void sendMessage(Uint8Array msgBytes);

    /**
     * "Half close" the stream, signaling to the server that no more messages will be sent, but that the client is still open to receiving messages.
     */
    void finishSend();

    /**
     * End the stream, both notifying the server that no more messages will be sent nor received, and preventing
     * the client from receiving any more events.
     */
    void cancel();
}
