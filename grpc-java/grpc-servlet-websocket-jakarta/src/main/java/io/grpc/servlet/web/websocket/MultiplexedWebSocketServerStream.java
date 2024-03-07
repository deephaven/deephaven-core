/*
 * Copyright 2022 Deephaven Data Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.grpc.servlet.web.websocket;

import io.grpc.Attributes;
import io.grpc.InternalLogId;
import io.grpc.Metadata;
import io.grpc.ServerStreamTracer;
import io.grpc.Status;
import io.grpc.internal.ReadableBuffers;
import io.grpc.internal.ServerTransportListener;
import io.grpc.internal.StatsTraceContext;
import jakarta.websocket.CloseReason;
import jakarta.websocket.EndpointConfig;
import jakarta.websocket.Session;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import static io.grpc.internal.GrpcUtil.TIMEOUT_KEY;

/**
 * Each instance of this type represents a single active websocket, which can allow several concurrent/overlapping gRPC
 * streams. This is in contrast to the {@link WebSocketServerStream} type, which supports one websocket per gRPC stream.
 * <p>
 * </p>
 * To achieve this, each grpc message starts with a 32 bit integer indicating the ID of the stream. If the MSB of that
 * int is 1, then the request must be closed by this message, and that MSB is set to zero to read the ID of the stream.
 * On the initial request, an extra header is sent from the client, indicating the path to the service method.
 * Technically, this makes it possible for a grpc message to split across several websocket frames, but at this time
 * each grpc message is exactly one websocket frame.
 */
public class MultiplexedWebSocketServerStream extends AbstractWebSocketServerStream {
    public static final String GRACEFUL_CLOSE = MultiplexedWebSocketServerStream.class.getName() + ".graceful_close";

    /**
     * Callback to initiate a graceful shutdown of this websocket instance, as an alternative to just closing the
     * websocket. Since this websocket behaves like gRPC transport, we give the client a chance to finish up and close
     * itself before the server does it.
     */
    public interface GracefulClose extends Supplier<CompletableFuture<Void>> {
    }

    private static final Logger logger = Logger.getLogger(MultiplexedWebSocketServerStream.class.getName());
    /** Custom metadata to hold the path requested by the incoming stream */
    public static final Metadata.Key<String> PATH =
            Metadata.Key.of("grpc-websockets-path", Metadata.ASCII_STRING_MARSHALLER);

    public static final String GRPC_WEBSOCKETS_MULTIPLEX_PROTOCOL = "grpc-websockets-multiplex";

    // No need to be thread-safe, this will only be accessed from the jsr356 callbacks in a serial manner
    private final Map<Integer, MultiplexedWebsocketStreamImpl> streams = new HashMap<>();
    private final boolean isTextRequest = false;// not supported yet

    /**
     * Enum to describe the process of closing a transport. After the server has begun to close but the client hasn't
     * yet acknowledged, it is permitted for the client to start a new stream, but after the server acknowledges, no new
     * streams can be started. Note that shutdown will proceed anyway, and will eventually stop all streams.
     */
    enum ClosedState {
        OPEN, CLOSING, CLOSED
    }

    private ClosedState closed = ClosedState.OPEN;
    private final CompletableFuture<Void> closingFuture = new CompletableFuture<>();

    public MultiplexedWebSocketServerStream(ServerTransportListener transportListener,
            List<? extends ServerStreamTracer.Factory> streamTracerFactories, int maxInboundMessageSize,
            Attributes attributes) {
        super(transportListener, streamTracerFactories, maxInboundMessageSize, attributes);
    }

    /**
     * Stops this multiplexed transport from accepting new streams. Instead, it will reply with its version of GO_AWAY,
     * a stream of Integer.MAX_INTEGER to the client to signal that new requests will not be accepted, and future
     * incoming streams will be closed by the server right away. In keeping with h2, until the client ACKs the close, we
     * will permit incoming streams that were sent before we closed, but they likely will not have a large window to get
     * their work done before they are closed the rest of the way.
     */
    private CompletableFuture<Void> stopAcceptingNewStreams() {
        if (closed != ClosedState.OPEN) {
            return closingFuture;
        }
        closed = ClosedState.CLOSING;
        ByteBuffer end = ByteBuffer.allocate(4);
        end.putInt(0, Integer.MAX_VALUE);
        // ignore the results of this future, the closingFuture will instead tell us when the client ACKs
        websocketSession.getAsyncRemote().sendBinary(end);
        return closingFuture;
    }

    @Override
    public void onOpen(Session websocketSession, EndpointConfig config) {
        super.onOpen(websocketSession, config);
        websocketSession.getUserProperties().put(GRACEFUL_CLOSE, (GracefulClose) this::stopAcceptingNewStreams);
    }

    @Override
    public void onClose(Session session, CloseReason closeReason) {
        // regardless of state, indicate that we are already closed and no need to wait
        closingFuture.complete(null);
    }

    @Override
    public void onMessage(String message) {
        for (MultiplexedWebsocketStreamImpl stream : streams.values()) {
            // This means the stream opened correctly, then sent a text payload, which doesn't make sense.
            // End the stream first.
            stream.transportReportStatus(Status.fromCode(Status.Code.UNKNOWN));
        }
        streams.clear();
        try {
            websocketSession
                    .close(new CloseReason(CloseReason.CloseCodes.PROTOCOL_ERROR, "Can't read string payloads"));
        } catch (IOException ignored) {
            // ignoring failure
        }
    }

    @Override
    public void onMessage(ByteBuffer message) throws IOException {
        // Each message starts with an int, to indicate stream id. If that int is negative, the other end has performed
        // a half close (and this is the final message).
        int streamId = message.getInt();
        final boolean closed;
        if (streamId < 0) {
            closed = true;
            // unset the msb, extract the actual streamid
            streamId = streamId ^ (1 << 31);
        } else {
            closed = false;
        }

        if (closed && streamId == Integer.MAX_VALUE) {
            if (this.closed != ClosedState.CLOSING) {
                // error, client tried to finish a close we didn't initiate, hang up
                websocketSession.close(new CloseReason(CloseReason.CloseCodes.PROTOCOL_ERROR, "Unexpected close ACK"));
                return;
            }
            // Client has ack'd our close, no more new streams allowed, client will be finishing up so we can exit
            this.closed = ClosedState.CLOSED;

            // Mark the future as finished
            closingFuture.complete(null);

            // (note that technically there is a 5th byte to indicate close, but we're ignoring that here)
            return;
        }

        // may be null if this is the first request for this streamId
        final MultiplexedWebsocketStreamImpl stream = streams.get(streamId);

        if (message.remaining() == 0) {
            // message is empty (no control flow, no data), error
            if (stream != null) {
                stream.transportReportStatus(Status.fromCode(Status.Code.UNKNOWN));
                streams.remove(streamId);
            }
            websocketSession.close(new CloseReason(CloseReason.CloseCodes.PROTOCOL_ERROR, "Unexpected empty message"));
            return;
        }

        // if this is the first message on this websocket, it is the request headers
        if (stream == null) {
            if (this.closed == ClosedState.CLOSED) {
                // Not accepting new streams on existing websockets, and the client knew that when they sent this (since
                // the GO_AWAY was ACK'd). We treat this as an error, since the client isn't behaving. If instead closed
                // was still CLOSING, then, client sent this before they saw that, we permit them to still open streams,
                // though the application likely has begun to clean up state.
                websocketSession.close(new CloseReason(CloseReason.CloseCodes.PROTOCOL_ERROR,
                        "Stream created after closing initiated"));

                return;
            }
            processHeaders(message, streamId);
            return;
        }

        // For every message after headers, the next byte is control flow - this is technically already managed by
        // "closed", but this lets us stay somewhat closer to the underlying grpc/grpc-web format.
        byte controlFlow = message.get();
        if (controlFlow == 1) {
            assert closed;
            // if first byte is 1, the client is finished sending
            if (message.remaining() != 0) {
                stream.transportReportStatus(Status.fromCode(Status.Code.UNKNOWN));
                streams.remove(streamId);
                websocketSession.close(
                        new CloseReason(CloseReason.CloseCodes.PROTOCOL_ERROR, "Unexpected bytes in close message"));
                return;
            }
            stream.inboundDataReceived(ReadableBuffers.empty(), true);
            streams.remove(streamId);
            return;
        }
        assert !closed;

        if (isTextRequest) {
            throw new UnsupportedOperationException("text requests not yet supported");
        }

        // Having already stripped the control flow byte, the rest of the payload is our request message
        stream.inboundDataReceived(ReadableBuffers.wrap(message), false);
    }

    @Override
    public void onError(Session session, Throwable error) {
        for (MultiplexedWebsocketStreamImpl stream : streams.values()) {
            stream.transportReportStatus(Status.UNKNOWN);// transport failure of some kind
        }
        streams.clear();
        // onClose will be called automatically

        // These two IOExceptions frequently occur when clients close the connection
        if (!(error instanceof ClosedChannelException || error instanceof EOFException)) {
            logger.log(Level.SEVERE, "Error from websocket", error);
        }
    }

    private void processHeaders(ByteBuffer headerPayload, int streamId) {
        Metadata headers = readHeaders(headerPayload);

        String path = headers.get(PATH);

        Long timeoutNanos = headers.get(TIMEOUT_KEY);
        if (timeoutNanos == null) {
            timeoutNanos = 0L;
        }
        // TODO handle timeout on a per-stream basis

        StatsTraceContext statsTraceCtx =
                StatsTraceContext.newServerContext(streamTracerFactories, path, headers);

        InternalLogId logId = InternalLogId.allocate(MultiplexedWebSocketServerStream.class, null);

        MultiplexedWebsocketStreamImpl stream =
                new MultiplexedWebsocketStreamImpl(statsTraceCtx, maxInboundMessageSize, websocketSession, logId,
                        attributes, streamId);
        stream.createStream(transportListener, path, headers);
        streams.put(streamId, stream);
    }
}
