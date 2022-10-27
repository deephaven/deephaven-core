/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
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
import jakarta.websocket.Session;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
 * <p>
 * </p>
 * JSR356 websockets always handle their incoming messages in a serial manner, so we don't need to worry here about
 * runOnTransportThread while in onMessage, as we're already in the transport thread.
 */
public class MultiplexedWebSocketServerStream extends AbstractWebSocketServerStream {
    private static final Logger logger = Logger.getLogger(MultiplexedWebSocketServerStream.class.getName());
    /** Custom metadata to hold the path requested by the incoming stream */
    public static final Metadata.Key<String> PATH =
            Metadata.Key.of("grpc-websockets-path", Metadata.ASCII_STRING_MARSHALLER);

    public static final String GRPC_WEBSOCKETS_MULTIPLEX_PROTOCOL = "grpc-websockets-multiplex";

    private final InternalLogId logId = InternalLogId.allocate(MultiplexedWebSocketServerStream.class, null);

    // No need to be thread-safe, this will only be accessed from the transport thread.
    private final Map<Integer, MultiplexedWebsocketStreamImpl> streams = new HashMap<>();
    private final boolean isTextRequest = false;// not supported yet

    public MultiplexedWebSocketServerStream(ServerTransportListener transportListener,
            List<? extends ServerStreamTracer.Factory> streamTracerFactories, int maxInboundMessageSize,
            Attributes attributes) {
        super(transportListener, streamTracerFactories, maxInboundMessageSize, attributes);
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
        if (error instanceof ClosedChannelException) {
            // ignore this for now
            // TODO need to understand why this is happening
        } else {
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

        MultiplexedWebsocketStreamImpl stream =
                new MultiplexedWebsocketStreamImpl(statsTraceCtx, maxInboundMessageSize, websocketSession, logId,
                        attributes, streamId);
        stream.createStream(transportListener, path, headers);
        streams.put(streamId, stream);
    }
}
