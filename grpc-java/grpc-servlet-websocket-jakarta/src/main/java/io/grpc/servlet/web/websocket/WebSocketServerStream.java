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
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static io.grpc.internal.GrpcUtil.TIMEOUT_KEY;

/**
 * Each instance of this type represents a single active websocket, which maps to a single gRPC stream.
 */
public class WebSocketServerStream extends AbstractWebSocketServerStream {
    private static final Logger logger = Logger.getLogger(WebSocketServerStream.class.getName());

    private final InternalLogId logId = InternalLogId.allocate(WebSocketServerStream.class, null);

    public static final String GRPC_WEBSOCKETS_PROTOCOL = "grpc-websockets";

    // fields set after headers are decoded
    private WebsocketStreamImpl stream;
    private boolean headersProcessed = false;
    private final boolean isTextRequest = false;// not supported yet

    public WebSocketServerStream(ServerTransportListener transportListener,
            List<? extends ServerStreamTracer.Factory> streamTracerFactories, int maxInboundMessageSize,
            Attributes attributes) {
        super(transportListener, streamTracerFactories, maxInboundMessageSize, attributes);
    }

    @Override
    public void onMessage(String message) {
        if (stream != null) {
            // This means the stream opened correctly, then sent a text payload, which doesn't make sense.
            // End the stream first.
            stream.transportReportStatus(Status.fromCode(Status.Code.UNKNOWN));
        }
        try {
            websocketSession
                    .close(new CloseReason(CloseReason.CloseCodes.PROTOCOL_ERROR, "Can't read string payloads"));
        } catch (IOException ignored) {
            // ignoring failure
        }
    }

    @Override
    public void onMessage(ByteBuffer message) throws IOException {
        if (message.remaining() == 0) {
            // message is empty (no control flow, no data), error
            if (stream != null) {
                stream.transportReportStatus(Status.fromCode(Status.Code.UNKNOWN));
            }
            websocketSession.close(new CloseReason(CloseReason.CloseCodes.PROTOCOL_ERROR, "Unexpected empty message"));
            return;
        }

        // if this is the first message on this websocket, it is the request headers
        if (!headersProcessed) {
            processHeaders(message);
            headersProcessed = true;
            return;
        }

        // For every message after headers, the first byte is control flow
        byte controlFlow = message.get();
        if (controlFlow == 1) {
            // if first byte is 1, the client is finished sending
            if (message.remaining() != 0) {
                stream.transportReportStatus(Status.fromCode(Status.Code.UNKNOWN));
                websocketSession.close(
                        new CloseReason(CloseReason.CloseCodes.PROTOCOL_ERROR, "Unexpected bytes in close message"));
                return;
            }
            stream.inboundDataReceived(ReadableBuffers.empty(), true);
            return;
        }

        if (isTextRequest) {
            throw new UnsupportedOperationException("text requests not yet supported");
        }

        // Having already stripped the control flow byte, the rest of the payload is our request message
        stream.inboundDataReceived(ReadableBuffers.wrap(message), false);
    }

    @Override
    public void onError(Session session, Throwable error) {
        stream.transportReportStatus(Status.UNKNOWN);// transport failure of some kind
        // onClose will be called automatically
        if (error instanceof ClosedChannelException) {
            // ignore this for now
            // TODO need to understand why this is happening
        } else {
            logger.log(Level.SEVERE, "Error from websocket", error);
        }
    }

    private String methodName() {
        return websocketSession.getRequestURI().getPath().substring(1);
    }

    private void processHeaders(ByteBuffer headerPayload) {
        Metadata headers = readHeaders(headerPayload);

        Long timeoutNanos = headers.get(TIMEOUT_KEY);
        if (timeoutNanos == null) {
            timeoutNanos = 0L;
        }
        // TODO handle timeout

        StatsTraceContext statsTraceCtx =
                StatsTraceContext.newServerContext(streamTracerFactories, methodName(), headers);

        stream = new WebsocketStreamImpl(statsTraceCtx, maxInboundMessageSize, websocketSession, logId,
                attributes);
        stream.createStream(transportListener, methodName(), headers);
    }
}
