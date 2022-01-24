package io.grpc.servlet.web.websocket;

import io.grpc.Attributes;
import io.grpc.InternalLogId;
import io.grpc.InternalMetadata;
import io.grpc.Metadata;
import io.grpc.ServerStreamTracer;
import io.grpc.Status;
import io.grpc.internal.ReadableBuffers;
import io.grpc.internal.ServerTransportListener;
import io.grpc.internal.StatsTraceContext;
import jakarta.websocket.CloseReason;
import jakarta.websocket.EndpointConfig;
import jakarta.websocket.OnClose;
import jakarta.websocket.OnError;
import jakarta.websocket.OnMessage;
import jakarta.websocket.OnOpen;
import jakarta.websocket.Session;
import jakarta.websocket.server.ServerEndpoint;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.grpc.internal.GrpcUtil.TIMEOUT_KEY;

/**
 * Each instance of this type represents a single active websocket, which maps to a single gRPC stream.
 *
 * JSR356 websockets always handle their incoming messages in a serial manner, so we don't need to worry here about
 * runOnTransportThread while in onMessage, as we're already in the transport thread.
 */
@ServerEndpoint(value = "/{service}/{method}", subprotocols = "grpc-websockets")
public class WebSocketServerStream {
    private final ServerTransportListener transportListener;
    private final List<? extends ServerStreamTracer.Factory> streamTracerFactories;
    private final int maxInboundMessageSize;
    private final Attributes attributes;

    private final InternalLogId logId = InternalLogId.allocate(WebSocketServerStream.class, null);

    // assigned on open, always available
    private Session websocketSession;

    // fields set after headers are decoded
    private WebsocketStreamImpl stream;
    private boolean headersProcessed = false;
    private final boolean isTextRequest = false;// not supported yet

    public WebSocketServerStream(ServerTransportListener transportListener,
            List<? extends ServerStreamTracer.Factory> streamTracerFactories, int maxInboundMessageSize,
            Attributes attributes) {
        this.transportListener = transportListener;
        this.streamTracerFactories = streamTracerFactories;
        this.maxInboundMessageSize = maxInboundMessageSize;
        this.attributes = attributes;
    }

    @OnOpen
    public void onOpen(Session websocketSession, EndpointConfig config) {
        this.websocketSession = websocketSession;

        // Configure defaults present in some servlet containers to avoid some confusing limits. Subclasses
        // can override this method to control those defaults on their own.
        websocketSession.setMaxIdleTimeout(0);
        websocketSession.setMaxBinaryMessageBufferSize(Integer.MAX_VALUE);
    }

    @OnMessage
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

    @OnMessage
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

    @OnError
    public void onError(Throwable error) {
        stream.transportReportStatus(Status.UNKNOWN);// transport failure of some kind
        // onClose will be called automatically
        if (error instanceof ClosedChannelException) {
            // ignore this for now
            // TODO need to understand why this is happening
        } else {
            error.printStackTrace();
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

    private static Metadata readHeaders(ByteBuffer headerPayload) {
        // Headers are passed as ascii (browsers don't support binary), ":"-separated key/value pairs, separated on
        // "\r\n". The client implementation shows that values might be comma-separated, but we'll pass that through
        // directly as a plain string.
        //
        List<byte[]> byteArrays = new ArrayList<>();
        while (headerPayload.hasRemaining()) {
            int nameStart = headerPayload.position();
            while (headerPayload.hasRemaining() && headerPayload.get() != ':');
            int nameEnd = headerPayload.position() - 1;
            int valueStart = headerPayload.position() + 1;// assumes that the colon is followed by a space

            while (headerPayload.hasRemaining() && headerPayload.get() != '\n');
            int valueEnd = headerPayload.position() - 2;// assumes that \n is preceded by a \r, this isnt generally
                                                        // safe?
            if (valueEnd < valueStart) {
                valueEnd = valueStart;
            }
            int endOfLinePosition = headerPayload.position();

            byte[] headerBytes = new byte[nameEnd - nameStart];
            headerPayload.position(nameStart);
            headerPayload.get(headerBytes);

            byteArrays.add(headerBytes);
            if (Arrays.equals(headerBytes, "content-type".getBytes(StandardCharsets.US_ASCII))) {
                // rewrite grpc-web content type to matching grpc content type
                byteArrays.add("grpc+proto".getBytes(StandardCharsets.US_ASCII));
                // TODO support other formats like text, non-proto
                headerPayload.position(valueEnd);
                continue;
            }

            // TODO check for binary header suffix
            // if (headerBytes.endsWith(Metadata.BINARY_HEADER_SUFFIX)) {
            //
            // } else {
            byte[] valueBytes = new byte[valueEnd - valueStart];
            headerPayload.position(valueStart);
            headerPayload.get(valueBytes);
            byteArrays.add(valueBytes);
            // }

            headerPayload.position(endOfLinePosition);
        }

        // add a te:trailers, as gRPC will expect it
        byteArrays.add("te".getBytes(StandardCharsets.US_ASCII));
        byteArrays.add("trailers".getBytes(StandardCharsets.US_ASCII));

        // TODO to support text encoding

        return InternalMetadata.newMetadata(byteArrays.toArray(new byte[][] {}));
    }
}
