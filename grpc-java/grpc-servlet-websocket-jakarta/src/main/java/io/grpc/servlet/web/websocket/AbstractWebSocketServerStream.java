package io.grpc.servlet.web.websocket;

import com.google.common.io.BaseEncoding;
import io.grpc.Attributes;
import io.grpc.InternalMetadata;
import io.grpc.Metadata;
import io.grpc.ServerStreamTracer;
import io.grpc.internal.ServerTransportListener;
import jakarta.websocket.Endpoint;
import jakarta.websocket.EndpointConfig;
import jakarta.websocket.Session;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class AbstractWebSocketServerStream extends Endpoint {
    private static final byte[] BINARY_HEADER_SUFFIX_ARR =
            Metadata.BINARY_HEADER_SUFFIX.getBytes(StandardCharsets.US_ASCII);
    protected final ServerTransportListener transportListener;
    protected final List<? extends ServerStreamTracer.Factory> streamTracerFactories;
    protected final int maxInboundMessageSize;
    protected final Attributes attributes;

    // assigned on open, always available
    protected Session websocketSession;

    protected AbstractWebSocketServerStream(ServerTransportListener transportListener,
            List<? extends ServerStreamTracer.Factory> streamTracerFactories, int maxInboundMessageSize,
            Attributes attributes) {
        this.transportListener = transportListener;
        this.streamTracerFactories = streamTracerFactories;
        this.maxInboundMessageSize = maxInboundMessageSize;
        this.attributes = attributes;
    }

    protected static Metadata readHeaders(ByteBuffer headerPayload) {
        // Headers are passed as ascii, ":"-separated key/value pairs, separated on "\r\n". The client
        // implementation shows that values might be comma-separated, but we'll pass that through directly as a plain
        // string.
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
                // rewrite grpc-web content type to matching grpc content type, regardless of what it said
                byteArrays.add("grpc+proto".getBytes(StandardCharsets.US_ASCII));
                // TODO support other formats like text, non-proto
                headerPayload.position(valueEnd);
                continue;
            }

            byte[] valueBytes = new byte[valueEnd - valueStart];
            headerPayload.position(valueStart);
            headerPayload.get(valueBytes);
            if (endsWithBinHeaderSuffix(headerBytes)) {
                byteArrays.add(BaseEncoding.base64().decode(ByteBuffer.wrap(valueBytes).asCharBuffer()));
            } else {
                byteArrays.add(valueBytes);
            }

            headerPayload.position(endOfLinePosition);
        }

        // add a te:trailers, as gRPC will expect it
        byteArrays.add("te".getBytes(StandardCharsets.US_ASCII));
        byteArrays.add("trailers".getBytes(StandardCharsets.US_ASCII));

        // TODO to support text encoding

        return InternalMetadata.newMetadata(byteArrays.toArray(new byte[][] {}));
    }

    private static boolean endsWithBinHeaderSuffix(byte[] headerBytes) {
        // This is intended to be equiv to
        // header.endsWith(Metadata.BINARY_HEADER_SUFFIX), without actually making a string for it
        if (headerBytes.length < BINARY_HEADER_SUFFIX_ARR.length) {
            return false;
        }
        for (int i = 0; i < BINARY_HEADER_SUFFIX_ARR.length; i++) {
            if (headerBytes[headerBytes.length - 3 + i] != BINARY_HEADER_SUFFIX_ARR[i]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void onOpen(Session websocketSession, EndpointConfig config) {
        this.websocketSession = websocketSession;

        websocketSession.addMessageHandler(String.class, this::onMessage);
        websocketSession.addMessageHandler(ByteBuffer.class, message -> {
            try {
                onMessage(message);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        // Configure defaults present in some servlet containers to avoid some confusing limits. Subclasses
        // can override this method to control those defaults on their own.
        websocketSession.setMaxIdleTimeout(0);
        websocketSession.setMaxBinaryMessageBufferSize(Integer.MAX_VALUE);
    }

    public abstract void onMessage(String message);

    public abstract void onMessage(ByteBuffer message) throws IOException;
}
