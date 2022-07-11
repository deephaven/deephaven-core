/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.grpc.servlet.web.websocket;

import io.grpc.Attributes;
import io.grpc.InternalLogId;
import io.grpc.InternalMetadata;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.internal.AbstractServerStream;
import io.grpc.internal.StatsTraceContext;
import io.grpc.internal.WritableBuffer;
import jakarta.websocket.Session;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MultiplexedWebsocketStreamImpl extends AbstractWebsocketStreamImpl {
    private static final Logger logger = Logger.getLogger(MultiplexedWebsocketStreamImpl.class.getName());

    private final Sink sink = new Sink();

    // Note that this isn't a "true" stream id in the h2 or grpc sense, this shouldn't be returned via streamId()
    private final int streamId;

    public MultiplexedWebsocketStreamImpl(StatsTraceContext statsTraceCtx, int maxInboundMessageSize,
            Session websocketSession, InternalLogId logId, Attributes attributes, int streamId) {
        super(ByteArrayWritableBuffer::new, statsTraceCtx, maxInboundMessageSize, websocketSession, logId, attributes,
                logger);
        this.streamId = streamId;
    }

    @Override
    protected AbstractServerStream.Sink abstractServerStreamSink() {
        return sink;
    }

    private final class Sink implements AbstractServerStream.Sink {

        @Override
        public void writeHeaders(Metadata headers) {
            // headers/trailers are always sent as asci, colon-delimited pairs, with \r\n separating them. The
            // trailer response must be prefixed with 0x80 (0r 0x81 if compressed), followed by the length of the
            // message

            byte[][] serializedHeaders = InternalMetadata.serialize(headers);
            // Total up the size of the payload: 4 bytes for multiplexing framing, 5 bytes for the prefix, and each
            // header needs a colon delimiter, and to end with \r\n
            int headerLength = Arrays.stream(serializedHeaders).mapToInt(arr -> arr.length + 2).sum();
            ByteBuffer message = ByteBuffer.allocate(headerLength + 9 + 4);
            message.putInt(streamId);
            message.put((byte) 0x80);
            message.putInt(headerLength);
            writeAsciiHeadersToMessage(serializedHeaders, message);
            message.flip();
            try {
                websocketSession.getBasicRemote().sendBinary(message);
            } catch (IOException e) {
                throw Status.fromThrowable(e).asRuntimeException();
            }
        }

        @Override
        public void writeFrame(@Nullable WritableBuffer frame, boolean flush, int numMessages) {
            if (frame == null && !flush) {
                return;
            }

            if (logger.isLoggable(Level.FINEST)) {
                logger.log(
                        Level.FINEST,
                        "[{0}] writeFrame: numBytes = {1}, flush = {2}, numMessages = {3}",
                        new Object[] {logId, frame == null ? 0 : frame.readableBytes(), flush, numMessages});
            }

            try {
                if (frame != null) {
                    int numBytes = frame.readableBytes();
                    if (numBytes > 0) {
                        onSendingBytes(numBytes);
                    }

                    ByteBuffer payload = ByteBuffer.allocate(frame.readableBytes() + 4);
                    payload.putInt(streamId);
                    payload.put(((ByteArrayWritableBuffer) frame).bytes, 0, frame.readableBytes());
                    payload.flip();

                    websocketSession.getBasicRemote().sendBinary(payload);
                    transportState.runOnTransportThread(() -> transportState.onSentBytes(numBytes));
                }

            } catch (IOException e) {
                // TODO log this off-thread, doing logging about an error from sending a log to a client is a mess
                // logger.log(WARNING, String.format("[{%s}] Exception writing message", logId), e);
                cancel(Status.fromThrowable(e));
            }
        }

        @Override
        public void writeTrailers(Metadata trailers, boolean headersSent, Status status) {
            if (logger.isLoggable(Level.FINE)) {
                logger.log(
                        Level.FINE,
                        "[{0}] writeTrailers: {1}, headersSent = {2}, status = {3}",
                        new Object[] {logId, trailers, headersSent, status});
            }

            // Trailers are always sent as asci, colon-delimited pairs, with \r\n separating them. The
            // trailer response must be prefixed with 0x80 (0r 0x81 if compressed), followed by the size in a 4 byte int

            byte[][] serializedTrailers = InternalMetadata.serialize(trailers);
            // Total up the size of the payload: 4 bytes for multiplexing framing, 5 bytes for the grpc payload prefix,
            // and each trailer needs a colon+space delimiter, and to end with \r\n
            int trailerLength = Arrays.stream(serializedTrailers).mapToInt(arr -> arr.length + 2).sum();

            ByteBuffer message = ByteBuffer.allocate(9 + trailerLength);
            message.putInt(streamId ^ (1 << 31));
            message.put((byte) 0x80);
            message.putInt(trailerLength);
            writeAsciiHeadersToMessage(serializedTrailers, message);
            message.flip();
            try {
                websocketSession.getBasicRemote().sendBinary(message);

                // websocketSession.close();//don't close this, leave it up to the client, or use a timeout
            } catch (IOException e) {
                throw Status.fromThrowable(e).asRuntimeException();
            }
            transportState().runOnTransportThread(() -> {
                transportState().complete();
            });
        }

        @Override
        public void cancel(Status status) {
            cancelSink(status);
        }
    }
}
