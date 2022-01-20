package io.grpc.servlet.web.websocket;

import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.Attributes;
import io.grpc.InternalLogId;
import io.grpc.InternalMetadata;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.internal.AbstractServerStream;
import io.grpc.internal.ReadableBuffer;
import io.grpc.internal.SerializingExecutor;
import io.grpc.internal.ServerTransportListener;
import io.grpc.internal.StatsTraceContext;
import io.grpc.internal.TransportTracer;
import io.grpc.internal.WritableBuffer;
import jakarta.websocket.Session;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.lang.Math.max;
import static java.lang.Math.min;

public class WebsocketStreamImpl extends AbstractServerStream {
    private static final Logger logger = Logger.getLogger(WebsocketStreamImpl.class.getName());

    public final class WebsocketTransportState extends TransportState {

        private final SerializingExecutor transportThreadExecutor =
                new SerializingExecutor(MoreExecutors.directExecutor());

        protected WebsocketTransportState(int maxMessageSize, StatsTraceContext statsTraceCtx,
                TransportTracer transportTracer) {
            super(maxMessageSize, statsTraceCtx, transportTracer);
        }

        @Override
        public void runOnTransportThread(Runnable r) {
            transportThreadExecutor.execute(r);
        }

        @Override
        public void bytesRead(int numBytes) {
            // no-op, no flow-control yet
        }

        @Override
        public void deframeFailed(Throwable cause) {
            if (logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE, String.format("[{%s}] Exception processing message", logId), cause);
            }
            cancel(Status.fromThrowable(cause));
        }
    }
    private static final class ByteArrayWritableBuffer implements WritableBuffer {

        private final int capacity;
        final byte[] bytes;
        private int index;

        ByteArrayWritableBuffer(int capacityHint) {
            this.bytes = new byte[min(1024 * 1024, max(4096, capacityHint))];
            this.capacity = bytes.length;
        }

        @Override
        public void write(byte[] src, int srcIndex, int length) {
            System.arraycopy(src, srcIndex, bytes, index, length);
            index += length;
        }

        @Override
        public void write(byte b) {
            bytes[index++] = b;
        }

        @Override
        public int writableBytes() {
            return capacity - index;
        }

        @Override
        public int readableBytes() {
            return index;
        }

        @Override
        public void release() {}
    }

    private final WebsocketTransportState transportState;
    private final Sink sink = new Sink();
    private final Session websocketSession;
    private final InternalLogId logId;
    private final Attributes attributes;

    public WebsocketStreamImpl(StatsTraceContext statsTraceCtx, int maxInboundMessageSize, Session websocketSession,
            InternalLogId logId, Attributes attributes) {
        super(ByteArrayWritableBuffer::new, statsTraceCtx);
        this.websocketSession = websocketSession;
        this.logId = logId;
        this.attributes = attributes;
        transportState = new WebsocketTransportState(maxInboundMessageSize, statsTraceCtx, new TransportTracer());
    }

    @Override
    public Attributes getAttributes() {
        return attributes;
    }

    public void createStream(ServerTransportListener transportListener, String methodName, Metadata headers) {
        transportListener.streamCreated(this, methodName, headers);
        transportState().onStreamAllocated();
    }

    public void inboundDataReceived(ReadableBuffer message, boolean endOfStream) {
        transportState().inboundDataReceived(message, endOfStream);
    }

    public void transportReportStatus(Status status) {
        transportState().transportReportStatus(status);
    }

    @Override
    public TransportState transportState() {
        return transportState;
    }

    @Override
    protected Sink abstractServerStreamSink() {
        return sink;
    }

    @Override
    public int streamId() {
        return -1;
    }

    private final class Sink implements AbstractServerStream.Sink {

        @Override
        public void writeHeaders(Metadata headers) {
            // headers/trailers are always sent as asci, colon-delimited pairs, with \r\n separating them. The
            // trailer response must be prefixed with 0x80 (0r 0x81 if compressed), followed by the length of the
            // message

            byte[][] serializedHeaders = InternalMetadata.serialize(headers);
            // Total up the size of the payload: 5 bytes for the prefix, and each header needs a colon delimiter, and to
            // end with \r\n
            int headerLength = Arrays.stream(serializedHeaders).mapToInt(arr -> arr.length + 2).sum();
            ByteBuffer prefix = ByteBuffer.allocate(5);
            prefix.put((byte) 0x80);
            prefix.putInt(headerLength);
            prefix.flip();
            ByteBuffer message = ByteBuffer.allocate(headerLength);
            for (int i = 0; i < serializedHeaders.length; i += 2) {
                message.put(serializedHeaders[i]);
                message.put((byte) ':');
                message.put((byte) ' ');
                message.put(serializedHeaders[i + 1]);
                message.put((byte) '\r');
                message.put((byte) '\n');
            }
            message.flip();
            try {
                // send in two separate payloads
                websocketSession.getBasicRemote().sendBinary(prefix);
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

                    ByteBuffer payload =
                            ByteBuffer.wrap(((ByteArrayWritableBuffer) frame).bytes, 0, frame.readableBytes());

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
            // Total up the size of the payload: 5 bytes for the prefix, and each trailer needs a colon+space delimiter,
            // and to end with \r\n
            int trailerLength = Arrays.stream(serializedTrailers).mapToInt(arr -> arr.length + 2).sum();
            ByteBuffer prefix = ByteBuffer.allocate(5);
            prefix.put((byte) 0x80);
            prefix.putInt(trailerLength);
            prefix.flip();
            ByteBuffer message = ByteBuffer.allocate(trailerLength);
            for (int i = 0; i < serializedTrailers.length; i += 2) {
                message.put(serializedTrailers[i]);
                message.put((byte) ':');
                message.put((byte) ' ');
                message.put(serializedTrailers[i + 1]);
                message.put((byte) '\r');
                message.put((byte) '\n');
            }
            message.flip();
            try {
                // send in two separate messages
                websocketSession.getBasicRemote().sendBinary(prefix);
                websocketSession.getBasicRemote().sendBinary(message);

                websocketSession.close();
            } catch (IOException e) {
                throw Status.fromThrowable(e).asRuntimeException();
            }
            transportState().runOnTransportThread(() -> {
                transportState().complete();
            });
        }

        @Override
        public void cancel(Status status) {
            if (!websocketSession.isOpen() && Status.Code.DEADLINE_EXCEEDED == status.getCode()) {
                return; // let the servlet timeout, the container will sent RST_STREAM automatically
            }
            transportState.runOnTransportThread(() -> transportState.transportReportStatus(status));
            // There is no way to RST_STREAM with CANCEL code, so write trailers instead
            close(Status.CANCELLED.withCause(status.asRuntimeException()), new Metadata());
            CountDownLatch countDownLatch = new CountDownLatch(1);
            transportState.runOnTransportThread(() -> {
                try {
                    websocketSession.close();
                } catch (IOException ioException) {
                    // already closing, ignore
                }
                countDownLatch.countDown();
            });
            try {
                countDownLatch.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

}
