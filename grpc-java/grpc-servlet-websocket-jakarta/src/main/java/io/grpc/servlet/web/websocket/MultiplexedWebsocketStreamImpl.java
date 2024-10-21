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
import io.grpc.Status;
import io.grpc.internal.AbstractServerStream;
import io.grpc.internal.StatsTraceContext;
import io.grpc.internal.TransportFrameUtil;
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
        if (streamId < 0) {
            throw new IllegalStateException("Can't create stream with negative id");
        }
        this.streamId = streamId;
    }

    @Override
    protected AbstractServerStream.Sink abstractServerStreamSink() {
        return sink;
    }

    private final class Sink implements AbstractServerStream.Sink {

        @Override
        public void writeHeaders(Metadata headers, boolean flush) {
            writeMetadataToStream(headers, false);
        }

        /**
         * Usual multiplexing framing applies - before writing metadata, we write the 32-bit int streamId, with the top
         * bit to represent if this stream (over the shared transport) should be closed or not.
         * <p>
         * </p>
         * Headers/trailers are always sent as asci, colon-delimited pairs, with \r\n separating them. The trailer
         * response must be prefixed with 0x80 (0r 0x81 if compressed), followed by the length of the message.
         */
        private void writeMetadataToStream(Metadata headers, boolean closeBitSet) {
            byte[][] serializedHeaders = TransportFrameUtil.toHttp2Headers(headers);
            // Total up the size of the payload: 4 bytes for multiplexing framing, 5 bytes for the prefix, and each
            // header needs a colon delimiter, and to end with \r\n
            int headerLength = Arrays.stream(serializedHeaders).mapToInt(arr -> arr.length + 2).sum();
            ByteBuffer message = ByteBuffer.allocate(headerLength + 9);
            // If this is the final write, set the highest bit on the streamid
            message.putInt(closeBitSet ? streamId ^ (1 << 31) : streamId);
            message.put((byte) 0x80);
            message.putInt(headerLength);
            writeAsciiHeadersToMessage(serializedHeaders, message);
            if (message.hasRemaining()) {
                throw new IllegalStateException("Incorrectly sized buffer, header/trailer payload will be sized wrong");
            }
            message.flip();
            try {
                websocketSession.getBasicRemote().sendBinary(message);
            } catch (IOException e) {
                // rethrowing from this method adds nonsense to the logs; onError will be invoked automatically
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

                    ByteBuffer payload = ByteBuffer.allocate(numBytes + 4);
                    payload.putInt(streamId);
                    payload.put(((ByteArrayWritableBuffer) frame).bytes, 0, numBytes);
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

            writeMetadataToStream(trailers, true);
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
