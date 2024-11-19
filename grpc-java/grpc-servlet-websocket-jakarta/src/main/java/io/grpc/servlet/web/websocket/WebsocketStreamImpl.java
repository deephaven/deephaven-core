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

public class WebsocketStreamImpl extends AbstractWebsocketStreamImpl {
    private static final Logger logger = Logger.getLogger(WebsocketStreamImpl.class.getName());

    private final Sink sink = new Sink();

    public WebsocketStreamImpl(StatsTraceContext statsTraceCtx, int maxInboundMessageSize, Session websocketSession,
            InternalLogId logId, Attributes attributes) {
        super(ByteArrayWritableBuffer::new, statsTraceCtx, maxInboundMessageSize, websocketSession, logId, attributes,
                logger);
    }

    @Override
    protected Sink abstractServerStreamSink() {
        return sink;
    }

    private final class Sink implements AbstractServerStream.Sink {

        @Override
        public void writeHeaders(Metadata headers, boolean flush) {
            // headers/trailers are always sent as asci, colon-delimited pairs, with \r\n separating them. The
            // trailer response must be prefixed with 0x80 (0r 0x81 if compressed), followed by the length of the
            // message

            byte[][] serializedHeaders = TransportFrameUtil.toHttp2Headers(headers);
            // Total up the size of the payload: 5 bytes for the prefix, and each header needs a colon delimiter, and to
            // end with \r\n
            int headerLength = Arrays.stream(serializedHeaders).mapToInt(arr -> arr.length + 2).sum();
            ByteBuffer prefix = ByteBuffer.allocate(5);
            prefix.put((byte) 0x80);
            prefix.putInt(headerLength);
            prefix.flip();
            ByteBuffer message = ByteBuffer.allocate(headerLength);
            writeAsciiHeadersToMessage(serializedHeaders, message);
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

            byte[][] serializedTrailers = TransportFrameUtil.toHttp2Headers(trailers);
            // Total up the size of the payload: 5 bytes for the prefix, and each trailer needs a colon+space delimiter,
            // and to end with \r\n
            int trailerLength = Arrays.stream(serializedTrailers).mapToInt(arr -> arr.length + 2).sum();
            ByteBuffer prefix = ByteBuffer.allocate(5);
            prefix.put((byte) 0x80);
            prefix.putInt(trailerLength);
            prefix.flip();
            ByteBuffer message = ByteBuffer.allocate(trailerLength);
            writeAsciiHeadersToMessage(serializedTrailers, message);
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
            cancelSink(status);
        }
    }
}
