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

import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.Attributes;
import io.grpc.InternalLogId;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.internal.AbstractServerStream;
import io.grpc.internal.ReadableBuffer;
import io.grpc.internal.SerializingExecutor;
import io.grpc.internal.ServerTransportListener;
import io.grpc.internal.StatsTraceContext;
import io.grpc.internal.TransportTracer;
import io.grpc.internal.WritableBufferAllocator;
import jakarta.websocket.Session;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class AbstractWebsocketStreamImpl extends AbstractServerStream {
    public final class WebsocketTransportState extends TransportState {

        private final SerializingExecutor transportThreadExecutor =
                new SerializingExecutor(MoreExecutors.directExecutor());
        private final Logger logger;

        private WebsocketTransportState(int maxMessageSize, StatsTraceContext statsTraceCtx,
                TransportTracer transportTracer, Logger logger) {
            super(maxMessageSize, statsTraceCtx, transportTracer);
            this.logger = logger;
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

    protected final TransportState transportState;
    protected final Session websocketSession;
    protected final InternalLogId logId;
    protected final Attributes attributes;

    public AbstractWebsocketStreamImpl(WritableBufferAllocator bufferAllocator, StatsTraceContext statsTraceCtx,
            int maxInboundMessageSize, Session websocketSession, InternalLogId logId, Attributes attributes,
            Logger logger) {
        super(bufferAllocator, statsTraceCtx);
        transportState =
                new WebsocketTransportState(maxInboundMessageSize, statsTraceCtx, new TransportTracer(), logger);
        this.websocketSession = websocketSession;
        this.logId = logId;
        this.attributes = attributes;
    }

    protected static void writeAsciiHeadersToMessage(byte[][] serializedHeaders, ByteBuffer message) {
        for (int i = 0; i < serializedHeaders.length; i += 2) {
            message.put(serializedHeaders[i]);
            message.put((byte) ':');
            message.put((byte) ' ');
            message.put(serializedHeaders[i + 1]);
            message.put((byte) '\r');
            message.put((byte) '\n');
        }
    }

    @Override
    public int streamId() {
        return -1;
    }

    @Override
    public Attributes getAttributes() {
        return attributes;
    }

    public void createStream(ServerTransportListener transportListener, String methodName, Metadata headers) {
        transportState().runOnTransportThread(() -> {
            transportListener.streamCreated(this, methodName, headers);
            transportState().onStreamAllocated();
        });
    }

    public void inboundDataReceived(ReadableBuffer message, boolean endOfStream) {
        transportState().runOnTransportThread(() -> {
            transportState().inboundDataReceived(message, endOfStream);
        });
    }

    public void transportReportStatus(Status status) {
        transportState().runOnTransportThread(() -> transportState().transportReportStatus(status));
    }

    @Override
    public TransportState transportState() {
        return transportState;
    }

    protected void cancelSink(Status status) {
        if (!websocketSession.isOpen() && Status.Code.DEADLINE_EXCEEDED == status.getCode()) {
            return;
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
