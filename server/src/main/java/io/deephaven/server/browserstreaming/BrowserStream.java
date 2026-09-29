//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.browserstreaming;

import com.google.rpc.Code;
import io.deephaven.base.RAPriQueue;
import io.deephaven.base.verify.Assert;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.util.GrpcServiceOverrideBuilder;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.io.Closeable;

public class BrowserStream<T> implements Closeable {
    public enum Mode {
        /**
         * Messages must be processed in order, if a gap is observed in sequences wait until the missing message
         * arrives.
         */
        IN_ORDER,
        /** Always process the current message if it has the highest sequence, if an old message arrives, ignore it. */
        MOST_RECENT
    }

    /**
     * Creates a BrowserStream based on the current session and the observer passed in to the open stream call,
     * optionally creating an export to scope the call to the session lifetime.
     */
    public interface Factory<ReqT, RespT> {
        BrowserStream<ReqT> create(SessionState sessionState, StreamData initialStreamData,
                StreamObserver<RespT> responseObserver);
    }

    private static class Message<T> {
        private int pos;
        private final T message;
        private final StreamData streamData;

        public Message(T message, StreamData streamData) {
            this.message = message;
            this.streamData = streamData;
        }

        public T getMessage() {
            return message;
        }

        public StreamData getStreamData() {
            return streamData;
        }
    }

    private interface Marshaller<T> {
        void onMessageReceived(T message);

        void onCancel();

        void onError(Throwable err);

        void onCompleted();
    }

    private static final Logger log = LoggerFactory.getLogger(BrowserStream.class);

    /**
     * Builds a BrowserStream factory based on the given mode and bidirectional stream method.
     */
    public static <ReqT, RespT> Factory<ReqT, RespT> factory(Mode mode,
            GrpcServiceOverrideBuilder.BidiDelegate<ReqT, RespT> bidiDelegate) {
        return (session, initialStreamData, responseObserver) -> {
            final Marshaller<ReqT> marshaller = new Marshaller<>() {
                private final StreamObserver<ReqT> requestObserver = bidiDelegate.doInvoke(responseObserver);

                @Override
                public void onMessageReceived(ReqT message) {
                    requestObserver.onNext(message);
                }

                @Override
                public void onCancel() {
                    StatusRuntimeException canceled =
                            Exceptions.statusRuntimeException(Code.CANCELLED, "Stream canceled on the server");
                    GrpcUtil.safelyError(responseObserver, canceled);

                    GrpcUtil.safelyError(requestObserver, canceled);
                }

                @Override
                public void onError(Throwable err) {
                    requestObserver.onError(err);
                }

                @Override
                public void onCompleted() {
                    requestObserver.onCompleted();
                }
            };
            return new BrowserStream<>(mode, session, initialStreamData, marshaller);
        };
    }

    /** represents the sequence that the listener will process next */
    private long nextSeq = 0;
    /** represents whether or not an item is currently being processed */
    private boolean processingMessage = false;
    /** represents the sequence that the client-stream should be considered closed */
    private long halfClosedSeq = -1;

    private final Mode mode;
    private final String logIdentity;
    private final SessionState session;
    private final Marshaller<T> marshaller;

    /** priority queue for all pending seq when mode is Mode.IN_ORDER */
    private RAPriQueue<Message<T>> pendingSeq;

    /** most recent queued msg for when mode is Mode.MOST_RECENT */
    private StreamData queuedStreamData;
    private T queuedMessage;

    /**
     * Export retaining this stream so later {@code Next} calls can resolve it by ticket, or {@code null} if the initial
     * message was a half-close. Released when the stream ends, otherwise it would leak for the session's lifetime.
     */
    private final SessionState.ExportObject<?> export;
    /** whether this stream has ended: completed, failed, cancelled by the client, or closed with the session */
    private boolean ended;

    private BrowserStream(final Mode mode, final SessionState session, final StreamData initialStreamData,
            final Marshaller<T> marshaller) {
        this.mode = mode;
        this.logIdentity = "BrowserStream(" + Integer.toHexString(System.identityHashCode(this)) + "): ";
        this.session = session;
        this.marshaller = marshaller;

        // Retain this stream so later Next calls can resolve it by ticket, unless the client won't send more messages;
        // released when the stream ends so that it (and everything it keeps reachable) can be collected without
        // waiting for the session to end.
        final SessionState.ExportBuilder<BrowserStream<T>> exportBuilder;
        try {
            exportBuilder = initialStreamData.isHalfClose() ? null
                    : session.newExport(initialStreamData.getRpcTicket(), "rpcTicket");
            // A Next call that arrived before this open may already be waiting on this export, and runs as soon as
            // its work does - possibly on another thread, before submit() returns - so it may end this stream at once.
            // Both the export and the close callback must be in place by then, or the end steps have nothing to
            // release.
            this.export = exportBuilder == null ? null : exportBuilder.getExport();
            this.session.addOnCloseCallback(this);
        } catch (final RuntimeException err) {
            // the session already expired; nothing has a reference to this instance yet, so all that is left is to let
            // the underlying call know now instead of leaving it to hang
            marshaller.onError(err);
            throw err;
        }
        if (exportBuilder == null) {
            return;
        }
        try {
            // no onError needed here: if this export is later cancelled by the session expiring, close() (registered
            // just above) already notifies the marshaller
            exportBuilder.submit(() -> this);
        } catch (final RuntimeException err) {
            // The ticket is already taken, so this stream can never be reached; undo the registration above and let
            // the underlying call know now instead of leaving it to hang. Until here `export` refers to whoever owns
            // the ticket, but this instance never escapes construction, so nothing can act on it - except close() if
            // the session expires in between, and by then the session has already cancelled every export (hence the
            // guard: the marshaller has been notified, and cancelling a terminal export is a no-op).
            if (this.session.removeOnCloseCallback(this)) {
                marshaller.onError(err);
            }
            throw err;
        }
    }

    public void onMessageReceived(T message, StreamData streamData) {
        synchronized (this) {
            if (ended) {
                // the stream is over (the client abandoned it, or it failed); there is nothing left to deliver to
                return;
            }
            if (halfClosedSeq != -1 && streamData.getSequence() > halfClosedSeq) {
                throw Exceptions.statusRuntimeException(Code.ABORTED, "Sequence sent after half close: closed seq="
                        + halfClosedSeq + " recv seq=" + streamData.getSequence());
            }

            if (streamData.isHalfClose()) {
                if (halfClosedSeq != -1) {
                    throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "Already half closed: closed seq="
                            + halfClosedSeq + " recv seq=" + streamData.getSequence());
                }
                halfClosedSeq = streamData.getSequence();
            }

            if (mode == Mode.IN_ORDER) {
                if (streamData.getSequence() < nextSeq) {
                    throw Exceptions.statusRuntimeException(Code.OUT_OF_RANGE,
                            "Duplicate sequence sent: next seq=" + nextSeq + " recv seq=" + streamData.getSequence());
                }
                boolean queueMsg = false;
                if (processingMessage) {
                    queueMsg = true;
                    log.debug().append(logIdentity).append("queueing; next seq=").append(nextSeq)
                            .append(" recv seq=").append(streamData.getSequence()).endl();
                } else if (streamData.getSequence() != nextSeq) {
                    queueMsg = true;
                    log.debug().append(logIdentity).append("queueing; waiting seq=").append(nextSeq)
                            .append(" recv seq=").append(streamData.getSequence()).endl();
                }
                if (queueMsg) {
                    if (pendingSeq == null) {
                        pendingSeq = new RAPriQueue<>(1, MessageInfoQueueAdapter.getInstance(), Message.class);
                    }
                    pendingSeq.enter(new Message<>(message, streamData));
                    return;
                }
            } else { // Mode.MOST_RECENT
                if (streamData.getSequence() < nextSeq
                        || (streamData.getSequence() == nextSeq && processingMessage) // checks for duplicate
                        || (queuedStreamData != null && streamData.getSequence() < queuedStreamData.getSequence())) {
                    // this message is too old
                    log.debug().append(logIdentity).append("dropping; next seq=").append(nextSeq)
                            .append(" queued seq=")
                            .append(queuedStreamData != null ? queuedStreamData.getSequence() : -1)
                            .append(" recv seq=").append(streamData.getSequence()).endl();
                    return;
                }
                // is most recent msg seen
                if (processingMessage) {
                    log.debug().append(logIdentity).append("queueing; processing seq=").append(nextSeq)
                            .append(" recv seq=").append(streamData.getSequence()).endl();
                    queuedStreamData = streamData;
                    queuedMessage = message;
                    return;
                }
            }

            nextSeq = streamData.getSequence() + 1;
            processingMessage = true;
        }

        do {
            synchronized (this) {
                if (ended) {
                    // the stream ended while the previous message was being delivered; drop whatever is still queued
                    processingMessage = false;
                    return;
                }
                if (streamData.isHalfClose()) {
                    onComplete();
                    processingMessage = false;
                    return;
                }
            }
            try {
                marshaller.onMessageReceived(message);
            } catch (final RuntimeException e) {
                onError(e);
                return;
            }

            synchronized (this) {
                if (mode == Mode.IN_ORDER) {
                    if (pendingSeq == null || pendingSeq.top() == null) {
                        message = null;
                        streamData = null;
                    } else {
                        Message<T> top = pendingSeq.top();
                        message = top.getMessage();
                        streamData = top.getStreamData();
                    }
                    if (streamData == null || streamData.getSequence() != nextSeq) {
                        processingMessage = false;
                        break;
                    }
                    Assert.eq(pendingSeq.removeTop().getMessage(), "pendingSeq.remoteTop()", message, "message");
                } else { // Mode.MOST_RECENT
                    message = queuedMessage;
                    streamData = queuedStreamData;
                    if (message == null) {
                        processingMessage = false;
                        break;
                    }
                    queuedStreamData = null;
                }

                log.debug().append(logIdentity).append("processing queued seq=").append(streamData.getSequence())
                        .endl();
                nextSeq = streamData.getSequence() + 1;
            }
        } while (true);
    }

    public void onError(final RuntimeException e) {
        markEnded();
        try {
            if (session.removeOnCloseCallback(this)) {
                log.error().append(logIdentity).append("closing browser stream on unexpected exception: ").append(e)
                        .endl();
                this.marshaller.onError(e);
            }
        } finally {
            releaseExport();
        }
    }

    /**
     * The client abandoned the call that opened this stream.
     */
    public void onCancel() {
        markEnded();
        try {
            if (session.removeOnCloseCallback(this)) {
                log.debug().append(logIdentity).append("browser stream cancelled by client").endl();
                this.marshaller.onCancel();
            }
        } finally {
            releaseExport();
        }
    }

    /**
     * The session this stream belongs to is closing or has expired.
     */
    @Override
    public void close() {
        markEnded();
        try {
            this.marshaller.onCancel();
        } finally {
            releaseExport();
        }
    }

    private void onComplete() {
        markEnded();
        try {
            if (session.removeOnCloseCallback(this)) {
                log.debug().append(logIdentity).append("browser stream completed").endl();
                this.marshaller.onCompleted();
            }
        } finally {
            releaseExport();
        }
    }

    /**
     * Marks this stream as ended, so that no further message is delivered to it, and drops anything still queued.
     * Idempotent, like the release and notification steps at each call site above:
     * {@link SessionState.ExportObject#cancel} tolerates redundant calls, and {@code removeOnCloseCallback} only
     * succeeds once, so the marshaller is notified at most once regardless.
     */
    private synchronized void markEnded() {
        ended = true;
        // nothing queued will be delivered now; drop it so that an ended stream retains only itself for as long as
        // the call that opened it keeps a reference to it
        pendingSeq = null;
        queuedMessage = null;
        queuedStreamData = null;
    }

    private void releaseExport() {
        if (export != null) {
            // cancel rather than release: the export may not have run yet, and the session may already be expired
            export.cancel();
        }
    }

    private static class MessageInfoQueueAdapter implements RAPriQueue.Adapter<Message<?>> {
        private static final MessageInfoQueueAdapter INSTANCE = new MessageInfoQueueAdapter();

        private static <T extends Message<?>> RAPriQueue.Adapter<T> getInstance() {
            // noinspection unchecked
            return (RAPriQueue.Adapter<T>) INSTANCE;
        }

        @Override
        public boolean less(Message<?> a, Message<?> b) {
            return a.getStreamData().getSequence() < b.getStreamData().getSequence();
        }

        @Override
        public void setPos(Message<?> mi, int pos) {
            mi.pos = pos;
        }

        @Override
        public int getPos(Message<?> mi) {
            return mi.pos;
        }
    }
}
