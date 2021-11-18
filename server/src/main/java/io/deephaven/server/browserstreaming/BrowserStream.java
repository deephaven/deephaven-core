package io.deephaven.server.browserstreaming;

import com.google.rpc.Code;
import io.deephaven.base.RAPriQueue;
import io.deephaven.base.verify.Assert;
import io.deephaven.extensions.barrage.util.GrpcUtil;
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
     * Creates a BrowserStream based on the current session and the observed passed in to the open stream call.
     */
    public interface Factory<ReqT, RespT> {
        BrowserStream<ReqT> create(SessionState sessionState, StreamObserver<RespT> responseObserver);
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
        return (session, responseObserver) -> new BrowserStream<>(mode, session, new Marshaller<ReqT>() {
            private final StreamObserver<ReqT> requestObserver = bidiDelegate.doInvoke(responseObserver);

            @Override
            public void onMessageReceived(ReqT message) {
                requestObserver.onNext(message);
            }

            @Override
            public void onCancel() {
                StatusRuntimeException canceled =
                        GrpcUtil.statusRuntimeException(Code.CANCELLED, "Stream canceled on the server");
                GrpcUtil.safelyExecute(() -> {
                    responseObserver.onError(canceled);
                });

                GrpcUtil.safelyExecute(() -> {
                    requestObserver.onError(canceled);
                });
            }

            @Override
            public void onError(Throwable err) {
                requestObserver.onError(err);
            }

            @Override
            public void onCompleted() {
                requestObserver.onCompleted();
            }
        });
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

    private BrowserStream(final Mode mode, final SessionState session, final Marshaller<T> marshaller) {
        this.mode = mode;
        this.logIdentity = "BrowserStream(" + Integer.toHexString(System.identityHashCode(this)) + "): ";
        this.session = session;
        this.marshaller = marshaller;

        this.session.addOnCloseCallback(this);
    }

    public void onMessageReceived(T message, StreamData streamData) {
        synchronized (this) {
            if (halfClosedSeq != -1 && streamData.getSequence() > halfClosedSeq) {
                throw GrpcUtil.statusRuntimeException(Code.ABORTED, "Sequence sent after half close: closed seq="
                        + halfClosedSeq + " recv seq=" + streamData.getSequence());
            }

            if (streamData.isHalfClose()) {
                if (halfClosedSeq != -1) {
                    throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Already half closed: closed seq="
                            + halfClosedSeq + " recv seq=" + streamData.getSequence());
                }
                halfClosedSeq = streamData.getSequence();
            }

            if (mode == Mode.IN_ORDER) {
                if (streamData.getSequence() < nextSeq) {
                    throw GrpcUtil.statusRuntimeException(Code.OUT_OF_RANGE,
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
        if (session.removeOnCloseCallback(this)) {
            log.error().append(logIdentity).append("closing browser stream on unexpected exception: ").append(e).endl();
            this.marshaller.onError(e);
        }
    }

    @Override
    public void close() {
        this.marshaller.onCancel();
    }

    private void onComplete() {
        if (session.removeOnCloseCallback(this)) {
            log.debug().append(logIdentity).append("browser stream completed").endl();
            this.marshaller.onCompleted();
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
